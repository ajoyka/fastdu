package main

import (
	"errors"
	"fastdu/lib"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

type dirCount struct {
	mu   sync.Mutex
	size map[string]int64
}

// limit number of files that are open simultaneously to avoid
// hitting into os limits
var fileSizes = make(chan int64)

var nbytes int64
var files int64
var wg sync.WaitGroup
var topFiles = flag.Int("t", 10, "number of top files/directories to display")
var numOpenFiles = flag.Int("c", 20, "concurrency factor")
var sema chan struct{}

func main() {
	flag.Parse()
	sema = make(chan struct{}, *numOpenFiles)
	fmt.Println("concurrency factor", cap(sema), *numOpenFiles)
	dirCount := &dirCount{size: make(map[string]int64)}
	go func() { // important that this be first
		fmt.Println("entering go func")
		for size := range fileSizes {
			files++
			nbytes += size
		}
		fmt.Println("go func exiting")
	}()

	for _, dir := range flag.Args() {
		wg.Add(1)
		go walkDir(dir, dirCount, fileSizes)
	}
	wg.Wait()
	close(fileSizes)
	fmt.Println("size len:", len(dirCount.size))
	keys := lib.SortedKeys(dirCount.size)

	if *topFiles > len(keys) || *topFiles == -1 {
		fmt.Printf("Printing top available %d\n ", len(keys))
	} else {
		keys = keys[:*topFiles]
		fmt.Printf("Printing top %d dirs/files\n", *topFiles)
	}

	for _, key := range keys {
		size := float64(dirCount.size[key])
		sizeGB := size / 1e9
		sizeMB := size / 1e6
		sizeKB := size / 1e3
		var units string

		switch {
		case sizeGB > 0.09:
			size = sizeGB
			units = "GB"
		case sizeMB > 0.09:
			size = sizeMB
			units = "MB"
		default:
			size = sizeKB
			units = "KB"

		}
		fmt.Printf("%.1f%s, %s\n", size, units, key)
	}
	fmt.Printf("%d files, %.1fGB\n", files, float64(nbytes)/1e9)
}

func walkDir(dir string, dirCount *dirCount, fileSizes chan<- int64) {
	defer wg.Done()
	for _, entry := range dirents(dir) {
		if entry.IsDir() {
			wg.Add(1)
			go walkDir(filepath.Join(dir, entry.Name()), dirCount, fileSizes)
		} else {
			dirCount.Inc(dir, entry.Size())
			fileSizes <- entry.Size()
		}
	}
}

func (d *dirCount) Inc(path string, size int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.size[path] += size
}

func dirents(dir string) []os.FileInfo {
	sema <- struct{}{} // acquire token
	defer func() {
		<-sema // release token
	}()

	info, err := ioutil.ReadDir(dir)
	if err != nil {
		if errors.Is(err, syscall.EMFILE) {
			fmt.Printf("\n**Error: %s\nReduce concurrency and retry\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s, %v\n", dir, err)
		return nil
	}
	return info
}
