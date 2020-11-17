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
	"time"
)

type dirCount struct {
	mu   sync.Mutex
	size map[string]int64
}

type fileCount struct {
	mu     sync.Mutex
	files  int64
	nbytes int64
}

// limit number of files that are open simultaneously to avoid
// hitting into os limits
var fileSizes = make(chan int64)

var wg sync.WaitGroup
var topFiles = flag.Int("t", 10, "number of top files/directories to display")
var numOpenFiles = flag.Int("c", 20, "concurrency factor")

var printInterval = flag.Duration("f", 0*time.Second, "print summary at frequency specified in seconds; default disabled with value 0")
var sema chan struct{}

func main() {
	flag.Parse()
	sema = make(chan struct{}, *numOpenFiles)
	fmt.Println("concurrency factor", cap(sema), *numOpenFiles)
	dirCount := &dirCount{size: make(map[string]int64)}
	fileCount := &fileCount{}

	roots := flag.Args()

	var tick <-chan time.Time
	if *printInterval != 0 {
		tick = time.Tick(*printInterval)
	}
	go func() {
	loop:
		for {
			// fmt.Println("in loop")
			select {
			case size, ok := <-fileSizes:
				if !ok {
					break loop // fileSizes was closed
				}
				fileCount.Inc(size)

			case <-tick:
				fmt.Print(".")
				// printFiles(dirCount)
				// fmt.Println("total bytes: ", nbytes)
			}
		}

	}()

	for _, root := range roots {
		wg.Add(1)
		fmt.Println("starting walkdir")
		go walkDir(root, dirCount, fileSizes)
	}

	wg.Wait()
	close(fileSizes)

	printFiles(dirCount)
	files, nbytes := fileCount.Get()
	fmt.Printf("%d files, %.1fGB\n", files, float64(nbytes)/1e9)

}

func printFiles(dirCount *dirCount) {
	dirCount.mu.Lock()
	defer dirCount.mu.Unlock()
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

func (f *fileCount) Inc(size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.files++
	f.nbytes += size
}

func (f *fileCount) Get() (int64, int64) {
	f.mu.Lock()
	f.mu.Unlock()

	return f.files, f.nbytes
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
