package main

import (
	"concdu/lib"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type dirCount struct {
	mu   sync.Mutex
	size map[string]int64
}

var sema = make(chan struct{}, 20)
var fileSizes = make(chan int64)

var nbytes int64
var files int64
var wg sync.WaitGroup

func main() {
	dirCount := &dirCount{size: make(map[string]int64)}
	go func() { // important that this be first
		fmt.Println("entering go func")
		for size := range fileSizes {
			files += 1
			nbytes += size
		}
		fmt.Println("go func exiting")
	}()

	for _, dir := range os.Args[1:] {
		wg.Add(1)
		go walkDir(dir, dirCount, fileSizes)
	}
	wg.Wait()
	close(fileSizes)
	fmt.Println("size len:", len(dirCount.size))
	keys := lib.SortedKeys(dirCount.size)

	for _, key := range keys[:5] {
		fmt.Printf("%.1fMB, %s\n", float64(dirCount.size[key])/1e6, key)
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
		fmt.Printf("%s, %v\n", dir, err)
		return nil
	}
	return info
}

