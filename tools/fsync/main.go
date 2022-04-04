package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
)

const (
	// BATCH is the number of write operations to repeat
	BATCH = 10000
	// BUCKETS is the number of parallel write jobs
	BUCKETS = 16
)

var data []byte
var fs []*os.File

var parallel = flag.Bool("parallel", false, "whether to use parallel writes")
var size = flag.Int("size", 64*1024, "size of each write in bytes")

func init() {
	flag.Parse()
	data = make([]byte, *size)
	if *parallel {
		fs = make([]*os.File, BUCKETS)
		for i := 0; i < len(fs); i++ {
			f, err := os.Create(fmt.Sprintf("%v.dat", i))
			if err != nil {
				panic(err)
			}
			fs[i] = f
		}
	} else {
		fs = make([]*os.File, 1)
		f, err := os.Create("test.dat")
		if err != nil {
			panic(err)
		}
		fs[0] = f
	}
}

func main() {
	if *parallel {
		parallelWrite()
	} else {
		sequentialWrite()
	}
	for _, f := range fs {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}
}

func sequentialWrite() {
	for i := 0; i < BATCH; i++ {
		for j := 0; j < BUCKETS; j++ {
			if _, err := fs[0].Write(data); err != nil {
				panic(err)
			}
		}
		if err := fs[0].Sync(); err != nil {
			panic(err)
		}
	}
}

func parallelWrite() {
	var wg sync.WaitGroup
	if len(fs) != BUCKETS {
		panic("unexpected file count")
	}
	for _, f := range fs {
		wg.Add(1)
		gf := f
		go func() {
			for i := 0; i < BATCH; i++ {
				if _, err := gf.Write(data); err != nil {
					panic(err)
				}
				if err := gf.Sync(); err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
