// s3-benchmark.go
// Copyright (c) 2017 Wasabi Technology, Inc.

package main

import (
	"github.com/czs007/suvlim/storage/pkg"
	. "github.com/czs007/suvlim/storage/pkg/types"
	"crypto/md5"
	"flag"
	"fmt"
	"code.cloudfoundry.org/bytefmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync/atomic"
	"time"
	"context"
)

// Global variables
var duration_secs, threads int
var object_size uint64
var object_data []byte
var running_threads, upload_count, upload_slowdown_count int32
var endtime, upload_finish time.Time


func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
		logfile.Close()
	}
}

func _putFile(ctx context.Context, store Store){
    //objnum := atomic.AddInt32(&upload_count, 1)
	key := "collection_abc"
    err := store.PutRow(ctx, []byte(key), object_data, "abc", uint64(time.Now().Unix()))
    if err != nil {
	atomic.AddInt32(&upload_slowdown_count, 1)
    }

}

func runPutFile(thread_num int) {
	var store Store
	var err error
	ctx := context.Background()
	store, err = storage.NewStore(ctx, TIKVDriver)
	if err != nil {
		panic(err.Error())
	}

	for time.Now().Before(endtime) {
		_putFile(ctx, store)
	}

	// Remember last done time
	upload_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func main() {
	// Hello

	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.IntVar(&duration_secs, "d", 1, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")

	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	var err error
	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}

	logit(fmt.Sprintf("Parameters: duration=%d, threads=%d, size=%s",
		duration_secs, threads, sizeArg))

	// Initialize data for the bucket
	object_data = make([]byte, object_size)
	rand.Read(object_data)
	hasher := md5.New()
	hasher.Write(object_data)

	// reset counters
	upload_count = 0
	upload_slowdown_count = 0

	running_threads = int32(threads)

	// Run the upload case
	starttime := time.Now()
	endtime = starttime.Add(time.Second * time.Duration(duration_secs))

	for n := 1; n <= threads; n++ {
		go runPutFile(n)
	}

	// Wait for it to finish
	for atomic.LoadInt32(&running_threads) > 0 {
		time.Sleep(time.Millisecond)
	}
	upload_time := upload_finish.Sub(starttime).Seconds()

	bps := float64(uint64(upload_count)*object_size) / upload_time
	logit(fmt.Sprintf("PUT time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
		upload_time, upload_count, bytefmt.ByteSize(uint64(bps)), float64(upload_count)/upload_time, upload_slowdown_count))

	fmt.Println(" upload_count :", upload_count)

}
