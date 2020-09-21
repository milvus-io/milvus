package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/pivotal-golang/bytefmt"
	"log"
	"math/rand"
	"os"
	minio "storage/internal/minio"
	tikv "storage/internal/tikv"
	"storage/pkg/types"
	"sync"
	"sync/atomic"
	"time"
)

// Global variables
var durationSecs, threads, loops, numVersion, batchOpSize int
var valueSize uint64
var valueData []byte
var batchValueData [][]byte
var counter, totalKeyCount, keyNum int32
var endTime, setFinish, getFinish, deleteFinish time.Time
var totalKeys [][]byte

var logFileName = "benchmark.log"
var logFile *os.File

var store types.Store
var wg sync.WaitGroup

func runSet() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&keyNum, 1)
		key := []byte(fmt.Sprint("key", num))
		for ver := 1; ver <= numVersion; ver++ {
			atomic.AddInt32(&counter, 1)
			err := store.Set(context.Background(), key, valueData, uint64(ver))
			if err != nil {
				log.Fatalf("Error setting key %s, %s", key, err.Error())
				//atomic.AddInt32(&setCount, -1)
			}
		}
	}
	// Remember last done time
	setFinish = time.Now()
	wg.Done()
}

func runBatchSet() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&keyNum, int32(batchOpSize))
		keys := make([][]byte, batchOpSize)
		for n := batchOpSize; n > 0; n-- {
			keys[n-1] = []byte(fmt.Sprint("key", num-int32(n)))
		}
		for ver := 1; ver <= numVersion; ver++ {
			atomic.AddInt32(&counter, 1)
			err := store.BatchSet(context.Background(), keys, batchValueData, uint64(numVersion))
			if err != nil {
				log.Fatalf("Error setting batch keys %s %s", keys, err.Error())
				//atomic.AddInt32(&batchSetCount, -1)
			}
		}
	}
	setFinish = time.Now()
	wg.Done()
}

func runGet() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&counter, 1)
		//num := atomic.AddInt32(&keyNum, 1)
		//key := []byte(fmt.Sprint("key", num))
		num = num % totalKeyCount
		key := totalKeys[num]
		_, err := store.Get(context.Background(), key, uint64(numVersion))
		if err != nil {
			log.Fatalf("Error getting key %s, %s", key, err.Error())
			//atomic.AddInt32(&getCount, -1)
		}
	}
	// Remember last done time
	getFinish = time.Now()
	wg.Done()
}

func runBatchGet() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&keyNum, int32(batchOpSize))
		//keys := make([][]byte, batchOpSize)
		//for n := batchOpSize; n > 0; n-- {
		//	keys[n-1] = []byte(fmt.Sprint("key", num-int32(n)))
		//}
		end := num % totalKeyCount
		if end < int32(batchOpSize) {
			end = int32(batchOpSize)
		}
		start := end - int32(batchOpSize)
		keys := totalKeys[start:end]
		atomic.AddInt32(&counter, 1)
		_, err := store.BatchGet(context.Background(), keys, uint64(numVersion))
		if err != nil {
			log.Fatalf("Error getting key %s, %s", keys, err.Error())
			//atomic.AddInt32(&batchGetCount, -1)
		}
	}
	// Remember last done time
	getFinish = time.Now()
	wg.Done()
}

func runDelete() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&counter, 1)
		//num := atomic.AddInt32(&keyNum, 1)
		//key := []byte(fmt.Sprint("key", num))
		num = num % totalKeyCount
		key := totalKeys[num]
		err := store.Delete(context.Background(), key, uint64(numVersion))
		if err != nil {
			log.Fatalf("Error getting key %s, %s", key, err.Error())
			//atomic.AddInt32(&deleteCount, -1)
		}
	}
	// Remember last done time
	deleteFinish = time.Now()
	wg.Done()
}

func runBatchDelete() {
	for time.Now().Before(endTime) {
		num := atomic.AddInt32(&keyNum, int32(batchOpSize))
		//keys := make([][]byte, batchOpSize)
		//for n := batchOpSize; n > 0; n-- {
		//	keys[n-1] = []byte(fmt.Sprint("key", num-int32(n)))
		//}
		end := num % totalKeyCount
		if end < int32(batchOpSize) {
			end = int32(batchOpSize)
		}
		start := end - int32(batchOpSize)
		keys := totalKeys[start:end]
		atomic.AddInt32(&counter, 1)
		err := store.BatchDelete(context.Background(), keys, uint64(numVersion))
		if err != nil {
			log.Fatalf("Error getting key %s, %s", keys, err.Error())
			//atomic.AddInt32(&batchDeleteCount, -1)
		}
	}
	// Remember last done time
	getFinish = time.Now()
	wg.Done()
}

func main() {
	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.IntVar(&durationSecs, "d", 5, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	var sizeArg string
	var storeType string
	myflag.StringVar(&sizeArg, "z", "1K", "Size of objects in bytes with postfix K, M, and G")
	myflag.StringVar(&storeType, "s", "tikv", "Storage type, tikv or minio")
	myflag.IntVar(&numVersion, "v", 1, "Max versions for each key")
	myflag.IntVar(&batchOpSize, "b", 100, "Batch operation kv pair number")

	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	var err error
	if valueSize, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}
	switch storeType {
	case "tikv":
		//var (
		//	pdAddr = []string{"127.0.0.1:2379"}
		//	conf   = config.Default()
		//)
		store, err = tikv.NewTikvStore(context.Background())
		if err != nil {
			log.Fatalf("Error when creating storage " + err.Error())
		}
	case "minio":
		store, err = minio.NewMinioStore(context.Background())
		if err != nil {
			log.Fatalf("Error when creating storage " + err.Error())
		}
	default:
		log.Fatalf("Not supported storage type")
	}
	logFile, err = os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		log.Fatalf("Prepare log file error, " + err.Error())
	}

	// Echo the parameters
	log.Printf("Benchmark log will write to file %s\n", logFile.Name())
	fmt.Fprint(logFile, fmt.Sprintf("Parameters: duration=%d, threads=%d, loops=%d, valueSize=%s, batchSize=%d, versions=%d\n", durationSecs, threads, loops, sizeArg, batchOpSize, numVersion))

	// Init test data
	valueData = make([]byte, valueSize)
	rand.Read(valueData)
	hasher := md5.New()
	hasher.Write(valueData)

	batchValueData = make([][]byte, batchOpSize)
	for i := range batchValueData {
		batchValueData[i] = make([]byte, valueSize)
		rand.Read(batchValueData[i])
		hasher := md5.New()
		hasher.Write(batchValueData[i])
	}

	// Loop running the tests
	for loop := 1; loop <= loops; loop++ {

		// reset counters
		counter = 0
		keyNum = 0
		totalKeyCount = 0
		totalKeys = nil

		// Run the set case
		startTime := time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runSet()
		}
		wg.Wait()

		setTime := setFinish.Sub(startTime).Seconds()

		bps := float64(uint64(counter)*valueSize) / setTime
		fmt.Fprint(logFile, fmt.Sprintf("Loop %d: PUT time %.1f secs, kv pairs = %d, speed = %sB/sec, %.1f operations/sec, %.1f kv/sec.\n",
			loop, setTime, counter, bytefmt.ByteSize(uint64(bps)), float64(counter)/setTime, float64(counter)/setTime))

		// Run the batchSet case
		// key seq start from setCount
		counter = 0
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runBatchSet()
		}
		wg.Wait()

		setTime = setFinish.Sub(startTime).Seconds()
		bps = float64(uint64(counter)*valueSize*uint64(batchOpSize)) / setTime
		fmt.Fprint(logFile, fmt.Sprintf("Loop %d: BATCH PUT time %.1f secs, batchs = %d, kv pairs = %d, speed = %sB/sec, %.1f operations/sec, %.1f kv/sec.\n",
			loop, setTime, counter, counter*int32(batchOpSize), bytefmt.ByteSize(uint64(bps)), float64(counter)/setTime, float64(counter * int32(batchOpSize))/setTime))

		// Record all test keys
		totalKeyCount = keyNum
		totalKeys = make([][]byte, totalKeyCount)
		for i := int32(0); i < totalKeyCount; i++ {
			totalKeys[i] = []byte(fmt.Sprint("key", i))
		}

		// Run the get case
		counter = 0
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runGet()
		}
		wg.Wait()

		getTime := getFinish.Sub(startTime).Seconds()
		bps = float64(uint64(counter)*valueSize) / getTime
		fmt.Fprint(logFile, fmt.Sprintf("Loop %d: GET time %.1f secs, kv pairs = %d, speed = %sB/sec, %.1f operations/sec, %.1f kv/sec.\n",
			loop, getTime, counter, bytefmt.ByteSize(uint64(bps)), float64(counter)/getTime, float64(counter)/getTime))

		// Run the batchGet case
		counter = 0
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runBatchGet()
		}
		wg.Wait()

		getTime = getFinish.Sub(startTime).Seconds()
		bps = float64(uint64(counter)*valueSize*uint64(batchOpSize)) / getTime
		fmt.Fprint(logFile, fmt.Sprintf("Loop %d: BATCH GET time %.1f secs, batchs = %d, kv pairs = %d, speed = %sB/sec, %.1f operations/sec, %.1f kv/sec.\n",
			loop, getTime, counter, counter*int32(batchOpSize), bytefmt.ByteSize(uint64(bps)), float64(counter)/getTime, float64(counter * int32(batchOpSize))/getTime))

		// Run the delete case
		counter = 0
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runDelete()
		}
		wg.Wait()

		deleteTime := deleteFinish.Sub(startTime).Seconds()
		bps = float64(uint64(counter)*valueSize) / deleteTime
		fmt.Fprint(logFile, fmt.Sprintf("Loop %d: Delete time %.1f secs, kv pairs = %d, %.1f operations/sec, %.1f kv/sec.\n",
			loop, deleteTime, counter, float64(counter)/deleteTime, float64(counter)/deleteTime))

		// Run the batchDelete case
		counter = 0
		startTime = time.Now()
		endTime = startTime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			wg.Add(1)
			go runBatchDelete()
		}
		wg.Wait()

		deleteTime = setFinish.Sub(startTime).Seconds()
		bps = float64(uint64(counter)*valueSize*uint64(batchOpSize)) / setTime
		fmt.Fprint(logFile, fmt.Sprintf("Loop %d: BATCH DELETE time %.1f secs, batchs = %d, kv pairs = %d, %.1f operations/sec, %.1f kv/sec.\n",
			loop, setTime, counter, counter*int32(batchOpSize), float64(counter)/setTime, float64(counter * int32(batchOpSize))/setTime))

		// Print line mark
		lineMark := "\n"
		fmt.Fprint(logFile, lineMark)

		// Clear test data
		err = store.BatchDelete(context.Background(), totalKeys, uint64(numVersion))
		if err != nil {
			log.Print("Clean test data error " + err.Error())
		}
	}
	log.Print("Benchmark test done.")
}
