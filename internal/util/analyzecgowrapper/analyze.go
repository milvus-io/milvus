// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package analyzecgowrapper

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "clustering/analyze_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"sync"
	"unsafe"

	"google.golang.org/protobuf/proto"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
)

type CodecAnalyze interface {
	Delete() error
	GetResult(size int) (string, int64, []string, []int64, error)
}

// CgoAnalyze wraps the C pointer
type CgoAnalyze struct {
	mu         sync.Mutex
	analyzePtr C.CAnalyze
}

var (
	analyzeQueue chan analyzeRequest
	once         sync.Once
)

type analyzeRequest struct {
	info   *clusteringpb.AnalyzeInfo
	ctx    context.Context
	result chan analyzeResult
}

type analyzeResult struct {
	analyze *CgoAnalyze
	err     error
}

// Analyze serializes calls to C.Analyze on a dedicated OS thread
func Analyze(ctx context.Context, analyzeInfo *clusteringpb.AnalyzeInfo) (CodecAnalyze, error) {
	initAnalyzeWorker()

	req := analyzeRequest{
		info:   analyzeInfo,
		ctx:    ctx,
		result: make(chan analyzeResult, 1),
	}

	// send request to the worker
	analyzeQueue <- req

	select {
	case res := <-req.result:
		return res.analyze, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// initAnalyzeWorker initializes the analysis worker once
func initAnalyzeWorker() {
	once.Do(func() {
		analyzeQueue = make(chan analyzeRequest, 1) // buffer 1 sufficient for single-task concurrency

		go func() {
			// lock this goroutine to a single OS thread
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			for req := range analyzeQueue {
				res := runAnalyze(req.ctx, req.info)
				req.result <- res
			}
		}()
	})
}

// runAnalyze performs the actual C.Analyze call
func runAnalyze(ctx context.Context, analyzeInfo *clusteringpb.AnalyzeInfo) analyzeResult {
	analyzeInfoBlob, err := proto.Marshal(analyzeInfo)
	if err != nil {
		mlog.Warn(ctx, "marshal analyzeInfo failed",
			mlog.FieldBuildID(analyzeInfo.GetBuildID()),
			mlog.Err(err))
		return nil, err
	}

	var analyzePtr C.CAnalyze
	status := C.Analyze(
		&analyzePtr,
		(*C.uint8_t)(unsafe.Pointer(&analyzeInfoBlob[0])),
		(C.uint64_t)(len(analyzeInfoBlob)),
	)

	if err := HandleCStatus(&status, "failed to analyze task"); err != nil {
		return analyzeResult{err: err}
	}

	analyze := &CgoAnalyze{
		analyzePtr: analyzePtr,
	}
	runtime.SetFinalizer(analyze, func(ca *CgoAnalyze) {
		ca.mu.Lock()
		leaked := ca.analyzePtr != nil
		ca.mu.Unlock()

		if leaked {
			mlog.Error(ctx, "CgoAnalyze object leaked, please check.")
		}
	})

	return analyzeResult{analyze: analyze}
}

func (ca *CgoAnalyze) Delete() error {
	ca.mu.Lock()
	ptr := ca.analyzePtr
	if ptr == nil {
		ca.mu.Unlock()
		return nil
	}
	ca.analyzePtr = nil
	ca.mu.Unlock()

	status := C.DeleteAnalyze(ptr)
	runtime.SetFinalizer(ca, nil)
	return HandleCStatus(&status, "failed to delete analyze")
}

// GetResult extracts results from the C.Analyze object
func (ca *CgoAnalyze) GetResult(size int) (string, int64, []string, []int64, error) {
	cOffsetMappingFilesPath := make([]unsafe.Pointer, size)
	cOffsetMappingFilesSize := make([]C.int64_t, size)
	cCentroidsFilePath := C.CString("")
	cCentroidsFileSize := C.int64_t(0)
	defer C.free(unsafe.Pointer(cCentroidsFilePath))

	status := C.GetAnalyzeResultMeta(ca.analyzePtr,
		&cCentroidsFilePath,
		&cCentroidsFileSize,
		unsafe.Pointer(&cOffsetMappingFilesPath[0]),
		&cOffsetMappingFilesSize[0],
	)
	if err := HandleCStatus(&status, "failed to get analyze result"); err != nil {
		return "", 0, nil, nil, err
	}

	offsetMappingFilesPath := make([]string, size)
	offsetMappingFilesSize := make([]int64, size)
	centroidsFilePath := C.GoString(cCentroidsFilePath)
	centroidsFileSize := int64(cCentroidsFileSize)

	for i := 0; i < size; i++ {
		offsetMappingFilesPath[i] = C.GoString((*C.char)(cOffsetMappingFilesPath[i]))
		offsetMappingFilesSize[i] = int64(cOffsetMappingFilesSize[i])
	}

	return centroidsFilePath, centroidsFileSize, offsetMappingFilesPath, offsetMappingFilesSize, nil
}
