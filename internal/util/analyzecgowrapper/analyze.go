// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzecgowrapper

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>	// free
#include "clustering/analyze_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/clusteringpb"
)

type CodecAnalyze interface {
	Delete() error
	GetResult(size int) (string, int64, []string, []int64, error)
}

func Analyze(ctx context.Context, analyzeInfo *clusteringpb.AnalyzeInfo) (CodecAnalyze, error) {
	analyzeInfoBlob, err := proto.Marshal(analyzeInfo)
	if err != nil {
		log.Ctx(ctx).Warn("marshal analyzeInfo failed",
			zap.Int64("buildID", analyzeInfo.GetBuildID()),
			zap.Error(err))
		return nil, err
	}
	var analyzePtr C.CAnalyze
	status := C.Analyze(&analyzePtr, (*C.uint8_t)(unsafe.Pointer(&analyzeInfoBlob[0])), (C.uint64_t)(len(analyzeInfoBlob)))
	if err := HandleCStatus(&status, "failed to analyze task"); err != nil {
		return nil, err
	}

	analyze := &CgoAnalyze{
		analyzePtr: analyzePtr,
		close:      false,
	}

	runtime.SetFinalizer(analyze, func(ca *CgoAnalyze) {
		if ca != nil && !ca.close {
			log.Error("there is leakage in analyze object, please check.")
		}
	})

	return analyze, nil
}

type CgoAnalyze struct {
	analyzePtr C.CAnalyze
	close      bool
}

func (ca *CgoAnalyze) Delete() error {
	if ca.close {
		return nil
	}
	var status C.CStatus
	if ca.analyzePtr != nil {
		status = C.DeleteAnalyze(ca.analyzePtr)
	}
	ca.close = true
	return HandleCStatus(&status, "failed to delete analyze")
}

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
	if err := HandleCStatus(&status, "failed to delete analyze"); err != nil {
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
