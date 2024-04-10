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

package analysiscgowrapper

/*
//libdir=/home/zc/work/milvus/internal/core/output/lib
//includedir=/home/zc/work/milvus/internal/core/output/include
//
//Libs: -L${libdir} -lmilvus_indexbuilder
//Cflags: -I${includedir
#cgo pkg-config: milvus_indexbuilder

#include <stdlib.h>	// free
#include "indexbuilder/analysis_c.h"
*/
import "C"

import (
	"context"
)

type CodecAnalysis interface {
	Delete() error
	CleanLocalData() error
	UpLoad(segmentIDs []int64) (string, map[int64]string, error)
}

func Analysis(ctx context.Context, analysisInfo *AnalysisInfo) (CodecAnalysis, error) {
	var analysisPtr C.CAnalysis
	status := C.Analysis(&analysisPtr, analysisInfo.cAnalysisInfo)
	if err := HandleCStatus(&status, "failed to analysis task"); err != nil {
		return nil, err
	}

	analysis := &CgoAnalysis{
		analysisPtr: analysisPtr,
		close:       false,
	}

	return analysis, nil
}

type CgoAnalysis struct {
	analysisPtr C.CAnalysis
	close       bool
}

func (ca *CgoAnalysis) Delete() error {
	if ca.close {
		return nil
	}
	// Todo: delete in segcore
	status := C.DeleteAnalysis(ca.analysisPtr)
	ca.close = true
	return HandleCStatus(&status, "failed to delete analysis")
	//return nil
}

func (ca *CgoAnalysis) CleanLocalData() error {
	status := C.CleanAnalysisLocalData(ca.analysisPtr)
	return HandleCStatus(&status, "failed to clean cached data on disk")
}

func (ca *CgoAnalysis) UpLoad(segmentIDs []int64) (string, map[int64]string, error) {
	status := C.SerializeAnalysisAndUpLoad(ca.analysisPtr)
	if err := HandleCStatus(&status, "failed to upload analysis result"); err != nil {
		return "", nil, err
	}

	centroidsFile, err := GetCentroidsFile()
	if err != nil {
		return "", nil, err
	}

	segmentsOffsetMapping := make(map[int64]string)
	for _, segID := range segmentIDs {
		offsetMappingFile, err := GetSegmentOffsetMapping(segID)
		if err != nil {
			return "", nil, err
		}
		segmentsOffsetMapping[segID] = offsetMappingFile
	}

	return centroidsFile, segmentsOffsetMapping, nil
}
