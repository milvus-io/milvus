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
//libdir=/home/zc/work/milvus/internal/core/output/lib
//includedir=/home/zc/work/milvus/internal/core/output/include
//
//Libs: -L${libdir} -lmilvus_clustering
//Cflags: -I${includedir
#cgo pkg-config: milvus_clustering

#include <stdlib.h>	// free
#include "clustering/analyze_c.h"
*/
import "C"

import (
	"context"
)

type CodecAnalyze interface {
	Delete() error
}

func Analyze(ctx context.Context, analyzeInfo *AnalyzeInfo) (CodecAnalyze, error) {
	var analyzePtr C.CAnalyze
	status := C.Analyze(&analyzePtr, analyzeInfo.cAnalyzeInfo)
	if err := HandleCStatus(&status, "failed to analyze task"); err != nil {
		return nil, err
	}

	analyze := &CgoAnalyze{
		analyzePtr: analyzePtr,
		close:      false,
	}

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
	status := C.DeleteAnalyze(ca.analyzePtr)
	ca.close = true
	return HandleCStatus(&status, "failed to delete analyze")
	// return nil
}
