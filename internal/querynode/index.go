// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

/*

#cgo CFLAGS: -I../core/output/include

#cgo LDFLAGS: -L../core/output/lib -lmilvus_segcore -Wl,-rpath=../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type IndexConfig struct{}

func (s *Segment) buildIndex(collection *Collection) commonpb.Status {
	/*
		int
		BuildIndex(CCollection c_collection, CSegmentBase c_segment);
	*/
	var status = C.BuildIndex(collection.collectionPtr, s.segmentPtr)
	if status != 0 {
		return commonpb.Status{ErrorCode: commonpb.ErrorCode_BuildIndexError}
	}
	return commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
}

func (s *Segment) dropIndex(fieldID int64) commonpb.Status {
	// WARN: Not support yet

	return commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
}
