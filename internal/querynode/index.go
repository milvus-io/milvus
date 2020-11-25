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
		return commonpb.Status{ErrorCode: commonpb.ErrorCode_BUILD_INDEX_ERROR}
	}
	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
}

func (s *Segment) dropIndex(fieldName string) commonpb.Status {
	// WARN: Not support yet

	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
}
