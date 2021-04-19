package reader

/*

#cgo CFLAGS: -I../core/output/include

#cgo LDFLAGS: -L../core/output/lib -lmilvus_dog_segment -Wl,-rpath=../core/output/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"
import (
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
)

type IndexConfig struct{}

func (s *Segment) buildIndex(collection* Collection) msgPb.Status {
	/*
	int
	BuildIndex(CCollection c_collection, CSegmentBase c_segment);
	*/
	var status = C.BuildIndex(collection.CollectionPtr, s.SegmentPtr)
	if status != 0 {
		return msgPb.Status{ErrorCode: msgPb.ErrorCode_BUILD_INDEX_ERROR}
	}
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (s *Segment) dropIndex(fieldName string) msgPb.Status {
	// WARN: Not support yet

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}
