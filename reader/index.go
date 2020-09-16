package reader

/*

#cgo CFLAGS: -I../core/include

#cgo LDFLAGS: -L../core/lib -lmilvus_dog_segment -Wl,-rpath=../core/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"
import (
	msgPb "github.com/czs007/suvlim/pkg/master/grpc/message"
)

type IndexConfig struct{}

func (s *Segment) buildIndex() msgPb.Status {
	/*C.BuildIndex
	int
	BuildIndex(CSegmentBase c_segment);
	*/
	var status = C.BuildIndex(s.SegmentPtr)
	if status != 0 {
		return msgPb.Status{ErrorCode: msgPb.ErrorCode_BUILD_INDEX_ERROR}
	}
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (s *Segment) dropIndex(fieldName string) msgPb.Status {
	// WARN: Not support yet

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}
