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
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type IndexConfig struct{}

func (s *Segment) buildIndex(collection* Collection) commonpb.Status {
	/*
	int
	BuildIndex(CCollection c_collection, CSegmentBase c_segment);
	*/
	var status = C.BuildIndex(collection.CollectionPtr, s.SegmentPtr)
	if status != 0 {
		return commonpb.Status{ErrorCode: commonpb.ErrorCode_BUILD_INDEX_ERROR}
	}
	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
}

func (s *Segment) dropIndex(fieldName string) commonpb.Status {
	// WARN: Not support yet

	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
}


func (node *QueryNode) UpdateIndexes(collection *Collection, indexConfig *string) {
	/*
		void
		UpdateIndexes(CCollection c_collection, const char *index_string);
	*/
	cCollectionPtr := collection.CollectionPtr
	cIndexConfig := C.CString(*indexConfig)
	C.UpdateIndexes(cCollectionPtr, cIndexConfig)
}
