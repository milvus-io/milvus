package model

import (
	"testing"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

var (
	segmentID = typeutil.UniqueID(1)
	buildID   = typeutil.UniqueID(1)

	segmentIdxPb = &pb.SegmentIndexInfo{
		CollectionID: colID,
		PartitionID:  partID,
		SegmentID:    segmentID,
		FieldID:      fieldID,
		IndexID:      indexID,
		BuildID:      buildID,
		EnableIndex:  true,
		CreateTime:   1,
	}

	indexModel2 = &Index{
		CollectionID: colID,
		IndexID:      indexID,
		FieldID:      fieldID,
		SegmentIndexes: map[int64]SegmentIndex{
			segmentID: {
				Segment: Segment{
					SegmentID:   segmentID,
					PartitionID: partID,
				},
				BuildID:     buildID,
				EnableIndex: true,
				CreateTime:  1,
			},
		},
	}
)

func TestUnmarshalSegmentIndexModel(t *testing.T) {
	ret := UnmarshalSegmentIndexModel(segmentIdxPb)
	assert.Equal(t, indexModel2, ret)
	assert.Nil(t, UnmarshalSegmentIndexModel(nil))
}
