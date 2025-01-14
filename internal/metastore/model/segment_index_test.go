package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
)

var (
	segmentID = int64(1)
	buildID   = int64(1)

	segmentIdxPb = &indexpb.SegmentIndex{
		CollectionID:  colID,
		PartitionID:   partID,
		SegmentID:     segmentID,
		NumRows:       1025,
		IndexID:       indexID,
		BuildID:       buildID,
		NodeID:        0,
		IndexVersion:  0,
		State:         commonpb.IndexState_Finished,
		FailReason:    "",
		IndexFileKeys: nil,
		Deleted:       false,
		CreateTime:    1,
		SerializeSize: 0,
	}

	indexModel2 = &SegmentIndex{
		CollectionID:        colID,
		PartitionID:         partID,
		SegmentID:           segmentID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IndexVersion:        0,
		IsDeleted:           false,
		CreatedUTCTime:      1,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
	}
)

func TestUnmarshalSegmentIndexModel(t *testing.T) {
	ret := UnmarshalSegmentIndexModel(segmentIdxPb)
	assert.Equal(t, indexModel2.SegmentID, ret.SegmentID)
	assert.Nil(t, UnmarshalSegmentIndexModel(nil))
}
