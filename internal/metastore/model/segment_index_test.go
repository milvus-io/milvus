package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
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
		IndexType:     "HNSW",
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
		IndexType:           "HNSW",
	}
)

func TestUnmarshalSegmentIndexModel(t *testing.T) {
	ret := UnmarshalSegmentIndexModel(segmentIdxPb)
	assert.Equal(t, indexModel2.SegmentID, ret.SegmentID)
	assert.Nil(t, UnmarshalSegmentIndexModel(nil))
}

func TestSegmentIndex_MarshalUnmarshal_IndexStorePathVersion(t *testing.T) {
	original := &SegmentIndex{
		SegmentID:             1,
		CollectionID:          100,
		PartitionID:           200,
		BuildID:               1000,
		IndexVersion:          1,
		IndexStorePathVersion: 1,
		IndexType:             "HNSW",
	}
	pb := MarshalSegmentIndexModel(original)
	assert.Equal(t, int32(1), pb.IndexStorePathVersion)
	restored := UnmarshalSegmentIndexModel(pb)
	assert.Equal(t, int32(1), restored.IndexStorePathVersion)
}

func TestSegmentIndex_MarshalUnmarshal_LegacyDefaultZero(t *testing.T) {
	pb := &indexpb.SegmentIndex{
		CollectionID: 100,
		BuildID:      1000,
	}
	restored := UnmarshalSegmentIndexModel(pb)
	assert.Equal(t, int32(0), restored.IndexStorePathVersion)
}

func TestSegmentIndex_Clone_PreservesPathVersion(t *testing.T) {
	original := &SegmentIndex{
		CollectionID:          100,
		BuildID:               1000,
		IndexStorePathVersion: 1,
	}
	cloned := CloneSegmentIndex(original)
	assert.Equal(t, int32(1), cloned.IndexStorePathVersion)
	cloned.IndexStorePathVersion = 0
	assert.Equal(t, int32(1), original.IndexStorePathVersion)
}
