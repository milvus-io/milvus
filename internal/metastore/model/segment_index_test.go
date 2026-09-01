package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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
	assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, pb.IndexStorePathVersion)
	restored := UnmarshalSegmentIndexModel(pb)
	assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, restored.IndexStorePathVersion)
}

func TestSegmentIndex_MarshalUnmarshal_LegacyDefaultZero(t *testing.T) {
	pb := &indexpb.SegmentIndex{
		CollectionID: 100,
		BuildID:      1000,
	}
	restored := UnmarshalSegmentIndexModel(pb)
	assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, restored.IndexStorePathVersion)
}

func TestSegmentIndex_Clone_PreservesPathVersion(t *testing.T) {
	original := &SegmentIndex{
		CollectionID:          100,
		BuildID:               1000,
		IndexStorePathVersion: 1,
	}
	cloned := CloneSegmentIndex(original)
	assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, cloned.IndexStorePathVersion)
	cloned.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED
	assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, original.IndexStorePathVersion)
}

// The whole migration rests on records written before manifest publication
// decoding as unpublished: that default is what makes the pre-existing etcd rows
// identify themselves as the backlog to backfill.
func TestSegmentIndex_ManifestPublished_LegacyRecordDecodesUnpublished(t *testing.T) {
	restored := UnmarshalSegmentIndexModel(&indexpb.SegmentIndex{
		CollectionID: 100,
		BuildID:      1000,
	})
	assert.False(t, restored.ManifestPublished)
	assert.False(t, (*SegmentIndex)(nil).GetManifestPublished())
}

// The flag has to survive every hop a record makes - marshal to etcd, decode
// back, and the clone that every state transition goes through - or a published
// entry would reappear as backfill work.
func TestSegmentIndex_ManifestPublished_SurvivesRoundTripAndClone(t *testing.T) {
	original := &SegmentIndex{
		CollectionID:      100,
		BuildID:           1000,
		ManifestPublished: true,
	}
	pb := MarshalSegmentIndexModel(original)
	assert.True(t, pb.GetManifestPublished())

	restored := UnmarshalSegmentIndexModel(pb)
	assert.True(t, restored.ManifestPublished)
	assert.True(t, CloneSegmentIndex(original).GetManifestPublished())
}
