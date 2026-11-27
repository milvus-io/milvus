package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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

func TestSegmentIndex_ManifestPublishedIsProcessLocal(t *testing.T) {
	original := &SegmentIndex{BuildID: 1000, ManifestPublished: true}
	cloned := CloneSegmentIndex(original)
	assert.True(t, cloned.ManifestPublished)
	cloned.ManifestPublished = false
	assert.True(t, original.ManifestPublished)

	restored := UnmarshalSegmentIndexModel(MarshalSegmentIndexModel(original))
	assert.Equal(t, original.BuildID, restored.BuildID)
	assert.False(t, restored.ManifestPublished, "catalog reload must not inherit manifest placement")
}

func TestSegmentIndexEncryptionPersistence(t *testing.T) {
	for _, state := range []indexpb.IndexEncryptionState{indexpb.IndexEncryptionState_INDEX_ENCRYPTION_UNKNOWN, indexpb.IndexEncryptionState_INDEX_ENCRYPTION_PLAIN, indexpb.IndexEncryptionState_INDEX_ENCRYPTION_ENCRYPTED} {
		t.Run(state.String(), func(t *testing.T) {
			original := &SegmentIndex{BuildID: 10, EncryptionState: state}
			wire, err := proto.Marshal(MarshalSegmentIndexModel(original))
			assert.NoError(t, err)
			decoded := &indexpb.SegmentIndex{}
			assert.NoError(t, proto.Unmarshal(wire, decoded))
			restored := UnmarshalSegmentIndexModel(decoded)
			assert.Equal(t, state, CloneSegmentIndex(restored).EncryptionState)
			summary := MarshalSegmentIndexSummary(SummarizeSegmentIndex(restored), 100)
			assert.Equal(t, state, SegmentIndexSummaryFromProto(&datapb.SegmentInfo{}, summary).EncryptionState)
		})
	}
	assert.Equal(t, indexpb.IndexEncryptionState_INDEX_ENCRYPTION_UNKNOWN, UnmarshalSegmentIndexModel(&indexpb.SegmentIndex{BuildID: 10}).EncryptionState)
}
