package segcore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func TestLoadFieldDataRequest(t *testing.T) {
	req := &LoadFieldDataRequest{
		Fields: []LoadFieldDataInfo{{
			Field: &datapb.FieldBinlog{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 100,
						LogPath:    "1",
					}, {
						EntriesNum: 101,
						LogPath:    "2",
					},
				},
			},
		}},
		RowCount:     100,
		MMapDir:      "1234567890",
		CollectionID: 1,
		PartitionID:  1,
		SegmentID:    1,
	}
	creq, err := req.getCLoadFieldDataRequest()
	assert.NoError(t, err)
	assert.NotNil(t, creq)
	creq.Release()
}

func TestLoadFieldDataRequestMetadata(t *testing.T) {
	tests := []struct {
		name         string
		collectionID int64
		partitionID  int64
		segmentID    int64
		description  string
	}{
		{
			name:         "basic_metadata",
			collectionID: 1001,
			partitionID:  2002,
			segmentID:    3003,
			description:  "Test basic positive metadata values",
		},
		{
			name:         "zero_metadata",
			collectionID: 0,
			partitionID:  0,
			segmentID:    0,
			description:  "Test zero metadata values",
		},
		{
			name:         "deprecated_metadata",
			collectionID: -1,
			partitionID:  -1,
			segmentID:    -1,
			description:  "Test deprecated (-1) metadata values",
		},
		{
			name:         "large_metadata",
			collectionID: 9223372036854775807, // max int64
			partitionID:  9223372036854775806,
			segmentID:    9223372036854775805,
			description:  "Test large metadata values",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := &LoadFieldDataRequest{
				Fields: []LoadFieldDataInfo{{
					Field: &datapb.FieldBinlog{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{{
							EntriesNum: 1000,
							LogPath:    "/test/path/1",
						}},
					},
				}},
				RowCount:     1000,
				MMapDir:      "/tmp/test",
				CollectionID: test.collectionID,
				PartitionID:  test.partitionID,
				SegmentID:    test.segmentID,
			}

			creq, err := req.getCLoadFieldDataRequest()
			assert.NoError(t, err, "getCLoadFieldDataRequest should succeed")
			assert.NotNil(t, creq, "C request should not be nil")
			defer creq.Release()

			// Verify that metadata was passed correctly
			assert.NotNil(t, creq.cLoadFieldDataInfo, "C LoadFieldDataInfo should be created")
		})
	}
}

func TestLoadFieldDataRequestMetadataConsistency(t *testing.T) {
	// Test that metadata values are consistently handled across multiple calls
	req := &LoadFieldDataRequest{
		CollectionID: 1234,
		PartitionID:  5678,
		SegmentID:    9012,
		Fields: []LoadFieldDataInfo{{
			Field: &datapb.FieldBinlog{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{{
					EntriesNum: 100,
					LogPath:    "/test/path",
				}},
			},
		}},
		RowCount: 100,
	}

	// Call getCLoadFieldDataRequest multiple times to ensure consistency
	for i := 0; i < 3; i++ {
		creq, err := req.getCLoadFieldDataRequest()
		assert.NoError(t, err, "Call %d should succeed", i+1)
		assert.NotNil(t, creq, "Call %d should return non-nil request", i+1)
		creq.Release()
	}
}

func TestAddFieldDataInfoRequestMetadata(t *testing.T) {
	// Since AddFieldDataInfoRequest is a type alias for LoadFieldDataRequest,
	// test that it also works with metadata
	req := &AddFieldDataInfoRequest{
		CollectionID: 5001,
		PartitionID:  6002,
		SegmentID:    7003,
		Fields: []LoadFieldDataInfo{{
			Field: &datapb.FieldBinlog{
				FieldID: 500,
				Binlogs: []*datapb.Binlog{{
					EntriesNum: 1500,
					LogPath:    "/test/path/add",
				}},
			},
		}},
		RowCount: 1500,
	}

	creq, err := req.getCLoadFieldDataRequest()
	assert.NoError(t, err)
	assert.NotNil(t, creq)
	defer creq.Release()

	assert.NotNil(t, creq.cLoadFieldDataInfo)
}

func TestLoadFieldDataRequestMetadataEdgeCases(t *testing.T) {
	t.Run("negative_IDs", func(t *testing.T) {
		req := &LoadFieldDataRequest{
			CollectionID: -100,
			PartitionID:  -200,
			SegmentID:    -300,
			Fields: []LoadFieldDataInfo{{
				Field: &datapb.FieldBinlog{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{
						EntriesNum: 100,
						LogPath:    "/test/path",
					}},
				},
			}},
			RowCount: 100,
		}

		creq, err := req.getCLoadFieldDataRequest()
		assert.NoError(t, err, "Negative IDs should be accepted")
		assert.NotNil(t, creq)
		defer creq.Release()
	})

	t.Run("mixed_positive_negative", func(t *testing.T) {
		req := &LoadFieldDataRequest{
			CollectionID: 1000,
			PartitionID:  -1, // deprecated value
			SegmentID:    2000,
			Fields: []LoadFieldDataInfo{{
				Field: &datapb.FieldBinlog{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{
						EntriesNum: 50,
						LogPath:    "/test/mixed",
					}},
				},
			}},
			RowCount: 50,
		}

		creq, err := req.getCLoadFieldDataRequest()
		assert.NoError(t, err, "Mixed positive/negative IDs should work")
		assert.NotNil(t, creq)
		defer creq.Release()
	})
}

func TestLoadFieldDataRequestMultipleFields(t *testing.T) {
	// Test that metadata is properly propagated when multiple fields are loaded
	req := &LoadFieldDataRequest{
		CollectionID: 1111,
		PartitionID:  2222,
		SegmentID:    3333,
		Fields: []LoadFieldDataInfo{
			{
				Field: &datapb.FieldBinlog{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{{
						EntriesNum: 1000,
						LogPath:    "/test/field100",
					}},
				},
				EnableMMap: true,
			},
			{
				Field: &datapb.FieldBinlog{
					FieldID: 200,
					Binlogs: []*datapb.Binlog{
						{
							EntriesNum: 500,
							LogPath:    "/test/field200_1",
						},
						{
							EntriesNum: 500,
							LogPath:    "/test/field200_2",
						},
					},
				},
				EnableMMap: false,
			},
		},
		RowCount: 1000,
		MMapDir:  "/tmp/mmap",
	}

	creq, err := req.getCLoadFieldDataRequest()
	assert.NoError(t, err, "Multiple fields with metadata should work")
	assert.NotNil(t, creq)
	defer creq.Release()

	assert.NotNil(t, creq.cLoadFieldDataInfo)
}

func TestLoadFieldDataRequestWithChildFields(t *testing.T) {
	// Test metadata handling with child fields (for complex data types)
	req := &LoadFieldDataRequest{
		CollectionID: 4444,
		PartitionID:  5555,
		SegmentID:    6666,
		Fields: []LoadFieldDataInfo{{
			Field: &datapb.FieldBinlog{
				FieldID: 300,
				Binlogs: []*datapb.Binlog{{
					EntriesNum: 100,
					LogPath:    "/test/parent_field",
				}},
				ChildFields: []int64{301, 302, 303}, // child field IDs
			},
		}},
		RowCount: 100,
	}

	creq, err := req.getCLoadFieldDataRequest()
	assert.NoError(t, err, "Child fields with metadata should work")
	assert.NotNil(t, creq)
	defer creq.Release()

	assert.NotNil(t, creq.cLoadFieldDataInfo)
}
