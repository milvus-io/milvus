// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/snapshotio"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// =========================== Test Helper Functions ===========================

func createTestSnapshotData() *snapshotstorage.SnapshotData {
	return &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			CreateTs:     1234567890,
			Name:         "test_snapshot",
			Description:  "test description",
			S3Location:   "s3://test-bucket/snapshot",
			PartitionIds: []int64{1, 2},
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 1, Name: "field1", DataType: schemapb.DataType_Int64},
					{FieldID: 2, Name: "field2", DataType: schemapb.DataType_FloatVector},
				},
			},
			NumShards:        2,
			NumPartitions:    2,
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			Properties:       []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:    1001,
				PartitionId:  1,
				SegmentLevel: 1,
				ChannelName:  "test_channel",
				NumOfRows:    100,
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum:    100,
								TimestampFrom: 1000,
								TimestampTo:   2000,
								LogPath:       "/path/to/binlog1",
								LogSize:       1024,
								LogID:         1,
								MemorySize:    2048,
							},
						},
					},
				},
				Deltalogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum:    10,
								TimestampFrom: 1500,
								TimestampTo:   2500,
								LogPath:       "/path/to/deltalog1",
								LogSize:       512,
								LogID:         2,
								MemorySize:    1024,
							},
						},
					},
				},
				Statslogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum:    5,
								TimestampFrom: 1000,
								TimestampTo:   2000,
								LogPath:       "/path/to/statslog1",
								LogSize:       256,
								LogID:         3,
								MemorySize:    512,
							},
						},
					},
				},
				Bm25Statslogs: []*datapb.FieldBinlog{
					{
						FieldID: 3,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum:    20,
								TimestampFrom: 1000,
								TimestampTo:   2000,
								LogPath:       "/path/to/bm25statslog1",
								LogSize:       128,
								LogID:         4,
								MemorySize:    256,
							},
						},
					},
				},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					100: {
						FieldID:    100,
						Version:    1,
						Files:      []string{"/text/index/file1", "/text/index/file2"},
						LogSize:    2048,
						MemorySize: 4096,
						BuildID:    5000,
					},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					200: {
						FieldID:                200,
						Version:                2,
						Files:                  []string{"/json/index/file1"},
						LogSize:                1024,
						MemorySize:             2048,
						BuildID:                6000,
						JsonKeyStatsDataFormat: 1,
					},
				},
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{
						SegmentID: 1001,
						FieldID:   2,
						IndexID:   2001,
						BuildID:   3001,
						IndexName: "test_index",
						IndexParams: []*commonpb.KeyValuePair{
							{Key: "metric_type", Value: "IP"},
							{Key: "index_type", Value: "IVF_FLAT"},
						},
						IndexFilePaths: []string{"/index/path1", "/index/path2"},
						SerializedSize: 4096,
						IndexVersion:   1,
						NumRows:        100,
						MemSize:        8192,
					},
				},
				StartPosition: &msgpb.MsgPosition{
					ChannelName: "test_channel",
					MsgID:       []byte{1, 2, 3, 4},
					MsgGroup:    "test_group",
					Timestamp:   1000,
				},
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: "test_channel",
					MsgID:       []byte{5, 6, 7, 8},
					MsgGroup:    "test_group",
					Timestamp:   2000,
				},
				StorageVersion: 2,
				IsSorted:       true,
			},
		},
		Indexes: []*indexpb.IndexInfo{
			{
				IndexID:   2001,
				FieldID:   2,
				IndexName: "test_index",
				IndexParams: []*commonpb.KeyValuePair{
					{Key: "metric_type", Value: "IP"},
					{Key: "index_type", Value: "IVF_FLAT"},
				},
			},
		},
	}
}

func TestSnapshotCommitTimestampRoundTrip(t *testing.T) {
	ctx := context.Background()
	for _, commit := range []uint64{0, 300} {
		for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
			t.Run(fmt.Sprintf("%s/%d", layout, commit), func(t *testing.T) {
				root := t.TempDir()
				cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
				data := &snapshotstorage.SnapshotData{
					SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 2},
					Collection:   &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "source"}, Partitions: map[string]int64{"default": 10}},
					Segments: []*datapb.SegmentDescription{
						{SegmentId: 20, PartitionId: 10, SegmentLevel: datapb.SegmentLevel_L1, CommitTimestamp: commit},
						{SegmentId: 30, PartitionId: common.AllPartitionsID, SegmentLevel: datapb.SegmentLevel_L0},
					},
				}
				uri, _, err := snapshotstorage.NewSnapshotWriter(cm).SaveToRootWithSize(ctx, data, root, layout)
				require.NoError(t, err)
				read, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, uri, true)
				require.NoError(t, err)
				commits := make(map[int64]uint64)
				for _, segment := range read.Segments {
					commits[segment.SegmentId] = segment.CommitTimestamp
				}
				require.Equal(t, map[int64]uint64{20: commit, 30: 0}, commits)
			})
		}
	}
}

func readManifestFileForTest(t *testing.T, cm storage.ChunkManager, manifestPath string, formatVersion int) []*datapb.SegmentDescription {
	t.Helper()
	data, err := cm.Read(context.Background(), snapshotstorage.NormalizeSnapshotObjectPath(manifestPath))
	require.NoError(t, err)
	segment, err := snapshotio.ParseSegmentManifest(data, formatVersion)
	require.NoError(t, err)
	return []*datapb.SegmentDescription{segment}
}

func readSnapshotMetadataForTest(t *testing.T, cm storage.ChunkManager, metadataPath string) *datapb.SnapshotMetadata {
	t.Helper()
	data, err := cm.Read(context.Background(), snapshotstorage.NormalizeSnapshotObjectPath(metadataPath))
	require.NoError(t, err)
	metadata, err := snapshotio.ParseSnapshotMetadata(data)
	require.NoError(t, err)
	return metadata
}

type bucketNameProvider interface {
	BucketName() string
}

type bucketChunkManagerTarget struct {
	storage.ChunkManager
	bucketNameProvider
}

type existChunkManagerTarget struct {
	storage.ChunkManager
}

// =========================== SnapshotWriter Tests ===========================

func TestSnapshotMetadata_ChannelSeekPositionsRoundTrip(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           101,
			Name:         "per_channel_snapshot",
			CollectionId: 100,
			CreateTs:     100,
			ChannelSeekPositions: []*msgpb.MsgPosition{
				{
					ChannelName: "by-dev-rootcoord-dml_0_100v0",
					Timestamp:   100,
					MsgID:       []byte{1, 2, 3},
				},
				{
					ChannelName: "by-dev-rootcoord-dml_1_100v1",
					Timestamp:   1000,
					MsgID:       []byte{4, 5, 6},
				},
			},
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test_collection"},
		},
	}

	metadataJSON, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(metadata)
	require.NoError(t, err)

	restored := &datapb.SnapshotMetadata{}
	err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(metadataJSON, restored)
	require.NoError(t, err)
	require.NotNil(t, restored.GetSnapshotInfo())

	positions := restored.GetSnapshotInfo().GetChannelSeekPositions()
	require.Len(t, positions, 2)
	assert.Equal(t, "by-dev-rootcoord-dml_0_100v0", positions[0].GetChannelName())
	assert.Equal(t, uint64(100), positions[0].GetTimestamp())
	assert.Equal(t, []byte{1, 2, 3}, positions[0].GetMsgID())
	assert.Equal(t, "by-dev-rootcoord-dml_1_100v1", positions[1].GetChannelName())
	assert.Equal(t, uint64(1000), positions[1].GetTimestamp())
	assert.Equal(t, []byte{4, 5, 6}, positions[1].GetMsgID())
}

func TestSnapshotMetadata_LegacySnapshotWithoutChannelSeekPositions(t *testing.T) {
	metadataJSON := []byte(`{
  "format_version": 3,
  "snapshot_info": {
    "name": "legacy_snapshot",
    "id": "99",
    "collection_id": "100",
    "partition_ids": ["1"],
    "create_ts": "12345"
  },
  "collection": {
    "schema": {
      "name": "legacy_collection"
    }
  },
  "manifest_list": [],
  "segment_ids": [],
  "build_ids": []
}`)

	metadata := &datapb.SnapshotMetadata{}
	err := protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(metadataJSON, metadata)
	require.NoError(t, err)
	require.NotNil(t, metadata.GetSnapshotInfo())
	assert.Equal(t, int64(12345), metadata.GetSnapshotInfo().GetCreateTs())
	assert.Empty(t, metadata.GetSnapshotInfo().GetChannelSeekPositions())
}

func TestSnapshotWriter_Save_RealAvro(t *testing.T) {
	// Use real ChunkManager and Avro operations, only mock storage layer
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()

	// Only mock storage layer Write operations, let Avro execute for real
	writeCallCount := 0
	mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).To(func(ctx context.Context, filePath string, content []byte) error {
		writeCallCount++
		// Verify that written content is not empty mock data
		assert.NotEmpty(t, content)
		if path.Ext(filePath) == ".json" {
			metadata, err := snapshotio.ParseSnapshotMetadata(content)
			require.NoError(t, err)
			assert.Equal(t, int32(6), metadata.GetFormatVersion())
		} else {
			_, err := snapshotio.ParseSegmentManifest(content, 6)
			require.NoError(t, err)
		}
		return nil
	}).Build()
	defer mockWrite.UnPatch()

	metadataPath, err := writer.Save(context.Background(), snapshotData)

	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)
	assert.Contains(t, metadataPath, "snapshots/100/metadata/")
	assert.Contains(t, metadataPath, ".json")
	assert.Equal(t, 2, writeCallCount) // manifest, metadata
}

func TestSnapshotWriter_SaveToRootWithSize(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	writer := snapshotstorage.NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()
	targetRoot := path.Join(cm.RootPath(), "export-root")

	metadataPath, totalBytes, err := writer.SaveToRootWithSize(
		ctx,
		snapshotData,
		targetRoot,
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	)
	require.NoError(t, err)
	manifestDir, expectedMetadataPath := snapshotstorage.GetSnapshotPaths(
		targetRoot,
		snapshotData.SnapshotInfo.GetCollectionId(),
		snapshotData.SnapshotInfo.GetId(),
	)
	assert.Equal(t, expectedMetadataPath, metadataPath)

	manifestPath := snapshotstorage.GetSegmentManifestPath(manifestDir, snapshotData.Segments[0].GetSegmentId())
	manifestBytes, err := cm.Size(ctx, manifestPath)
	require.NoError(t, err)
	metadataBytes, err := cm.Size(ctx, metadataPath)
	require.NoError(t, err)
	assert.Equal(t, manifestBytes+metadataBytes, totalBytes)
}

func TestSnapshotWriter_StagedMetadataPublication(t *testing.T) {
	prepare := func(t *testing.T) (*storage.LocalChunkManager, *snapshotstorage.SnapshotWriter, string, string, int64) {
		t.Helper()
		ctx := context.Background()
		cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		writer := snapshotstorage.NewSnapshotWriter(cm)
		snapshotData := createTestSnapshotData()
		targetRoot := path.Join(cm.RootPath(), "export-root")
		_, metadataPath := snapshotstorage.GetSnapshotPaths(
			targetRoot,
			snapshotData.SnapshotInfo.GetCollectionId(),
			snapshotData.SnapshotInfo.GetId(),
		)
		snapshotData.SnapshotInfo.S3Location = metadataPath
		stagingPath := snapshotstorage.GetSnapshotStagingMetadataPath(targetRoot)
		preparedPath, totalBytes, err := writer.SaveToRootWithSize(ctx, snapshotData, targetRoot, datapb.SnapshotLayout_SnapshotLayoutSelfContained)
		require.NoError(t, err)
		require.Equal(t, metadataPath, preparedPath)
		data, err := cm.Read(ctx, metadataPath)
		require.NoError(t, err)
		metadata, err := snapshotio.ParseSnapshotMetadata(data)
		require.NoError(t, err)
		require.NoError(t, cm.Remove(ctx, metadataPath))
		err = writer.PrepareMetadataWithStaging(ctx, metadata, metadataPath, stagingPath)
		require.NoError(t, err)
		return cm, writer, stagingPath, metadataPath, totalBytes
	}

	for _, mode := range []string{"marshal failure", "invalid metadata", "empty staging", "same path", "read failure", "read mismatch"} {
		t.Run("preparation/"+mode, func(t *testing.T) {
			cm, writer, staging, final, _ := prepare(t)
			data, err := cm.Read(context.Background(), staging)
			require.NoError(t, err)
			metadata, err := snapshotio.ParseSnapshotMetadata(data)
			require.NoError(t, err)
			want := merr.ErrServiceInternal
			switch mode {
			case "marshal failure":
				metadata.SnapshotInfo.Name = string([]byte{0xff})
			case "invalid metadata":
				metadata.Collection = nil
				want = merr.ErrDataIntegrity
			case "empty staging":
				staging = ""
			case "same path":
				staging = final
			case "read failure":
				want = merr.ErrServiceUnavailable
				defer mockey.Mock((*storage.LocalChunkManager).Read).Return(nil, want).Build().UnPatch()
			case "read mismatch":
				want = merr.ErrDataIntegrity
				defer mockey.Mock((*storage.LocalChunkManager).Read).Return([]byte("different"), nil).Build().UnPatch()
			}
			err = writer.PrepareMetadataWithStaging(context.Background(), metadata, final, staging)
			require.ErrorIs(t, err, want)
			exists, err := cm.Exist(context.Background(), final)
			require.NoError(t, err)
			require.False(t, exists, "preparation must never publish final metadata")
		})
	}

	t.Run("normal and already published", func(t *testing.T) {
		ctx := context.Background()
		cm, writer, stagingPath, metadataPath, totalBytes := prepare(t)
		exists, err := cm.Exist(ctx, metadataPath)
		require.NoError(t, err)
		assert.False(t, exists)

		metadataBytes, err := writer.CommitStagedMetadata(ctx, stagingPath, metadataPath, metadataPath)
		require.NoError(t, err)
		assert.Greater(t, metadataBytes, int64(0))
		assert.Greater(t, totalBytes, metadataBytes)

		secondBytes, err := writer.CommitStagedMetadata(ctx, stagingPath, metadataPath, metadataPath)
		require.NoError(t, err)
		assert.Equal(t, metadataBytes, secondBytes)
	})

	t.Run("write error after commit is verified", func(t *testing.T) {
		ctx := context.Background()
		cm, writer, stagingPath, metadataPath, _ := prepare(t)
		expected := merr.WrapErrIoTooManyRequests(metadataPath, errors.New("write response timed out"))
		var origin func(*storage.LocalChunkManager, context.Context, string, []byte) error
		mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).To(
			func(manager *storage.LocalChunkManager, writeCtx context.Context, filePath string, content []byte) error {
				if filePath != metadataPath {
					return origin(manager, writeCtx, filePath, content)
				}
				require.NoError(t, origin(manager, writeCtx, filePath, content))
				return expected
			}).Origin(&origin).Build()
		defer mockWrite.UnPatch()

		_, err := writer.CommitStagedMetadata(ctx, stagingPath, metadataPath, metadataPath)
		require.NoError(t, err)
		published, err := cm.Read(ctx, metadataPath)
		require.NoError(t, err)
		staged, err := cm.Read(ctx, stagingPath)
		require.NoError(t, err)
		assert.Equal(t, staged, published)
	})

	t.Run("different final metadata is rejected", func(t *testing.T) {
		ctx := context.Background()
		cm, writer, stagingPath, metadataPath, _ := prepare(t)
		require.NoError(t, cm.Write(ctx, metadataPath, []byte("different")))

		_, err := writer.CommitStagedMetadata(ctx, stagingPath, metadataPath, metadataPath)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("missing and corrupt staging metadata", func(t *testing.T) {
		ctx := context.Background()
		cm, writer, stagingPath, metadataPath, _ := prepare(t)
		require.NoError(t, cm.Remove(ctx, stagingPath))

		_, err := writer.CommitStagedMetadata(ctx, stagingPath, metadataPath, metadataPath)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		require.ErrorContains(t, err, "staged snapshot metadata object is missing")

		require.NoError(t, cm.Write(ctx, stagingPath, []byte("{")))
		_, err = writer.CommitStagedMetadata(ctx, stagingPath, metadataPath, metadataPath)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})
}

func TestSnapshotWriter_Save_DoesNotPublishMetadataAfterCancellation(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writeCallCount := 0
	mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).To(func(context.Context, string, []byte) error {
		writeCallCount++
		if writeCallCount == 1 {
			cancel()
		}
		return nil
	}).Build()
	defer mockWrite.UnPatch()

	_, err := writer.Save(ctx, snapshotData)

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, writeCallCount)
}

func TestSnapshotWriter_Save_StorageError(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()
	expectedError := merr.WrapErrIoTooManyRequests("snapshot object", errors.New("storage throttled"))

	mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).Return(expectedError).Build()
	defer mockWrite.UnPatch()

	_, err := writer.Save(context.Background(), snapshotData)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write")
	assert.Equal(t, merr.Code(expectedError), merr.Code(err))
	assert.True(t, merr.IsRetryableErr(err))
}

func TestSnapshotWriter_Save_EmptySegments(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()
	snapshotData.Segments = nil // Empty segments

	writeCallCount := 0
	mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).To(func(ctx context.Context, filePath string, content []byte) error {
		writeCallCount++
		return nil
	}).Build()
	defer mockWrite.UnPatch()

	metadataPath, err := writer.Save(context.Background(), snapshotData)

	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)
	// When there are no segments, only metadata file is written (no manifest files)
	assert.Equal(t, 1, writeCallCount) // Only metadata file is written
}

func TestSnapshotWriter_Drop_Success(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)

	// Mock metadata file content
	metadata := &datapb.SnapshotMetadata{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:       1,
			CreateTs: 1234567890,
			Name:     "test_snapshot",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{"manifest1.avro"},
	}
	opts := protojson.MarshalOptions{UseProtoNames: true}
	metadataJSON, _ := opts.Marshal(metadata)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	// Mock file operations - no longer need ListAllChunkWithPrefix
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataFilePath {
			return metadataJSON, nil
		}
		return []byte("mock-data"), nil
	}).Build()
	defer mockRead.UnPatch()

	mockMultiRemove := mockey.Mock((*storage.LocalChunkManager).MultiRemove).Return(nil).Build()
	defer mockMultiRemove.UnPatch()

	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).Return(nil).Build()
	defer mockRemove.UnPatch()

	err := writer.Drop(context.Background(), metadataFilePath)

	assert.NoError(t, err)
}

func TestSnapshotWriter_Drop_MetadataReadFailed(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	// Mock Read to return error (file not found)
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).Return(
		nil,
		fmt.Errorf("file not found"),
	).Build()
	defer mockRead.UnPatch()

	err := writer.Drop(context.Background(), metadataFilePath)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read metadata file")
}

func TestSnapshotWriter_Drop_EmptyPath(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)

	err := writer.Drop(context.Background(), "")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata file path cannot be empty")
}

// =========================== SnapshotReader Tests ===========================

func TestSnapshotReader_ReadSnapshot_Success(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := snapshotstorage.NewSnapshotReader(cm)

	metadata := &datapb.SnapshotMetadata{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:       1,
			CreateTs: 1234567890,
			Name:     "test_snapshot",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{"manifest1.avro"},
	}
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	metadataJSON, _ := marshalOpts.Marshal(metadata)

	// Generate valid manifest entry with all required fields (single record per file)
	manifestEntry := snapshotio.ManifestEntry{
		SegmentID:         1001,
		PartitionID:       1,
		SegmentLevel:      1,
		ChannelName:       "test_channel",
		NumOfRows:         100,
		BinlogFiles:       []snapshotio.AvroFieldBinlog{},
		DeltalogFiles:     []snapshotio.AvroFieldBinlog{},
		StatslogFiles:     []snapshotio.AvroFieldBinlog{},
		Bm25StatslogFiles: []snapshotio.AvroFieldBinlog{},
		TextIndexFiles:    []snapshotio.AvroTextIndexEntry{},
		JSONKeyIndexFiles: []snapshotio.AvroJSONKeyIndexEntry{},
		IndexFiles:        []snapshotio.AvroIndexFilePathInfo{},
		StartPosition:     &snapshotio.AvroMsgPosition{ChannelName: "", MsgID: []byte{}, MsgGroup: "", Timestamp: 0},
		DmlPosition:       &snapshotio.AvroMsgPosition{ChannelName: "", MsgID: []byte{}, MsgGroup: "", Timestamp: 0},
		StorageVersion:    0,
		IsSorted:          false,
	}

	// Pre-generate valid Avro data for manifest using the real schema (single record)
	manifestSchema, _ := snapshotio.ManifestSchemaByVersion(0)
	validManifestData, _ := avro.Marshal(manifestSchema, manifestEntry)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	// Mock file operations - no longer need ListAllChunkWithPrefix since we pass the path directly
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataFilePath {
			return metadataJSON, nil
		}
		if filePath == "manifest1.avro" {
			return validManifestData, nil
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	snapshot, err := reader.ReadSnapshot(context.Background(), metadataFilePath, true)

	assert.NoError(t, err)
	assert.NotNil(t, snapshot)
	assert.Equal(t, int64(1), snapshot.SnapshotInfo.GetId())
	assert.Equal(t, "test_snapshot", snapshot.SnapshotInfo.GetName())
	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, snapshot.Layout)
	assert.Len(t, snapshot.Segments, 1)
	assert.Equal(t, int64(1001), snapshot.Segments[0].SegmentId)
	assert.Equal(t, "test_channel", snapshot.Segments[0].ChannelName)
	assert.Equal(t, int64(100), snapshot.Segments[0].NumOfRows)
}

func TestSnapshotReader_ReadSnapshot_RejectsManifestSegmentIDMismatch(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	snapshot := createTestSnapshotData()
	snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{SegmentId: 1002})
	snapshot.SegmentIDs = []int64{1001, 1002}
	metadataPath, err := snapshotstorage.NewSnapshotWriter(cm).Save(ctx, snapshot)
	require.NoError(t, err)

	metadata := readSnapshotMetadataForTest(t, cm, metadataPath)
	require.Len(t, metadata.GetManifestList(), 2)
	metadata.ManifestList = metadata.ManifestList[:1]
	metadataJSON, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, cm.Write(ctx, snapshotstorage.NormalizeSnapshotObjectPath(metadataPath), metadataJSON))

	readSnapshot, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
	require.Error(t, err)
	assert.Nil(t, readSnapshot)
	assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
	assert.Contains(t, err.Error(), "segment IDs")
	assert.Contains(t, err.Error(), "1002")
}

func TestSnapshotReader_ReadSnapshot_RejectsMetadataLocationIDMismatch(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	ctx := context.Background()
	snapshot := createTestSnapshotData()
	metadataPath, err := snapshotstorage.NewSnapshotWriter(cm).Save(ctx, snapshot)
	require.NoError(t, err)

	metadata := readSnapshotMetadataForTest(t, cm, metadataPath)
	metadata.SnapshotInfo.CollectionId++
	metadataJSON, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, cm.Write(ctx, snapshotstorage.NormalizeSnapshotObjectPath(metadataPath), metadataJSON))

	readSnapshot, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
	require.NoError(t, err)
	err = snapshotstorage.ValidateSnapshotMetadataLocation(metadataPath, readSnapshot.SnapshotInfo)

	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
	assert.Contains(t, err.Error(), "collection ID")
}

func TestSnapshotReader_ReadSnapshot_RejectsInvalidStorageV2ManifestMappings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*datapb.SnapshotMetadata)
	}{
		{
			name: "nil mapping",
			mutate: func(metadata *datapb.SnapshotMetadata) {
				metadata.Storagev2ManifestList = append(metadata.Storagev2ManifestList, nil)
			},
		},
		{
			name: "empty path",
			mutate: func(metadata *datapb.SnapshotMetadata) {
				metadata.Storagev2ManifestList[0].Manifest = ""
			},
		},
		{
			name: "duplicate segment",
			mutate: func(metadata *datapb.SnapshotMetadata) {
				metadata.Storagev2ManifestList = append(
					metadata.Storagev2ManifestList,
					proto.Clone(metadata.Storagev2ManifestList[0]).(*datapb.StorageV2SegmentManifest),
				)
			},
		},
		{
			name: "unknown segment",
			mutate: func(metadata *datapb.SnapshotMetadata) {
				metadata.Storagev2ManifestList[0].SegmentId = 9999
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			ctx := context.Background()
			snapshot := createTestSnapshotData()
			snapshot.Segments[0].StorageVersion = storage.StorageV2
			snapshot.Segments[0].ManifestPath = "files/insert_log/100/10/1001/manifest.json"
			metadataPath, err := snapshotstorage.NewSnapshotWriter(cm).Save(ctx, snapshot)
			require.NoError(t, err)

			metadata := readSnapshotMetadataForTest(t, cm, metadataPath)
			require.Len(t, metadata.GetStoragev2ManifestList(), 1)
			tt.mutate(metadata)
			metadataJSON, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
			require.NoError(t, err)
			require.NoError(t, cm.Write(ctx, snapshotstorage.NormalizeSnapshotObjectPath(metadataPath), metadataJSON))

			readSnapshot, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
			require.Error(t, err)
			assert.Nil(t, readSnapshot)
			assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
		})
	}
}

func TestSnapshotReader_ReadSnapshot_RejectsDuplicateManifestWithoutSegmentIDs(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	ctx := context.Background()

	snapshot := createTestSnapshotData()
	snapshot.SegmentIDs = nil
	metadataPath, err := snapshotstorage.NewSnapshotWriter(cm).Save(ctx, snapshot)
	require.NoError(t, err)

	metadata := readSnapshotMetadataForTest(t, cm, metadataPath)
	require.Len(t, metadata.GetManifestList(), 1)
	metadata.ManifestList = append(metadata.ManifestList, metadata.ManifestList[0])
	metadataJSON, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, cm.Write(ctx, snapshotstorage.NormalizeSnapshotObjectPath(metadataPath), metadataJSON))

	readSnapshot, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
	require.Error(t, err)
	assert.Nil(t, readSnapshot)
	assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
	assert.Contains(t, err.Error(), "duplicate manifest")
}

func TestSnapshotReader_ReadSnapshot_RejectsSensitivePathInReferencedManifest(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	snapshot := createTestSnapshotData()
	snapshot.SegmentIDs = []int64{1001}
	snapshot.Segments[0].Binlogs[0].Binlogs[0].LogPath = "s3://source-bucket/files/insert/1?X-Amz-Signature=TOPSECRET"
	metadataPath, err := snapshotstorage.NewSnapshotWriter(cm).Save(ctx, snapshot)
	require.NoError(t, err)

	readSnapshot, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
	require.Error(t, err)
	assert.Nil(t, readSnapshot)
	assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
	assert.NotContains(t, err.Error(), "TOPSECRET")
	assert.NotContains(t, err.Error(), "X-Amz-Signature")
}

func TestSnapshotReader_CapturedMetadataErrors(t *testing.T) {
	reader := snapshotstorage.NewSnapshotReader(nil)
	ctx := context.Background()
	_, err := reader.ReadMetadata(ctx, "s3://user:secret@source/key")
	require.Error(t, err)
	for _, metadata := range []*datapb.SnapshotMetadata{
		nil,
		{},
		{SnapshotInfo: &datapb.SnapshotInfo{}},
		{SnapshotInfo: &datapb.SnapshotInfo{}, Collection: &datapb.CollectionDescription{}, ManifestList: []string{"s3://user:secret@source/key"}},
	} {
		_, err := reader.ReadSnapshotFromMetadata(ctx, "root/snapshots/1/metadata/2.json", metadata, false)
		require.Error(t, err)
	}
}

func TestSnapshotReader_ReadSnapshot_EmptyPath(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := snapshotstorage.NewSnapshotReader(cm)

	// Test with empty path - should return error immediately
	_, err := reader.ReadSnapshot(context.Background(), "", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata file path cannot be empty")
}

func TestSnapshotReader_ReadSnapshot_FileNotFound(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := snapshotstorage.NewSnapshotReader(cm)

	metadataFilePath := "snapshots/100/metadata/nonexistent.json"

	// Mock Read to return error (file not found)
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).Return(
		nil,
		fmt.Errorf("file not found"),
	).Build()
	defer mockRead.UnPatch()

	_, err := reader.ReadSnapshot(context.Background(), metadataFilePath, false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read metadata file")
}

func TestSnapshotReader_ListSnapshots_Success(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := snapshotstorage.NewSnapshotReader(cm)

	metadata1 := &datapb.SnapshotMetadata{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:          1,
			CreateTs:    1234567890,
			Name:        "snapshot1",
			Description: "first snapshot",
		},
		Collection:   &datapb.CollectionDescription{},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{},
	}
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	metadata1JSON, _ := marshalOpts.Marshal(metadata1)

	metadata2 := &datapb.SnapshotMetadata{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:          2,
			CreateTs:    1234567900,
			Name:        "snapshot2",
			Description: "second snapshot",
		},
		Collection:   &datapb.CollectionDescription{},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{},
	}
	metadata2JSON, _ := marshalOpts.Marshal(metadata2)

	mockList := mockey.Mock(storage.ListAllChunkWithPrefix).Return(
		[]string{
			"snapshots/100/metadata/00001-uuid1.json",
			"snapshots/100/metadata/00002-uuid2.json",
			"snapshots/100/metadata/version-hint.txt",
		},
		[]time.Time{},
		nil,
	).Build()
	defer mockList.UnPatch()

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == "snapshots/100/metadata/00001-uuid1.json" {
			return metadata1JSON, nil
		}
		if filePath == "snapshots/100/metadata/00002-uuid2.json" {
			return metadata2JSON, nil
		}
		return []byte("mock-data"), nil
	}).Build()
	defer mockRead.UnPatch()

	snapshots, err := reader.ListSnapshots(context.Background(), 100)

	assert.NoError(t, err)
	assert.Len(t, snapshots, 2)
	assert.Equal(t, "snapshot1", snapshots[0].GetName())
	assert.Equal(t, "snapshot2", snapshots[1].GetName())
}

func TestSnapshotReader_ListSnapshots_ListFailurePreservesIoError(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := snapshotstorage.NewSnapshotReader(cm)

	mockList := mockey.Mock(storage.ListAllChunkWithPrefix).Return(
		nil,
		nil,
		fmt.Errorf("list failed"),
	).Build()
	defer mockList.UnPatch()

	_, err := reader.ListSnapshots(context.Background(), 100)

	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrIoFailed)
}

// =========================== Data Conversion Tests ===========================

func TestFieldBinlog_RoundtripConversion(t *testing.T) {
	originalFieldBinlog := &datapb.FieldBinlog{
		FieldID:     123,
		ChildFields: []int64{123, 124, 125},
		Format:      "parquet",
		Binlogs: []*datapb.Binlog{
			{
				EntriesNum:    1000,
				TimestampFrom: 1500,
				TimestampTo:   2500,
				LogPath:       "/original/path",
				LogSize:       2048,
				LogID:         10,
				MemorySize:    4096,
			},
			{
				EntriesNum:    500,
				TimestampFrom: 2000,
				TimestampTo:   3000,
				LogPath:       "/another/path",
				LogSize:       1024,
				LogID:         11,
				MemorySize:    2048,
			},
		},
	}

	avroFieldBinlog := snapshotio.FieldBinlogToAvro(originalFieldBinlog)
	resultFieldBinlog := snapshotio.AvroToFieldBinlog(avroFieldBinlog)

	assert.Equal(t, originalFieldBinlog.FieldID, resultFieldBinlog.FieldID)
	assert.Equal(t, originalFieldBinlog.ChildFields, resultFieldBinlog.ChildFields)
	assert.Equal(t, originalFieldBinlog.Format, resultFieldBinlog.Format)
	assert.Len(t, resultFieldBinlog.Binlogs, len(originalFieldBinlog.Binlogs))

	for i, originalBinlog := range originalFieldBinlog.Binlogs {
		resultBinlog := resultFieldBinlog.Binlogs[i]
		assert.Equal(t, originalBinlog.EntriesNum, resultBinlog.EntriesNum)
		assert.Equal(t, originalBinlog.TimestampFrom, resultBinlog.TimestampFrom)
		assert.Equal(t, originalBinlog.TimestampTo, resultBinlog.TimestampTo)
		assert.Equal(t, originalBinlog.LogPath, resultBinlog.LogPath)
		assert.Equal(t, originalBinlog.LogSize, resultBinlog.LogSize)
		assert.Equal(t, originalBinlog.LogID, resultBinlog.LogID)
		assert.Equal(t, originalBinlog.MemorySize, resultBinlog.MemorySize)
	}
}

func TestIndexFilePathInfo_RoundtripConversion(t *testing.T) {
	originalIndexInfo := &indexpb.IndexFilePathInfo{
		SegmentID: 9999,
		FieldID:   88,
		IndexID:   7777,
		BuildID:   6666,
		IndexName: "comprehensive_index",
		IndexParams: []*commonpb.KeyValuePair{
			{Key: "metric_type", Value: "IP"},
			{Key: "index_type", Value: "IVF_FLAT"},
			{Key: "nlist", Value: "1024"},
		},
		IndexFilePaths:        []string{"/idx/path1", "/idx/path2", "/idx/path3"},
		SerializedSize:        16384,
		IndexVersion:          5,
		NumRows:               50000,
		CurrentIndexVersion:   5,
		MemSize:               32768,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}

	avroIndexInfo := snapshotio.IndexFilePathInfoToAvro(originalIndexInfo)
	resultIndexInfo := snapshotio.AvroToIndexFilePathInfo(avroIndexInfo)

	assert.Equal(t, originalIndexInfo.SegmentID, resultIndexInfo.SegmentID)
	assert.Equal(t, originalIndexInfo.FieldID, resultIndexInfo.FieldID)
	assert.Equal(t, originalIndexInfo.IndexID, resultIndexInfo.IndexID)
	assert.Equal(t, originalIndexInfo.BuildID, resultIndexInfo.BuildID)
	assert.Equal(t, originalIndexInfo.IndexName, resultIndexInfo.IndexName)
	assert.Equal(t, originalIndexInfo.SerializedSize, resultIndexInfo.SerializedSize)
	assert.Equal(t, originalIndexInfo.IndexVersion, resultIndexInfo.IndexVersion)
	assert.Equal(t, originalIndexInfo.NumRows, resultIndexInfo.NumRows)
	assert.Equal(t, originalIndexInfo.CurrentIndexVersion, resultIndexInfo.CurrentIndexVersion)
	assert.Equal(t, originalIndexInfo.MemSize, resultIndexInfo.MemSize)
	assert.Equal(t, originalIndexInfo.IndexStorePathVersion, resultIndexInfo.IndexStorePathVersion)

	// Verify IndexParams
	assert.Len(t, resultIndexInfo.IndexParams, len(originalIndexInfo.IndexParams))
	for i, originalParam := range originalIndexInfo.IndexParams {
		resultParam := resultIndexInfo.IndexParams[i]
		assert.Equal(t, originalParam.Key, resultParam.Key)
		assert.Equal(t, originalParam.Value, resultParam.Value)
	}

	// Verify IndexFilePaths
	assert.Equal(t, originalIndexInfo.IndexFilePaths, resultIndexInfo.IndexFilePaths)
}

func TestSnapshotReader_ReadManifestLegacyIndexFilePathInfoDefaultsBuildRooted(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	segment := &datapb.SegmentDescription{
		SegmentId:   1001,
		PartitionId: 2001,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				SegmentID:      1001,
				FieldID:        101,
				IndexID:        201,
				BuildID:        301,
				IndexName:      "vec_idx",
				IndexFilePaths: []string{"files/index_files/301/1/2001/1001/index_data"},
			},
		},
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.NotContains(t, snapshotio.AvroSchemaV1(), "index_store_path_version")
	oldSchema, err := snapshotio.ManifestSchemaByVersion(1)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(oldSchema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "legacy_manifest.avro")
	err = cm.Write(context.Background(), manifestPath, binaryData)
	require.NoError(t, err)
	segments := readManifestFileForTest(t, cm, manifestPath, 1)
	require.Len(t, segments, 1)
	require.Len(t, segments[0].GetIndexFiles(), 1)
	assert.Equal(t,
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
		segments[0].GetIndexFiles()[0].GetIndexStorePathVersion())
}

// TestSnapshotManifest_CommitTimestampRoundtripV3 verifies that CommitTimestamp
// survives a Marshal/Unmarshal cycle with the current (V3) schema. This is the
// invariant the snapshot.go field comment promises ("preserved so that GC, TTL,
// and MVCC protections survive snapshot/restore").
func TestSnapshotManifest_CommitTimestampRoundtripV3(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	const wantCommitTs = uint64(1234567890)
	segment := &datapb.SegmentDescription{
		SegmentId:       1001,
		PartitionId:     2001,
		ChannelName:     "ch-0",
		CommitTimestamp: wantCommitTs,
	}
	entry := snapshotio.SegmentToManifestEntry(segment)
	require.Equal(t, int64(wantCommitTs), entry.CommitTimestamp)

	assert.Contains(t, snapshotio.AvroSchemaV3(), "commit_timestamp")
	schema, err := snapshotio.ManifestSchemaByVersion(3)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v3_manifest.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))
	segments := readManifestFileForTest(t, cm, manifestPath, 3)
	require.Len(t, segments, 1)
	assert.Equal(t, wantCommitTs, segments[0].GetCommitTimestamp())
}

// TestSnapshotManifest_LegacyV2NoCommitTimestamp verifies that a manifest
// written with the V2 schema (no commit_timestamp field) still decodes cleanly
// under the V2 reader and surfaces CommitTimestamp=0. This guarantees that
// pre-existing on-disk snapshots remain readable after the V3 bump.
func TestSnapshotManifest_LegacyV2NoCommitTimestamp(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	segment := &datapb.SegmentDescription{
		SegmentId:       1001,
		PartitionId:     2001,
		ChannelName:     "ch-0",
		CommitTimestamp: 999, // set on the struct; V2 schema must drop it
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.NotContains(t, snapshotio.AvroSchemaV2(), "commit_timestamp")
	v2Schema, err := snapshotio.ManifestSchemaByVersion(2)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(v2Schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v2_manifest.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))
	segments := readManifestFileForTest(t, cm, manifestPath, 2)
	require.Len(t, segments, 1)
	assert.Equal(t, uint64(0), segments[0].GetCommitTimestamp(),
		"V2 manifest must decode with CommitTimestamp=0 (field absent in schema)")
}

func TestSnapshotManifest_FieldBinlogChildFieldsAndFormatRoundtripV4(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	segment := &datapb.SegmentDescription{
		SegmentId:   1001,
		PartitionId: 2001,
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID:     900,
				ChildFields: []int64{100, 101},
				Format:      "parquet",
				Binlogs: []*datapb.Binlog{{
					EntriesNum: 10,
					LogPath:    "files/insert_log/1/2/3/900/1",
				}},
			},
		},
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.Contains(t, snapshotio.AvroSchemaV4(), "child_fields")
	assert.Contains(t, snapshotio.AvroSchemaV4(), `"format"`)
	v4Schema, err := snapshotio.ManifestSchemaByVersion(4)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(v4Schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v4_manifest.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))
	segments := readManifestFileForTest(t, cm, manifestPath, 4)
	require.Len(t, segments, 1)
	require.Len(t, segments[0].GetBinlogs(), 1)
	assert.Equal(t, []int64{100, 101}, segments[0].GetBinlogs()[0].GetChildFields())
	assert.Equal(t, "parquet", segments[0].GetBinlogs()[0].GetFormat())
}

func TestSnapshotManifest_LegacyV3NoChildFieldsOrFormat(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	segment := &datapb.SegmentDescription{
		SegmentId:   1001,
		PartitionId: 2001,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID:     900,
			ChildFields: []int64{100, 101},
			Format:      "parquet",
			Binlogs: []*datapb.Binlog{{
				EntriesNum: 10,
				LogPath:    "files/insert_log/1/2/3/900/1",
			}},
		}},
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.NotContains(t, snapshotio.AvroSchemaV3(), "child_fields")
	assert.NotContains(t, snapshotio.AvroSchemaV3(), `"format"`)
	v3Schema, err := snapshotio.ManifestSchemaByVersion(3)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(v3Schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v3_manifest.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))
	segments := readManifestFileForTest(t, cm, manifestPath, 3)
	require.Len(t, segments, 1)
	require.Len(t, segments[0].GetBinlogs(), 1)
	assert.Empty(t, segments[0].GetBinlogs()[0].GetChildFields())
	assert.Equal(t, "", segments[0].GetBinlogs()[0].GetFormat())
}

func TestSnapshotManifest_LegacyV2NoChildFieldsOrFormat(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	segment := &datapb.SegmentDescription{
		SegmentId:   1001,
		PartitionId: 2001,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID:     900,
			ChildFields: []int64{100, 101},
			Format:      "parquet",
			Binlogs: []*datapb.Binlog{{
				EntriesNum: 10,
				LogPath:    "files/insert_log/1/2/3/900/1",
			}},
		}},
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.NotContains(t, snapshotio.AvroSchemaV2(), "child_fields")
	assert.NotContains(t, snapshotio.AvroSchemaV2(), `"format"`)
	v2Schema, err := snapshotio.ManifestSchemaByVersion(2)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(v2Schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v2_no_child_fields.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))
	segments := readManifestFileForTest(t, cm, manifestPath, 2)
	require.Len(t, segments, 1)
	require.Len(t, segments[0].GetBinlogs(), 1)
	assert.Empty(t, segments[0].GetBinlogs()[0].GetChildFields())
	assert.Equal(t, "", segments[0].GetBinlogs()[0].GetFormat())
}

// =========================== Integration Tests ===========================

func TestSnapshotWriter_ManifestList_Roundtrip(t *testing.T) {
	// This test is no longer relevant since we removed the manifest list layer
	// Manifest paths are now stored directly in metadata.json
	t.Skip("Manifest list layer has been removed - manifest paths are now stored directly in metadata.json")
}

func TestSnapshot_CompleteWorkflow(t *testing.T) {
	// Test complete snapshot workflow
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)
	reader := snapshotstorage.NewSnapshotReader(cm)

	// 1. Create test data
	snapshotData := createTestSnapshotData()

	// 2. Save snapshot
	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)

	// 3. Read snapshot using the metadata path returned from Save
	readSnapshot, err := reader.ReadSnapshot(context.Background(), metadataPath, true)
	assert.NoError(t, err)
	assert.NotNil(t, readSnapshot)

	// 4. Verify data consistency
	assert.Equal(t, snapshotData.SnapshotInfo.GetId(), readSnapshot.SnapshotInfo.GetId())
	assert.Equal(t, snapshotData.SnapshotInfo.GetName(), readSnapshot.SnapshotInfo.GetName())
	assert.Equal(t, snapshotData.SnapshotInfo.GetDescription(), readSnapshot.SnapshotInfo.GetDescription())

	// 5. Verify collection information
	assert.Equal(t, snapshotData.Collection.Schema.Name, readSnapshot.Collection.Schema.Name)
	assert.Len(t, readSnapshot.Collection.Schema.Fields, len(snapshotData.Collection.Schema.Fields))

	// 6. Verify segments information
	assert.Len(t, readSnapshot.Segments, len(snapshotData.Segments))
	if len(readSnapshot.Segments) > 0 {
		assert.Equal(t, snapshotData.Segments[0].SegmentId, readSnapshot.Segments[0].SegmentId)
		assert.Equal(t, snapshotData.Segments[0].PartitionId, readSnapshot.Segments[0].PartitionId)
		if len(snapshotData.Segments[0].GetBinlogs()) > 0 {
			require.Len(t, readSnapshot.Segments[0].GetBinlogs(), len(snapshotData.Segments[0].GetBinlogs()))
			assert.Equal(t, snapshotData.Segments[0].GetBinlogs()[0].GetChildFields(), readSnapshot.Segments[0].GetBinlogs()[0].GetChildFields())
			assert.Equal(t, snapshotData.Segments[0].GetBinlogs()[0].GetFormat(), readSnapshot.Segments[0].GetBinlogs()[0].GetFormat())
		}
	}

	// 7. Verify indexes information
	assert.Len(t, readSnapshot.Indexes, len(snapshotData.Indexes))
	if len(readSnapshot.Indexes) > 0 {
		assert.Equal(t, snapshotData.Indexes[0].IndexID, readSnapshot.Indexes[0].IndexID)
		assert.Equal(t, snapshotData.Indexes[0].IndexName, readSnapshot.Indexes[0].IndexName)
	}

	// 8. Cleanup - use the metadata path returned from Save
	err = writer.Drop(context.Background(), metadataPath)
	assert.NoError(t, err)
}

func TestSnapshotFingerprintIsDeterministicAndContentSensitive(t *testing.T) {
	left := createTestSnapshotData()
	right := createTestSnapshotData()
	left.Segments = append(left.Segments, proto.Clone(left.Segments[0]).(*datapb.SegmentDescription))
	left.Segments[1].SegmentId = 1002
	right.Segments = []*datapb.SegmentDescription{
		proto.Clone(left.Segments[1]).(*datapb.SegmentDescription),
		proto.Clone(left.Segments[0]).(*datapb.SegmentDescription),
	}

	leftFingerprint, err := snapshotstorage.SnapshotFingerprint(left)
	require.NoError(t, err)
	rightFingerprint, err := snapshotstorage.SnapshotFingerprint(right)
	require.NoError(t, err)
	assert.Equal(t, leftFingerprint, rightFingerprint)

	right.SnapshotInfo.PinIds = []int64{9001}
	right.SnapshotInfo.PinExpireAtMs = map[int64]int64{9001: time.Now().Add(time.Hour).UnixMilli()}
	pinnedFingerprint, err := snapshotstorage.SnapshotFingerprint(right)
	require.NoError(t, err)
	assert.Equal(t, leftFingerprint, pinnedFingerprint)

	right.Segments[0].NumOfRows++
	changedFingerprint, err := snapshotstorage.SnapshotFingerprint(right)
	require.NoError(t, err)
	assert.NotEqual(t, leftFingerprint, changedFingerprint)
	// Layout changes source path interpretation; format version does not change selection.
	right.Layout = datapb.SnapshotLayout_SnapshotLayoutSelfContained
	right.ManifestFormatVersion = 5
	legacy, err := snapshotstorage.SnapshotFingerprint(right)
	require.NoError(t, err)
	require.NotEqual(t, changedFingerprint, legacy)
	right.ManifestFormatVersion = 6
	current, err := snapshotstorage.SnapshotFingerprint(right)
	require.NoError(t, err)
	require.Equal(t, legacy, current)
}

func validateSnapshotDataFiles(ctx context.Context, snapshot *snapshotstorage.SnapshotData) error {
	return snapshotstorage.ValidateExternalSnapshotDataFiles(ctx, "snapshots/100/metadata/1.json", snapshot)
}

func TestValidateSnapshotDataFiles_DefersMissingFileToCopy(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	missingPath := path.Join(tempDir, "files/insert_log/100/10/1001/missing")
	snapshot := &snapshotstorage.SnapshotData{Segments: []*datapb.SegmentDescription{{
		SegmentId: 1001,
		Binlogs:   []*datapb.FieldBinlog{{FieldID: 10, Binlogs: []*datapb.Binlog{{LogPath: missingPath}}}},
	}}}

	err := validateSnapshotDataFiles(context.Background(), snapshot)
	require.NoError(t, err)
	// Validation accepts the reference, but execution must still fail on the
	// actual missing object rather than report a successful restore.
	err = snapshotstorage.CopyFiles(context.Background(), []snapshotstorage.FileCopy{{SourcePath: missingPath}},
		make(chan struct{}, 1), func(ctx context.Context, item snapshotstorage.FileCopy) error {
			_, err := cm.Read(ctx, item.SourcePath)
			return err
		})
	require.Error(t, err)
}

func TestValidateSnapshotDataFiles_SucceedsWhenObjectsExist(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()
	existingPath := path.Join(tempDir, "files/insert_log/100/10/1001/1")
	require.NoError(t, cm.Write(ctx, existingPath, []byte("insert")))

	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: existingPath,
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	err := validateSnapshotDataFiles(ctx, snapshot)
	require.NoError(t, err)
}

// Job creation validates the references carried by Avro without inspecting the
// native manifest or source objects. DataNode rejects invalid native references.
func TestValidateSnapshotDataFiles_StorageV3MetadataOnly(t *testing.T) {
	defer mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(func(*storage.LocalChunkManager, context.Context, string, bool, storage.ChunkObjectWalkFunc) error {
		t.Error("job validation must not list objects")
		return merr.ErrServiceInternal
	}).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestLobFiles).To(func(string, *indexpb.StorageConfig) ([]packed.LobFileInfo, error) {
		t.Error("job validation must not query native LOBs")
		return nil, merr.ErrServiceInternal
	}).Build().UnPatch()
	defer mockey.Mock(packed.ReadManifestFiles).To(func(string, *indexpb.StorageConfig) (*packed.ManifestFiles, error) {
		t.Error("job validation must not read native manifests")
		return nil, merr.ErrServiceInternal
	}).Build().UnPatch()
	for _, tc := range []struct {
		name, manifest, index string
		rows                  int64
		fail                  bool
	}{
		{"missing native manifest", "bundle/files/insert_log/100/20/1001", "", 100, false},
		{"empty segment", "bundle/files/insert_log/100/20/1001", "", 0, false},
		{"outside root", "other/insert_log/100/20/1001", "", 100, true},
		{"foreign bucket", "s3://other/bundle/files/insert_log/100/20/1001", "", 100, true},
		{"outside self-contained files", "bundle/insert_log/100/20/1001", "", 100, true},
		{"foreign Avro index", "bundle/files/insert_log/100/20/1001", "s3://other/bundle/files/index_files/1", 100, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			segment := &datapb.SegmentDescription{
				SegmentId: 1001, StorageVersion: storage.StorageV3,
				ManifestPath: packed.MarshalManifestPath(tc.manifest, 1), NumOfRows: tc.rows,
				Binlogs:           []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "stale/insert"}}}},
				TextIndexFiles:    map[int64]*datapb.TextIndexStats{10: {Files: []string{"stale/text"}}},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{11: {Files: []string{"stale/json"}}},
			}
			if tc.index != "" {
				segment.IndexFiles = []*indexpb.IndexFilePathInfo{{IndexFilePaths: []string{tc.index}}}
			}
			snapshot := &snapshotstorage.SnapshotData{
				Layout:        datapb.SnapshotLayout_SnapshotLayoutSelfContained,
				ManifestPaths: []string{"bundle/snapshots/100/manifests/1.avro"}, Segments: []*datapb.SegmentDescription{segment},
			}
			err := snapshotstorage.ValidateExternalSnapshotDataFiles(context.Background(),
				"s3://source/bundle/snapshots/100/metadata/1.json", snapshot)
			if tc.fail {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			} else {
				require.NoError(t, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, snapshotstorage.ValidateExternalSnapshotDataFiles(ctx, "", createTestSnapshotData()), context.Canceled)
	require.ErrorIs(t, snapshotstorage.ValidateExternalSnapshotDataFiles(context.Background(), "", nil), merr.ErrServiceInternal)
}

func TestValidateSnapshotDataFiles_StorageV3RejectsEmptyManifestBasePath(t *testing.T) {
	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath("", 1)
	segment.Binlogs = nil
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	err := validateSnapshotDataFiles(context.Background(), snapshot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage v3 segment 1001 requires manifest base path")
}

func TestValidateSnapshotObjectPathForBucket(t *testing.T) {
	baseCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	cm := &bucketChunkManagerTarget{ChunkManager: baseCM}
	mockBucket := mockey.Mock((*bucketChunkManagerTarget).BucketName).Return("test-bucket").Build()
	defer mockBucket.UnPatch()

	tests := []struct {
		name    string
		path    string
		wantErr string
	}{
		{
			name: "relative key",
			path: "files/snapshots/100/metadata/1.json",
		},
		{
			name: "same bucket object key",
			path: "export-root/snapshots/100/metadata/1.json",
		},
		{
			name: "absolute local path",
			path: "/tmp/snapshots/100/metadata/1.json",
		},
		{
			name: "matching bucket",
			path: "s3://test-bucket/files/snapshots/100/metadata/1.json",
		},
		{
			name: "gcp bucket URI",
			path: "gs://test-bucket/files/snapshots/100/metadata/1.json",
		},
		{
			name: "azure endpoint URI",
			path: "azure://blob.core.windows.net/test-bucket/files/snapshots/100/metadata/1.json",
		},
		{
			name: "https endpoint URI",
			path: "https://storage.example.com/test-bucket/files/snapshots/100/metadata/1.json",
		},
		{
			name:    "empty path",
			path:    "",
			wantErr: "snapshot_s3_location is required",
		},
		{
			name:    "credentials",
			path:    "s3://access:secret@test-bucket/files/snapshots/100/metadata/1.json",
			wantErr: "snapshot_s3_location must not embed credentials in the URI",
		},
		{
			name:    "unsupported scheme",
			path:    "ftp://test-bucket/files/snapshots/100/metadata/1.json",
			wantErr: `foreign_uri scheme "ftp" is not supported`,
		},
		{
			name:    "missing bucket",
			path:    "s3:///files/snapshots/100/metadata/1.json",
			wantErr: "snapshot_s3_location URI must include a bucket or endpoint host",
		},
		{
			name:    "bucket mismatch",
			path:    "s3://other-bucket/files/snapshots/100/metadata/1.json",
			wantErr: `snapshot_s3_location bucket "other-bucket" does not match configured bucket "test-bucket"`,
		},
		{
			name:    "endpoint URI bucket mismatch",
			path:    "https://storage.example.com/other-bucket/files/snapshots/100/metadata/1.json",
			wantErr: `snapshot_s3_location bucket "other-bucket" does not match configured bucket "test-bucket"`,
		},
		{
			name:    "URI path traversal",
			path:    "s3://test-bucket/root/../root/snapshots/100/metadata/1.json",
			wantErr: "foreign_uri object key must not contain path traversal",
		},
		{
			name:    "escaped URI path traversal",
			path:    "s3://test-bucket/root/%2e%2e/root/snapshots/100/metadata/1.json",
			wantErr: "foreign_uri object key must not contain path traversal",
		},
		{
			name:    "query credentials",
			path:    "s3://test-bucket/files/snapshots/100/metadata/1.json?X-Amz-Signature=secret",
			wantErr: "foreign_uri must not include query parameters or fragments",
		},
		{
			name:    "URI fragment",
			path:    "s3://test-bucket/files/snapshots/100/metadata/1.json#credential",
			wantErr: "foreign_uri must not include query parameters or fragments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := snapshotstorage.ValidateSnapshotObjectPathForBucket(cm, "snapshot_s3_location", tt.path, "")
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidateExternalSnapshotPaths_ValidatesURIIdentityBeforeNormalization(t *testing.T) {
	tests := []struct {
		name         string
		metadataPath string
		manifestPath string
		dataPath     string
		wantErr      bool
	}{
		{
			name:         "matching bucket",
			metadataPath: "s3://source-bucket/root/snapshots/100/metadata/1.json",
			manifestPath: "s3://source-bucket/root/snapshots/100/manifests/1/1001.avro",
			dataPath:     "s3://source-bucket/root/files/insert_log/100/1/1001/1",
		},
		{
			name:         "equivalent GCS schemes",
			metadataPath: "gs://source-bucket/root/snapshots/100/metadata/1.json",
			manifestPath: "gcs://source-bucket/root/snapshots/100/manifests/1/1001.avro",
			dataPath:     "gcs://source-bucket/root/files/insert_log/100/1/1001/1",
		},
		{
			name:         "equivalent Azure schemes",
			metadataPath: "az://blob.core.windows.net/source-container/root/snapshots/100/metadata/1.json",
			manifestPath: "azure://blob.core.windows.net/source-container/root/snapshots/100/manifests/1/1001.avro",
			dataPath:     "azure://blob.core.windows.net/source-container/root/files/insert_log/100/1/1001/1",
		},
		{
			name:         "manifest bucket mismatch",
			metadataPath: "s3://source-bucket/root/snapshots/100/metadata/1.json",
			manifestPath: "s3://other-bucket/root/snapshots/100/manifests/1/1001.avro",
			dataPath:     "root/files/insert_log/100/1/1001/1",
			wantErr:      true,
		},
		{
			name:         "data endpoint mismatch",
			metadataPath: "https://source.example.com/source-bucket/root/snapshots/100/metadata/1.json",
			manifestPath: "root/snapshots/100/manifests/1/1001.avro",
			dataPath:     "https://other.example.com/source-bucket/root/files/insert_log/100/1/1001/1",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := &snapshotstorage.SnapshotData{
				Layout:        datapb.SnapshotLayout_SnapshotLayoutReferenced,
				ManifestPaths: []string{tt.manifestPath},
			}
			refs := []snapshotstorage.SnapshotFileRef{{
				Path:           tt.dataPath,
				NormalizedPath: snapshotstorage.NormalizeSnapshotObjectPath(tt.dataPath),
			}}

			err := snapshotstorage.ValidateExternalSnapshotPaths(tt.metadataPath, snapshot, refs)
			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
		})
	}
}

func TestSnapshotWriter_SaveAllowsEmptyRoot(t *testing.T) {
	t.Chdir(t.TempDir())
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshotData := createTestSnapshotData()

	metadataPath, _, err := snapshotstorage.NewSnapshotWriter(cm).SaveToRootWithSize(
		context.Background(),
		snapshotData,
		"",
		datapb.SnapshotLayout_SnapshotLayoutReferenced,
	)

	require.NoError(t, err)
	assert.Equal(t, "snapshots/100/metadata/1.json", metadataPath)
	exist, err := cm.Exist(context.Background(), metadataPath)
	require.NoError(t, err)
	assert.True(t, exist)
}

func TestRedactSnapshotObjectPath(t *testing.T) {
	assert.Equal(t,
		"s3://redacted@test-bucket/files/snapshots/100/metadata/1.json",
		snapshotstorage.RedactSnapshotObjectPath("s3://access:secret@test-bucket/files/snapshots/100/metadata/1.json"))
	assert.Equal(t,
		"files/snapshots/100/metadata/1.json",
		snapshotstorage.RedactSnapshotObjectPath("files/snapshots/100/metadata/1.json"))
	assert.Equal(t,
		"s3://test-bucket/files/snapshots/100/metadata/1.json",
		snapshotstorage.RedactSnapshotObjectPath("s3://test-bucket/files/snapshots/100/metadata/1.json?X-Amz-Signature=secret#credential"))
}

func TestValidateSelfContainedSnapshotMetadata_StorageV3IgnoresManifestOwnedIndexPaths(t *testing.T) {
	manifestRoot := "bundle/files/insert_log/100/20/1001"
	metadata := &datapb.SnapshotMetadata{
		ManifestList: []string{"bundle/snapshots/100/manifests/1/1001.avro"},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{SegmentId: 1001, Manifest: packed.MarshalManifestPath(manifestRoot, 1)},
		},
	}
	segment := &datapb.SegmentDescription{
		SegmentId:      1001,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(manifestRoot, 1),
		TextIndexFiles: map[int64]*datapb.TextIndexStats{
			10: {FieldID: 10, Files: []string{"stale/text/path"}},
		},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
			11: {FieldID: 11, Files: []string{"stale/json/path"}},
		},
	}

	err := snapshotstorage.ValidateSelfContainedSnapshotMetadata(
		"bundle/snapshots/100/metadata/1.json",
		metadata,
		[]*datapb.SegmentDescription{segment},
	)
	require.NoError(t, err)
}

func TestValidateSelfContainedSnapshotMetadata_RejectsDataOutsideFilesSubtree(t *testing.T) {
	tests := []struct {
		name     string
		metadata *datapb.SnapshotMetadata
		segments []*datapb.SegmentDescription
	}{
		{
			name: "segment binlog",
			metadata: &datapb.SnapshotMetadata{
				ManifestList: []string{"bundle/snapshots/100/manifests/1/1001.avro"},
			},
			segments: []*datapb.SegmentDescription{{
				SegmentId: 1001,
				Binlogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{LogPath: "bundle/other/insert_log/100/1/1001/1"}},
				}},
			}},
		},
		{
			name: "storage manifest",
			metadata: &datapb.SnapshotMetadata{
				ManifestList: []string{"bundle/snapshots/100/manifests/1/1001.avro"},
				Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{{
					SegmentId: 1001,
					Manifest:  packed.MarshalManifestPath("bundle/other/insert_log/100/1/1001", 1),
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := snapshotstorage.ValidateSelfContainedSnapshotMetadata(
				"bundle/snapshots/100/metadata/1.json",
				tt.metadata,
				tt.segments,
			)

			require.Error(t, err)
			assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
			assert.Contains(t, err.Error(), "outside snapshot data root")
		})
	}
}

func TestValidateSelfContainedSnapshotMetadata_RedactsInvalidNestedURI(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		forbidden []string
	}{
		{
			name:      "userinfo",
			path:      "s3://access:secret@test-bucket/bundle/files/insert/1",
			forbidden: []string{"access", "secret"},
		},
		{
			name:      "presigned query",
			path:      "s3://test-bucket/bundle/files/insert/1?X-Amz-Credential=AKIA&X-Amz-Signature=TOPSECRET",
			forbidden: []string{"AKIA", "TOPSECRET", "X-Amz-Credential", "X-Amz-Signature"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segment := &datapb.SegmentDescription{
				SegmentId: 1001,
				Binlogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{LogPath: tt.path}},
				}},
			}

			err := snapshotstorage.ValidateSelfContainedSnapshotMetadata(
				"bundle/snapshots/100/metadata/1.json",
				&datapb.SnapshotMetadata{},
				[]*datapb.SegmentDescription{segment},
			)
			require.Error(t, err)
			assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
			for _, value := range tt.forbidden {
				assert.NotContains(t, err.Error(), value)
			}
		})
	}
}

func TestSnapshotReader_ReadSnapshot_RejectsSelfContainedPathOutsideRoot(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	snapshotData := createTestSnapshotData()
	segment := snapshotData.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{
			LogID:   1,
			LogPath: path.Join(tempDir, "outside/insert_log/1"),
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil
	segment.IndexFiles = nil

	targetRoot := path.Join(tempDir, "bundle")
	metadataPath, _, err := snapshotstorage.NewSnapshotWriter(cm).SaveToRootWithSize(ctx, snapshotData, targetRoot, datapb.SnapshotLayout_SnapshotLayoutSelfContained)
	assert.NoError(t, err)

	readSnapshot, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
	assert.Error(t, err)
	assert.Nil(t, readSnapshot)
	assert.Contains(t, err.Error(), "outside snapshot data root")
}

func TestRebaseSelfContainedSnapshotMetadata_RewritesManifestLists(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		ManifestList: []string{
			"export-root/snapshots/100/manifests/1/1001.avro",
		},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{
				SegmentId: 1001,
				Manifest:  packed.MarshalManifestPath("export-root/files/insert_log/100/1/1001", 7),
			},
		},
	}

	err := snapshotstorage.RebaseSelfContainedSnapshotMetadata(metadata, "export-root", "restored/x")
	require.NoError(t, err)
	assert.Equal(t, "restored/x/snapshots/100/manifests/1/1001.avro", metadata.GetManifestList()[0])

	gotBasePath, gotVersion, err := packed.UnmarshalManifestPath(metadata.GetStoragev2ManifestList()[0].GetManifest())
	require.NoError(t, err)
	assert.Equal(t, "restored/x/files/insert_log/100/1/1001", gotBasePath)
	assert.Equal(t, int64(7), gotVersion)
}

func TestRebaseSelfContainedSnapshotMetadata_RewritesConfiguredBucketS3URI(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		ManifestList: []string{
			"s3://test-bucket/export-root/snapshots/100/manifests/1/1001.avro",
		},
	}

	err := snapshotstorage.RebaseSelfContainedSnapshotMetadata(metadata, "export-root", "restored/x")
	require.NoError(t, err)
	assert.Equal(t, "restored/x/snapshots/100/manifests/1/1001.avro", metadata.GetManifestList()[0])
}

func TestRebaseSelfContainedSnapshotMetadata_RebasesFromBucketRoot(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		ManifestList: []string{"snapshots/100/manifests/1/1001.avro"},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{
				SegmentId: 1001,
				Manifest:  packed.MarshalManifestPath("files/insert_log/100/1/1001", 7),
			},
		},
	}

	err := snapshotstorage.RebaseSelfContainedSnapshotMetadata(metadata, "", "restored/x")
	require.NoError(t, err)
	assert.Equal(t, "restored/x/snapshots/100/manifests/1/1001.avro", metadata.GetManifestList()[0])
	basePath, version, err := packed.UnmarshalManifestPath(metadata.GetStoragev2ManifestList()[0].GetManifest())
	require.NoError(t, err)
	assert.Equal(t, "restored/x/files/insert_log/100/1/1001", basePath)
	assert.Equal(t, int64(7), version)
}

func TestRebaseSelfContainedSnapshotMetadata_RebasesToBucketRoot(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		ManifestList: []string{"export-root/snapshots/100/manifests/1/1001.avro"},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{
				SegmentId: 1001,
				Manifest:  packed.MarshalManifestPath("export-root/files/insert_log/100/1/1001", 7),
			},
		},
	}

	err := snapshotstorage.RebaseSelfContainedSnapshotMetadata(metadata, "export-root", "")
	require.NoError(t, err)
	assert.Equal(t, "snapshots/100/manifests/1/1001.avro", metadata.GetManifestList()[0])
	basePath, version, err := packed.UnmarshalManifestPath(metadata.GetStoragev2ManifestList()[0].GetManifest())
	require.NoError(t, err)
	assert.Equal(t, "files/insert_log/100/1/1001", basePath)
	assert.Equal(t, int64(7), version)
}

func TestRebaseSelfContainedSnapshotMetadata_LeavesUnmatchedManifestPathUnchanged(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("other-root/files/insert_log/100/1/1001", 7)
	metadata := &datapb.SnapshotMetadata{
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{
				SegmentId: 1001,
				Manifest:  manifestPath,
			},
		},
	}

	err := snapshotstorage.RebaseSelfContainedSnapshotMetadata(metadata, "export-root", "restored/x")
	require.NoError(t, err)
	assert.Equal(t, manifestPath, metadata.GetStoragev2ManifestList()[0].GetManifest())
}

func TestRebaseSelfContainedSnapshotMetadata_PreservesOptionalAndUnmatchedPaths(t *testing.T) {
	for _, tc := range []struct {
		name, src, oldRoot, newRoot, want string
	}{
		{"empty_optional", "", "old", "new", ""},
		{"unchanged_root_uri", "s3://bucket/old/files/data", "old", "old", "s3://bucket/old/files/data"},
		{"unmatched_uri", "s3://bucket/other/files/data", "old", "new", "s3://bucket/other/files/data"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &datapb.SnapshotMetadata{ManifestList: []string{tc.src}}
			require.NoError(t, snapshotstorage.RebaseSelfContainedSnapshotMetadata(metadata, tc.oldRoot, tc.newRoot))
			assert.Equal(t, []string{tc.want}, metadata.GetManifestList())
		})
	}
}

func TestRebaseSelfContainedSnapshotData_RewritesSegmentReferences(t *testing.T) {
	snapshot := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "relocated"},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:    1001,
				PartitionId:  1,
				SegmentLevel: datapb.SegmentLevel_L1,
				ChannelName:  "by-dev-rootcoord-dml_0_100v0",
				NumOfRows:    100,
				Binlogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{
						LogID:   1,
						LogPath: "export-root/files/insert_log/100/1/1001/1",
					}},
				}},
				Statslogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{
						LogID:   2,
						LogPath: "export-root/files/stats_log/100/1/1001/2",
					}},
				}},
				Deltalogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{
						LogID:   3,
						LogPath: "export-root/files/delta_log/100/1/1001/3",
					}},
				}},
				Bm25Statslogs: []*datapb.FieldBinlog{{
					FieldID: 3,
					Binlogs: []*datapb.Binlog{{
						LogID:   4,
						LogPath: "export-root/files/bm25_stats_log/100/1/1001/4",
					}},
				}},
				IndexFiles: []*indexpb.IndexFilePathInfo{{
					SegmentID:             1001,
					FieldID:               2,
					BuildID:               3001,
					IndexFilePaths:        []string{"export-root/files/index_files/100/1/1001/3001/index"},
					IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
				}},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					100: {
						FieldID: 100,
						BuildID: 5000,
						Files:   []string{"export-root/files/text_index/100/1/1001/5000/text"},
					},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					200: {
						FieldID: 200,
						BuildID: 6000,
						Files:   []string{"export-root/files/json_key_index/100/1/1001/6000/json"},
					},
				},
				ManifestPath: packed.MarshalManifestPath("export-root/files/insert_log/100/1/1001", 7),
			},
		},
	}

	err := snapshotstorage.RebaseSelfContainedSnapshotData(snapshot, "export-root", "restored/x")
	require.NoError(t, err)

	segment := snapshot.Segments[0]
	assert.Equal(t, "restored/x/files/insert_log/100/1/1001/1", segment.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "restored/x/files/stats_log/100/1/1001/2", segment.GetStatslogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "restored/x/files/delta_log/100/1/1001/3", segment.GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "restored/x/files/bm25_stats_log/100/1/1001/4", segment.GetBm25Statslogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "restored/x/files/index_files/100/1/1001/3001/index", segment.GetIndexFiles()[0].GetIndexFilePaths()[0])
	assert.Equal(t, "restored/x/files/text_index/100/1/1001/5000/text", segment.GetTextIndexFiles()[100].GetFiles()[0])
	assert.Equal(t, "restored/x/files/json_key_index/100/1/1001/6000/json", segment.GetJsonKeyIndexFiles()[200].GetFiles()[0])

	gotBasePath, gotVersion, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, "restored/x/files/insert_log/100/1/1001", gotBasePath)
	assert.Equal(t, int64(7), gotVersion)
}

func TestSnapshotReader_ReadSnapshot_RebasesRelocatedSelfContainedBundleBeforeManifestRead(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	reader := snapshotstorage.NewSnapshotReader(cm)
	ctx := context.Background()

	oldRoot := "export-root"
	newRoot := "restored/x"
	oldMetadataPath := path.Join(oldRoot, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotMetadataSubPath, "1.json")
	newMetadataPath := path.Join(newRoot, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotMetadataSubPath, "1.json")
	oldManifestPath := path.Join(oldRoot, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotManifestsSubPath, "1", "1001.avro")
	newManifestPath := path.Join(newRoot, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotManifestsSubPath, "1", "1001.avro")
	oldBinlogPath := path.Join(oldRoot, "files", "insert_log", "100", "1", "1001", "1")
	newBinlogPath := path.Join(newRoot, "files", "insert_log", "100", "1", "1001", "1")

	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "relocated",
			S3Location:   oldMetadataPath,
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{oldManifestPath},
		SegmentIds:   []int64{1001},
		Layout:       datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)

	segment := &datapb.SegmentDescription{
		SegmentId:    1001,
		PartitionId:  1,
		SegmentLevel: datapb.SegmentLevel_L1,
		ChannelName:  "by-dev-rootcoord-dml_0_100v0",
		NumOfRows:    100,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{{
				LogID:   1,
				LogPath: oldBinlogPath,
			}},
		}},
		Statslogs:         []*datapb.FieldBinlog{},
		Deltalogs:         []*datapb.FieldBinlog{},
		Bm25Statslogs:     []*datapb.FieldBinlog{},
		TextIndexFiles:    map[int64]*datapb.TextIndexStats{},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{},
		IndexFiles:        []*indexpb.IndexFilePathInfo{},
		StartPosition:     &msgpb.MsgPosition{ChannelName: "by-dev-rootcoord-dml_0_100v0"},
		DmlPosition:       &msgpb.MsgPosition{ChannelName: "by-dev-rootcoord-dml_0_100v0"},
		StorageVersion:    2,
		CommitTimestamp:   10,
	}
	manifestBytes, err := snapshotio.MarshalSegmentManifest(segment)
	require.NoError(t, err)

	readPaths := make([]string, 0, 2)
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		readPaths = append(readPaths, filePath)
		switch filePath {
		case newMetadataPath:
			return metadataJSON, nil
		case newManifestPath:
			return manifestBytes, nil
		case oldManifestPath:
			return nil, fmt.Errorf("old manifest path should not be read: %s", filePath)
		default:
			return nil, fmt.Errorf("unexpected file path: %s", filePath)
		}
	}).Build()
	defer mockRead.UnPatch()

	got, err := reader.ReadSnapshot(ctx, newMetadataPath, true)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Segments, 1)
	assert.Equal(t, newMetadataPath, got.SnapshotInfo.GetS3Location())
	assert.Equal(t, newBinlogPath, got.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Contains(t, readPaths, newManifestPath)
	assert.NotContains(t, readPaths, oldManifestPath)
	// Capturing metadata and retrying preparation must not mutate its original
	// root or reopen the top-level object after Create has returned.
	frozen := proto.Clone(metadata).(*datapb.SnapshotMetadata)
	readPaths = nil
	for attempt := 0; attempt < 2; attempt++ {
		got, err = reader.ReadSnapshotFromMetadata(ctx, newMetadataPath, metadata, true)
		require.NoError(t, err)
		require.Equal(t, newBinlogPath, got.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
		require.True(t, proto.Equal(frozen, metadata))
	}
	require.Equal(t, []string{newManifestPath, newManifestPath}, readPaths)
	rebaseMetadata := mockey.Mock(snapshotstorage.RebaseSelfContainedSnapshotMetadata).Return(merr.ErrIoFailed).Build()
	defer rebaseMetadata.UnPatch()
	_, err = reader.ReadSnapshotFromMetadata(ctx, newMetadataPath, metadata, true)
	require.ErrorIs(t, err, merr.ErrIoFailed)
	rebaseMetadata.UnPatch()
	rebaseData := mockey.Mock(snapshotstorage.RebaseSelfContainedSnapshotData).Return(merr.ErrIoFailed).Build()
	defer rebaseData.UnPatch()
	_, err = reader.ReadSnapshotFromMetadata(ctx, newMetadataPath, metadata, true)
	require.ErrorIs(t, err, merr.ErrIoFailed)
}

func TestSnapshotReader_ReadSnapshot_AcceptsSelfContainedBundleAtBucketRoot(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	metadataPath := "snapshots/100/metadata/1.json"
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "bucket-root",
			S3Location:   metadataPath,
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test_collection"},
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).Return(metadataJSON, nil).Build()
	defer mockRead.UnPatch()

	got, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(context.Background(), metadataPath, false)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, metadataPath, got.SnapshotInfo.GetS3Location())
}

func TestSnapshotReader_ReadSnapshot_RejectsRelocatedMetadataWithoutSnapshotsAnchor(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	reader := snapshotstorage.NewSnapshotReader(cm)

	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "relocated",
			S3Location:   "export-root/snapshots/100/metadata/1.json",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		ManifestList: []string{"export-root/snapshots/100/manifests/1/1001.avro"},
		Layout:       datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)

	metadataPathWithoutAnchor := "restored/x/meta.json"
	readPaths := make([]string, 0, 2)
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		readPaths = append(readPaths, filePath)
		if filePath == metadataPathWithoutAnchor {
			return metadataJSON, nil
		}
		if filePath == metadata.GetManifestList()[0] {
			return nil, fmt.Errorf("manifest should not be read")
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	got, err := reader.ReadSnapshot(context.Background(), metadataPathWithoutAnchor, true)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "cannot derive snapshot root from metadata path")
	assert.NotContains(t, readPaths, metadata.GetManifestList()[0])
}

func TestSnapshotReader_ReadSnapshot_RejectsSelfContainedMetadataWithoutSnapshotInfo(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	reader := snapshotstorage.NewSnapshotReader(cm)

	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		ManifestList: []string{"restored/x/snapshots/100/manifests/1/1001.avro"},
		Layout:       datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)

	metadataPath := "restored/x/snapshots/100/metadata/1.json"
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataPath {
			return metadataJSON, nil
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	var got *snapshotstorage.SnapshotData
	require.NotPanics(t, func() {
		got, err = reader.ReadSnapshot(context.Background(), metadataPath, false)
	})
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "snapshot info")
}

func TestSnapshotReader_ReadSnapshot_ClassifiesPermanentMetadataErrorsAsDataIntegrity(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "invalid json",
			data: []byte("{"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			mockRead := mockey.Mock((*storage.LocalChunkManager).Read).Return(tt.data, nil).Build()
			defer mockRead.UnPatch()

			got, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(
				context.Background(),
				"snapshots/100/metadata/1.json",
				false,
			)

			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			assert.Nil(t, got)
		})
	}
}

func TestSnapshotReader_ReadSnapshot_ClassifiesInvalidManifestAsDataIntegrity(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	metadataPath := "snapshots/100/metadata/1.json"
	manifestPath := "snapshots/100/manifests/1/1001.avro"
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "invalid-manifest",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test_collection"},
		},
		ManifestList: []string{manifestPath},
		Layout:       datapb.SnapshotLayout_SnapshotLayoutReferenced,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(
		func(_ context.Context, filePath string) ([]byte, error) {
			switch filePath {
			case metadataPath:
				return metadataJSON, nil
			case manifestPath:
				return []byte("not-avro"), nil
			default:
				return nil, merr.WrapErrIoKeyNotFound(filePath)
			}
		},
	).Build()
	defer mockRead.UnPatch()

	got, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(context.Background(), metadataPath, true)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.Nil(t, got)
}

func TestSnapshot_JSONStatsPaths_RoundTripPreservesSnapshotRestorePaths(t *testing.T) {
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)
	reader := snapshotstorage.NewSnapshotReader(cm)

	snapshotData := createTestSnapshotData()
	snapshotData.Segments[0].ManifestPath = `{"ver":7,"base_path":"files/insert_log/1/2/1001"}`
	snapshotData.Segments[0].JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{
		200: {
			FieldID:                200,
			Version:                2,
			Files:                  []string{"files/json_stats/3/6000/2/100/1/1001/200/shared_key_index/.managed.json_0"},
			LogSize:                1024,
			MemorySize:             2048,
			BuildID:                6000,
			JsonKeyStatsDataFormat: 3,
		},
		201: {
			FieldID: 201,
			Version: 3,
			Files: []string{
				"files/insert_log/1/2/1001/_stats/json_stats.201/shared_key_index/.managed.json_1",
				"files/insert_log/1/2/1001/_stats/json_stats.201/shredding_data/data.parquet",
			},
			LogSize:                2048,
			MemorySize:             4096,
			BuildID:                6001,
			JsonKeyStatsDataFormat: 3,
		},
	}

	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)

	readSnapshot, err := reader.ReadSnapshot(context.Background(), metadataPath, true)
	assert.NoError(t, err)
	assert.Len(t, readSnapshot.Segments, 1)

	got := readSnapshot.Segments[0].GetJsonKeyIndexFiles()
	assert.Equal(t, snapshotData.Segments[0].GetJsonKeyIndexFiles()[200].GetFiles(), got[200].GetFiles())
	assert.Equal(t, snapshotData.Segments[0].GetJsonKeyIndexFiles()[201].GetFiles(), got[201].GetFiles())
	assert.Equal(t, snapshotData.Segments[0].GetManifestPath(), readSnapshot.Segments[0].GetManifestPath())
}

// =========================== New Fields Tests ===========================

func TestSnapshot_NewFields_Serialization(t *testing.T) {
	// Test that all new fields (Bm25Statslogs, TextIndexFiles, JsonKeyIndexFiles,
	// StartPosition, DmlPosition, StorageVersion, IsSorted) are properly serialized
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := snapshotstorage.NewSnapshotWriter(cm)
	reader := snapshotstorage.NewSnapshotReader(cm)

	snapshotData := createTestSnapshotData()

	// 1. Save snapshot
	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)

	// 2. Read snapshot back using the metadata path
	readSnapshot, err := reader.ReadSnapshot(context.Background(), metadataPath, true)
	assert.NoError(t, err)
	assert.NotNil(t, readSnapshot)
	assert.Len(t, readSnapshot.Segments, 1)

	original := snapshotData.Segments[0]
	restored := readSnapshot.Segments[0]

	// 3. Verify Bm25Statslogs field
	assert.Equal(t, len(original.Bm25Statslogs), len(restored.Bm25Statslogs), "Bm25Statslogs count should match")
	if len(original.Bm25Statslogs) > 0 {
		assert.Equal(t, original.Bm25Statslogs[0].FieldID, restored.Bm25Statslogs[0].FieldID)
		assert.Equal(t, original.Bm25Statslogs[0].ChildFields, restored.Bm25Statslogs[0].ChildFields)
		assert.Equal(t, original.Bm25Statslogs[0].Format, restored.Bm25Statslogs[0].Format)
		assert.Equal(t, len(original.Bm25Statslogs[0].Binlogs), len(restored.Bm25Statslogs[0].Binlogs))
		if len(original.Bm25Statslogs[0].Binlogs) > 0 {
			assert.Equal(t, original.Bm25Statslogs[0].Binlogs[0].LogPath, restored.Bm25Statslogs[0].Binlogs[0].LogPath)
			assert.Equal(t, original.Bm25Statslogs[0].Binlogs[0].LogSize, restored.Bm25Statslogs[0].Binlogs[0].LogSize)
		}
	}

	// 3.5 Verify insert binlog metadata fields
	require.NotEmpty(t, restored.Binlogs)
	assert.Equal(t, original.Binlogs[0].ChildFields, restored.Binlogs[0].ChildFields)
	assert.Equal(t, original.Binlogs[0].Format, restored.Binlogs[0].Format)

	// 3.6 Verify stats/delta binlog metadata fields
	require.NotEmpty(t, restored.Statslogs)
	require.NotEmpty(t, restored.Deltalogs)
	assert.Equal(t, original.Statslogs[0].ChildFields, restored.Statslogs[0].ChildFields)
	assert.Equal(t, original.Statslogs[0].Format, restored.Statslogs[0].Format)
	assert.Equal(t, original.Deltalogs[0].ChildFields, restored.Deltalogs[0].ChildFields)
	assert.Equal(t, original.Deltalogs[0].Format, restored.Deltalogs[0].Format)

	// 4. Verify TextIndexFiles field (map type)
	assert.Equal(t, len(original.TextIndexFiles), len(restored.TextIndexFiles), "TextIndexFiles count should match")
	for fieldID, origStats := range original.TextIndexFiles {
		restStats, exists := restored.TextIndexFiles[fieldID]
		assert.True(t, exists, "TextIndexFiles field %d should exist", fieldID)
		assert.Equal(t, origStats.FieldID, restStats.FieldID)
		assert.Equal(t, origStats.Version, restStats.Version)
		assert.Equal(t, origStats.Files, restStats.Files)
		assert.Equal(t, origStats.LogSize, restStats.LogSize)
		assert.Equal(t, origStats.MemorySize, restStats.MemorySize)
		assert.Equal(t, origStats.BuildID, restStats.BuildID)
	}

	// 5. Verify JsonKeyIndexFiles field (map type)
	assert.Equal(t, len(original.JsonKeyIndexFiles), len(restored.JsonKeyIndexFiles), "JsonKeyIndexFiles count should match")
	for fieldID, origStats := range original.JsonKeyIndexFiles {
		restStats, exists := restored.JsonKeyIndexFiles[fieldID]
		assert.True(t, exists, "JsonKeyIndexFiles field %d should exist", fieldID)
		assert.Equal(t, origStats.FieldID, restStats.FieldID)
		assert.Equal(t, origStats.Version, restStats.Version)
		assert.Equal(t, origStats.Files, restStats.Files)
		assert.Equal(t, origStats.LogSize, restStats.LogSize)
		assert.Equal(t, origStats.MemorySize, restStats.MemorySize)
		assert.Equal(t, origStats.BuildID, restStats.BuildID)
		assert.Equal(t, origStats.JsonKeyStatsDataFormat, restStats.JsonKeyStatsDataFormat)
	}

	// 6. Verify StartPosition field
	assert.NotNil(t, restored.StartPosition, "StartPosition should not be nil")
	assert.Equal(t, original.StartPosition.ChannelName, restored.StartPosition.ChannelName)
	assert.Equal(t, original.StartPosition.MsgID, restored.StartPosition.MsgID)
	assert.Equal(t, original.StartPosition.MsgGroup, restored.StartPosition.MsgGroup)
	assert.Equal(t, original.StartPosition.Timestamp, restored.StartPosition.Timestamp)

	// 7. Verify DmlPosition field
	assert.NotNil(t, restored.DmlPosition, "DmlPosition should not be nil")
	assert.Equal(t, original.DmlPosition.ChannelName, restored.DmlPosition.ChannelName)
	assert.Equal(t, original.DmlPosition.MsgID, restored.DmlPosition.MsgID)
	assert.Equal(t, original.DmlPosition.MsgGroup, restored.DmlPosition.MsgGroup)
	assert.Equal(t, original.DmlPosition.Timestamp, restored.DmlPosition.Timestamp)

	// 8. Verify StorageVersion field
	assert.Equal(t, original.StorageVersion, restored.StorageVersion, "StorageVersion should match")

	// 9. Verify IsSorted field
	assert.Equal(t, original.IsSorted, restored.IsSorted, "IsSorted should match")

	// 10. Cleanup - use the metadata path returned from Save
	err = writer.Drop(context.Background(), metadataPath)
	assert.NoError(t, err)
}

func TestSnapshot_ConversionFunctions(t *testing.T) {
	// Test conversion functions for new types

	// Test MsgPosition conversion
	t.Run("MsgPosition conversion", func(t *testing.T) {
		original := &msgpb.MsgPosition{
			ChannelName: "test_channel",
			MsgID:       []byte{1, 2, 3, 4},
			MsgGroup:    "test_group",
			Timestamp:   12345,
		}
		avro := snapshotio.MsgPositionToAvro(original)
		restored := snapshotio.AvroToMsgPosition(avro)

		assert.Equal(t, original.ChannelName, restored.ChannelName)
		assert.Equal(t, original.MsgID, restored.MsgID)
		assert.Equal(t, original.MsgGroup, restored.MsgGroup)
		assert.Equal(t, original.Timestamp, restored.Timestamp)
	})

	// Test MsgPosition nil handling
	t.Run("MsgPosition nil handling", func(t *testing.T) {
		avro := snapshotio.MsgPositionToAvro(nil)
		assert.NotNil(t, avro)
		assert.Equal(t, "", avro.ChannelName)
		assert.Equal(t, []byte{}, avro.MsgID)

		restored := snapshotio.AvroToMsgPosition(nil)
		assert.Nil(t, restored)
	})

	// Test TextIndexStats conversion
	t.Run("TextIndexStats conversion", func(t *testing.T) {
		original := &datapb.TextIndexStats{
			FieldID:    100,
			Version:    1,
			Files:      []string{"/file1", "/file2"},
			LogSize:    1024,
			MemorySize: 2048,
			BuildID:    5000,
		}
		avro := snapshotio.TextIndexStatsToAvro(original)
		restored := snapshotio.AvroToTextIndexStats(avro)

		assert.Equal(t, original.FieldID, restored.FieldID)
		assert.Equal(t, original.Version, restored.Version)
		assert.Equal(t, original.Files, restored.Files)
		assert.Equal(t, original.LogSize, restored.LogSize)
		assert.Equal(t, original.MemorySize, restored.MemorySize)
		assert.Equal(t, original.BuildID, restored.BuildID)
	})

	// Test JsonKeyStats conversion
	t.Run("JsonKeyStats conversion", func(t *testing.T) {
		original := &datapb.JsonKeyStats{
			FieldID:                200,
			Version:                2,
			Files:                  []string{"/json/file1"},
			LogSize:                512,
			MemorySize:             1024,
			BuildID:                6000,
			JsonKeyStatsDataFormat: 1,
		}
		avro := snapshotio.JSONKeyStatsToAvro(original)
		restored := snapshotio.AvroToJSONKeyStats(avro)

		assert.Equal(t, original.FieldID, restored.FieldID)
		assert.Equal(t, original.Version, restored.Version)
		assert.Equal(t, original.Files, restored.Files)
		assert.Equal(t, original.LogSize, restored.LogSize)
		assert.Equal(t, original.MemorySize, restored.MemorySize)
		assert.Equal(t, original.BuildID, restored.BuildID)
		assert.Equal(t, original.JsonKeyStatsDataFormat, restored.JsonKeyStatsDataFormat)
	})

	// Test TextIndexMap conversion (map to array and back)
	t.Run("TextIndexMap conversion", func(t *testing.T) {
		originalMap := map[int64]*datapb.TextIndexStats{
			100: {FieldID: 100, Version: 1, Files: []string{"/file1"}},
			200: {FieldID: 200, Version: 2, Files: []string{"/file2"}},
		}
		avroArray := snapshotio.TextIndexMapToAvro(originalMap)
		restoredMap := snapshotio.AvroToTextIndexMap(avroArray)

		assert.Equal(t, len(originalMap), len(restoredMap))
		for fieldID, origStats := range originalMap {
			restStats, exists := restoredMap[fieldID]
			assert.True(t, exists)
			assert.Equal(t, origStats.FieldID, restStats.FieldID)
			assert.Equal(t, origStats.Version, restStats.Version)
			assert.Equal(t, origStats.Files, restStats.Files)
		}
	})

	// Test JsonKeyIndexMap conversion (map to array and back)
	t.Run("JsonKeyIndexMap conversion", func(t *testing.T) {
		originalMap := map[int64]*datapb.JsonKeyStats{
			100: {FieldID: 100, Version: 1, Files: []string{"/json1"}, JsonKeyStatsDataFormat: 1},
			200: {FieldID: 200, Version: 2, Files: []string{"/json2"}, JsonKeyStatsDataFormat: 2},
		}
		avroArray := snapshotio.JSONKeyIndexMapToAvro(originalMap)
		restoredMap := snapshotio.AvroToJSONKeyIndexMap(avroArray)

		assert.Equal(t, len(originalMap), len(restoredMap))
		for fieldID, origStats := range originalMap {
			restStats, exists := restoredMap[fieldID]
			assert.True(t, exists)
			assert.Equal(t, origStats.FieldID, restStats.FieldID)
			assert.Equal(t, origStats.Version, restStats.Version)
			assert.Equal(t, origStats.Files, restStats.Files)
			assert.Equal(t, origStats.JsonKeyStatsDataFormat, restStats.JsonKeyStatsDataFormat)
		}
	})
}

// =========================== StorageV2 Manifest Tests ===========================

func TestSnapshotWriter_Save_WithStorageV2Manifest(t *testing.T) {
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	defer cm.RemoveWithPrefix(context.Background(), "")

	writer := snapshotstorage.NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()

	// Add manifest_path to segment
	snapshotData.Segments[0].ManifestPath = "s3://bucket/collection/partition/segment1/manifest.json"

	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)

	// Read back the metadata and verify StorageV2ManifestList
	metadata := readSnapshotMetadataForTest(t, cm, metadataPath)
	assert.NotNil(t, metadata)
	assert.Len(t, metadata.GetStoragev2ManifestList(), 1)

	// Verify the manifest list content
	assert.Equal(t, int64(1001), metadata.GetStoragev2ManifestList()[0].GetSegmentId())
	assert.Equal(t, "s3://bucket/collection/partition/segment1/manifest.json", metadata.GetStoragev2ManifestList()[0].GetManifest())
}

func TestSnapshotReader_ReadSnapshot_WithStorageV2Manifest(t *testing.T) {
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	defer cm.RemoveWithPrefix(context.Background(), "")

	// Write a snapshot with StorageV2 manifest
	writer := snapshotstorage.NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()
	snapshotData.Segments[0].ManifestPath = "s3://bucket/collection/partition/segment1/manifest.json"

	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)

	// Read back the snapshot using the metadata path
	reader := snapshotstorage.NewSnapshotReader(cm)
	readData, err := reader.ReadSnapshot(context.Background(), metadataPath, true)
	assert.NoError(t, err)
	assert.NotNil(t, readData)
	assert.Len(t, readData.Segments, 1)

	// Verify manifest_path is restored
	assert.Equal(t, int64(1001), readData.Segments[0].GetSegmentId())
	assert.Equal(t, "s3://bucket/collection/partition/segment1/manifest.json", readData.Segments[0].GetManifestPath())
}

func TestSnapshotReader_ReadSnapshot_RebasesStorageV2ManifestListBeforeFillingSegments(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	reader := snapshotstorage.NewSnapshotReader(cm)
	ctx := context.Background()

	oldRoot := "export-root"
	newRoot := "restored/x"
	newMetadataPath := path.Join(newRoot, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotMetadataSubPath, "1.json")
	oldMetadataPath := path.Join(oldRoot, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotMetadataSubPath, "1.json")
	oldManifestPath := path.Join(oldRoot, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotManifestsSubPath, "1", "1001.avro")
	newManifestPath := path.Join(newRoot, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotManifestsSubPath, "1", "1001.avro")
	oldStorageV2BasePath := path.Join(oldRoot, "files", "insert_log", "100", "1", "1001")
	newStorageV2BasePath := path.Join(newRoot, "files", "insert_log", "100", "1", "1001")

	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "relocated",
			S3Location:   oldMetadataPath,
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test_collection"},
		},
		ManifestList: []string{oldManifestPath},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{
				SegmentId: 1001,
				Manifest:  packed.MarshalManifestPath(oldStorageV2BasePath, 9),
			},
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)

	segment := &datapb.SegmentDescription{
		SegmentId:         1001,
		PartitionId:       1,
		SegmentLevel:      datapb.SegmentLevel_L1,
		ChannelName:       "by-dev-rootcoord-dml_0_100v0",
		NumOfRows:         100,
		Binlogs:           []*datapb.FieldBinlog{},
		Statslogs:         []*datapb.FieldBinlog{},
		Deltalogs:         []*datapb.FieldBinlog{},
		Bm25Statslogs:     []*datapb.FieldBinlog{},
		TextIndexFiles:    map[int64]*datapb.TextIndexStats{},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{},
		IndexFiles:        []*indexpb.IndexFilePathInfo{},
		StartPosition:     &msgpb.MsgPosition{ChannelName: "by-dev-rootcoord-dml_0_100v0"},
		DmlPosition:       &msgpb.MsgPosition{ChannelName: "by-dev-rootcoord-dml_0_100v0"},
		StorageVersion:    2,
		CommitTimestamp:   10,
	}
	manifestBytes, err := snapshotio.MarshalSegmentManifest(segment)
	require.NoError(t, err)

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		switch filePath {
		case newMetadataPath:
			return metadataJSON, nil
		case newManifestPath:
			return manifestBytes, nil
		default:
			return nil, fmt.Errorf("unexpected file path: %s", filePath)
		}
	}).Build()
	defer mockRead.UnPatch()

	got, err := reader.ReadSnapshot(ctx, newMetadataPath, true)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)

	gotBasePath, gotVersion, err := packed.UnmarshalManifestPath(got.Segments[0].GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, newStorageV2BasePath, gotBasePath)
	assert.Equal(t, int64(9), gotVersion)
}

func TestSnapshotWriter_Save_EmptyManifestPath(t *testing.T) {
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	defer cm.RemoveWithPrefix(context.Background(), "")

	writer := snapshotstorage.NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()

	// Segment without manifest_path (StorageV1 segment)
	snapshotData.Segments[0].ManifestPath = ""

	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)

	// Read back the metadata
	metadata := readSnapshotMetadataForTest(t, cm, metadataPath)
	assert.NotNil(t, metadata)
	// StorageV2ManifestList should be empty since no segment has manifest_path
	assert.Len(t, metadata.GetStoragev2ManifestList(), 0)
}

// =========================== Format Version Tests ===========================

func TestValidateFormatVersion(t *testing.T) {
	tests := []struct {
		name        string
		version     int
		wantErr     bool
		errContains string
	}{
		{
			name:    "version_0_legacy",
			version: 0,
			wantErr: false,
		},
		{
			name:    "version_1_current",
			version: 1,
			wantErr: false,
		},
		{
			name:    "version_2_legacy",
			version: 2,
			wantErr: false,
		},
		{
			name:    "version_3_current",
			version: 3,
			wantErr: false,
		},
		{
			name:    "version_4_legacy",
			version: 4,
			wantErr: false,
		},
		{
			name:    "version_5_current",
			version: 5,
			wantErr: false,
		},
		{
			name:    "version_100_future",
			version: 100,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := snapshotio.ValidateFormatVersion(tt.version)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetManifestSchemaByVersion(t *testing.T) {
	tests := []struct {
		name        string
		version     int
		wantErr     bool
		errContains string
	}{
		{
			name:    "version_0_legacy",
			version: 0,
			wantErr: false,
		},
		{
			name:    "version_1_current",
			version: 1,
			wantErr: false,
		},
		{
			name:    "version_2_legacy",
			version: 2,
			wantErr: false,
		},
		{
			name:    "version_3_current",
			version: 3,
			wantErr: false,
		},
		{
			name:    "version_4_legacy",
			version: 4,
			wantErr: false,
		},
		{
			name:    "version_5_current",
			version: 5,
			wantErr: false,
		},
		{
			name:        "version_99_unsupported",
			version:     99,
			wantErr:     true,
			errContains: "unsupported manifest schema version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := snapshotio.ManifestSchemaByVersion(tt.version)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, schema)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
			}
		})
	}
}

func TestSnapshotMetadata_FormatVersion(t *testing.T) {
	// Test that FormatVersion is correctly serialized/deserialized
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1,
			Name: "test",
		},
		Collection:   &datapb.CollectionDescription{},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{},
	}

	// Serialize to JSON using protojson
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := marshalOpts.Marshal(metadata)
	assert.NoError(t, err)

	// Verify JSON contains format_version field (protojson uses snake_case with UseProtoNames)
	assert.Contains(t, string(data), `"format_version":`)

	// Deserialize back
	restored := &datapb.SnapshotMetadata{}
	unmarshalOpts := protojson.UnmarshalOptions{DiscardUnknown: true}
	err = unmarshalOpts.Unmarshal(data, restored)
	assert.NoError(t, err)
	assert.Equal(t, int32(snapshotstorage.SnapshotFormatVersion), restored.GetFormatVersion())
}

func TestSnapshotExportWholePlan_ErrorRedaction(t *testing.T) {
	req := &datapb.SnapshotExportCopyRequest{ExternalSpec: `{"extfs":{"access_key_id":"target-key","access_key_value":"target-secret","source_sas_token":"sig=a+b"}}`, SourceStorageConfig: &indexpb.StorageConfig{SecretAccessKey: "source-secret"}}
	reason := snapshotstorage.SanitizeSnapshotExportReason(fmt.Errorf("source-secret target-key target-secret sig=a+b sig%%3Da%%2Bb"), req)
	require.NotContains(t, reason, "source-secret")
	require.NotContains(t, reason, "target-secret")
	require.NotContains(t, reason, "target-key")
	require.NotContains(t, reason, "sig=a+b")
	require.NotContains(t, reason, "sig%3Da%2Bb")
	require.Empty(t, snapshotstorage.SanitizeSnapshotExportReason(nil, req))
	for _, message := range []string{strings.Repeat("x", 2000), "x" + strings.Repeat("复制失败🙂", 200)} {
		reason := snapshotstorage.SanitizeSnapshotExportReason(fmt.Errorf("%s source-secret", message), req)
		require.Equal(t, message+" <redacted>", reason)
		encoded, err := proto.Marshal(&datapb.SnapshotExportCopyResponse{Reason: reason})
		require.NoError(t, err)
		var decoded datapb.SnapshotExportCopyResponse
		require.NoError(t, proto.Unmarshal(encoded, &decoded))
		require.Equal(t, reason, decoded.GetReason())
	}
}

type snapshotWholePlanCopier struct{}

func (*snapshotWholePlanCopier) CopyCrossBucketAndGetSize(context.Context, string, string, string, string) (int64, error) {
	return 0, merr.ErrServiceInternal
}

func (c *snapshotWholePlanCopier) CopyCrossBucket(ctx context.Context, sourceBucket, sourcePath, targetBucket, targetPath string) error {
	_, err := c.CopyCrossBucketAndGetSize(ctx, sourceBucket, sourcePath, targetBucket, targetPath)
	return err
}

func mockSnapshotLocalCopy(t *testing.T, source, target storage.ChunkManager) *atomic.Int64 {
	t.Helper()
	copies := &atomic.Int64{}
	patch := mockey.Mock((*snapshotWholePlanCopier).CopyCrossBucketAndGetSize).To(func(ctx context.Context, _, src, _, dst string) (int64, error) {
		copies.Add(1)
		data, err := source.Read(ctx, src)
		if err != nil {
			return 0, err
		}
		return int64(len(data)), target.Write(ctx, dst, data)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return copies
}

// Persist the caller's snapshot and build matching worker inputs. Tests keep
// file creation, fault injection, execution and publication assertions explicit.
func snapshotExportFixture(t *testing.T, ctx context.Context, source, target storage.ChunkManager, snapshot *snapshotstorage.SnapshotData, cfg *indexpb.StorageConfig) (*datapb.SnapshotExportCopyRequest, *snapshotstorage.ResolvedForeignStorage) {
	t.Helper()
	metadata, _, err := snapshotstorage.NewSnapshotWriter(source).SaveToRootWithSize(ctx, snapshot, source.RootPath(), datapb.SnapshotLayout_SnapshotLayoutReferenced)
	require.NoError(t, err)
	req := &datapb.SnapshotExportCopyRequest{
		JobId: 1, TaskId: 1,
		SnapshotId: snapshot.SnapshotInfo.GetId(), CollectionId: snapshot.SnapshotInfo.GetCollectionId(), SegmentEnd: int64(len(snapshot.Segments)),
		SnapshotMetadataPath: metadata, TargetUri: target.RootPath(), SourceStorageConfig: cfg,
	}
	return req, &snapshotstorage.ResolvedForeignStorage{ForeignCM: target, ForeignBucket: cfg.GetBucketName(), ForeignStorageConfig: cfg, Copier: &snapshotWholePlanCopier{}}
}

func TestSnapshotExportPreparation(t *testing.T) {
	for _, mode := range []string{"success", "v2 metadata", "missing source", "wrong identity", "manifest read failure", "copy failure", "manifest encode failure", "missing data", "partial failure", "canceled", "cancel after decode"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			target := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			snapshot := &snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "snapshot"}, Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "collection"}}}
			file := path.Join(source.RootPath(), "data")
			require.NoError(t, source.Write(ctx, file, []byte("payload")))
			snapshot.Segments = []*datapb.SegmentDescription{{SegmentId: 1001, PartitionId: 1, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: file}}}}}}
			if mode == "v2 metadata" {
				segment := snapshot.Segments[0]
				segment.StorageVersion = storage.StorageV2
				segment.ManifestPath = path.Join(source.RootPath(), "native-manifest")
				require.NoError(t, source.Write(ctx, segment.ManifestPath, []byte("native metadata excluded from statistics")))
			}
			req, resolved := snapshotExportFixture(t, ctx, source, target, snapshot, &indexpb.StorageConfig{})
			if mode == "partial failure" {
				second := path.Join(source.RootPath(), "data2")
				snapshot.Segments[0].Binlogs[0].Binlogs = append(snapshot.Segments[0].Binlogs[0].Binlogs, &datapb.Binlog{LogPath: second})
				_, _, err := snapshotstorage.NewSnapshotWriter(source).SaveToRootWithSize(ctx, snapshot, source.RootPath(), datapb.SnapshotLayout_SnapshotLayoutReferenced)
				require.NoError(t, err)
				require.NoError(t, target.Write(ctx, path.Join(req.GetTargetUri(), "files/data"), []byte("payload")))
			}
			var copies atomic.Int64
			defer mockey.Mock((*snapshotWholePlanCopier).CopyCrossBucketAndGetSize).To(func(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) (int64, error) {
				src := srcObject
				dst := dstObject
				copies.Add(1)
				if mode == "copy failure" || mode == "partial failure" && path.Base(src) == "data2" {
					return 0, merr.ErrIoTooManyRequests
				}
				data, err := source.Read(ctx, src)
				if err != nil {
					return 0, err
				}
				return int64(len(data)), target.Write(ctx, dst, data)
			}).Build().UnPatch()
			switch mode {
			case "manifest read failure":
				defer mockey.Mock((*storage.LocalChunkManager).Read).To(func(_ context.Context, key string) ([]byte, error) {
					if strings.HasSuffix(key, ".avro") {
						return nil, merr.ErrIoPermissionDenied
					}
					return os.ReadFile(key)
				}).Build().UnPatch()
			case "cancel after decode":
				var parse func([]byte, int) (*datapb.SegmentDescription, error)
				defer mockey.Mock(snapshotio.ParseSegmentManifest).Origin(&parse).To(func(data []byte, version int) (*datapb.SegmentDescription, error) {
					segment, err := parse(data, version)
					cancel()
					return segment, err
				}).Build().UnPatch()
			case "manifest encode failure":
				defer mockey.Mock(snapshotio.MarshalSegmentManifest).Return(nil, merr.ErrServiceUnavailable).Build().UnPatch()
			case "missing source":
				req.SnapshotMetadataPath += ".missing"
			case "wrong identity":
				req.SnapshotId++
			case "missing data":
				require.NoError(t, source.Remove(ctx, path.Join(source.RootPath(), "data")))
			case "canceled":
				cancel()
			}
			var reported, reportedBytes int64
			result, err := snapshotstorage.ExecuteSnapshotExport(ctx, req, source, resolved, make(chan struct{}, 1), func(copied, copiedBytes, total int64) {
				reported = copied
				reportedBytes = copiedBytes
			})
			_, finalPath := snapshotstorage.GetSnapshotPaths(req.GetTargetUri(), 100, 1)
			exists, readErr := target.Exist(context.Background(), finalPath)
			require.NoError(t, readErr)
			require.False(t, exists, "DataNode must never publish final metadata")
			if mode != "success" && mode != "v2 metadata" {
				require.Error(t, err)
				if mode == "cancel after decode" {
					require.ErrorIs(t, err, context.Canceled)
					require.Zero(t, copies.Load(), "cancellation before planning must prevent object copies")
				}
				if mode == "partial failure" {
					data, readErr := target.Read(ctx, path.Join(req.GetTargetUri(), "files/data"))
					require.NoError(t, readErr)
					require.Equal(t, "payload", string(data), "failed export must retain confirmed writes and existing objects")
				}
				if mode == "manifest read failure" {
					require.Zero(t, copies.Load())
					require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
				}
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, copies.Load()+1, result.GetTotalFiles())
			require.Equal(t, result.GetTotalFiles(), reported)
			require.Equal(t, result.GetCopiedBytes(), reportedBytes)
			require.EqualValues(t, len("payload"), result.GetCopiedBytes())
			staged := snapshotstorage.GetSnapshotStagingMetadataPath(req.GetTargetUri())
			exists, err = target.Exist(ctx, staged)
			require.NoError(t, err)
			require.False(t, exists, "range workers must not prepare staging metadata")
			other := proto.Clone(req).(*datapb.SnapshotExportCopyRequest)
			other.TargetUri += "-another-job"
			require.NotEqual(t, req.GetTargetUri(), other.GetTargetUri())
		})
	}
}

func TestSnapshotExportCopyTimeSizes(t *testing.T) {
	for _, mode := range []string{"success", "data failure", "metadata failure", "negative size", "size overflow"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			a, b := path.Join(cm.RootPath(), "a"), path.Join(cm.RootPath(), "b")
			snapshot := &snapshotstorage.SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100},
				Collection:   &datapb.CollectionDescription{},
				Segments: []*datapb.SegmentDescription{{SegmentId: 1, Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{
					{LogPath: a, LogSize: 999}, {LogPath: b, LogSize: 999},
				}}}}},
			}
			snapshot.ManifestPaths = []string{path.Join(cm.RootPath(), "1.avro")}
			snapshot.ManifestFormatVersion = snapshotstorage.SnapshotFormatVersion
			// Referenced objects need not be accessed until their copy operation starts.
			defer mockey.Mock((*storage.LocalChunkManager).Size).To(func(context.Context, string) (int64, error) {
				t.Error("preparation must not query object sizes")
				return 0, merr.ErrIoKeyNotFound
			}).Build().UnPatch()
			cfg := &indexpb.StorageConfig{Address: "s3.us-west-2.amazonaws.com", BucketName: "target", CloudProvider: objectstorage.CloudProviderAWS, Region: "us-west-2", UseSSL: true}
			items, err := snapshotstorage.BuildSnapshotExportFilesForTest(ctx, cm, cm, "source", "target", snapshot, "s3://target/export", cfg, cfg)
			require.NoError(t, err)
			require.Len(t, items, 3)
			var copied, completed int64
			defer mockey.Mock((*snapshotWholePlanCopier).CopyCrossBucket).To(func(_ *snapshotWholePlanCopier, _ context.Context, srcBucket, src, dstBucket, dst string) error {
				require.Equal(t, snapshot.ManifestPaths[0], src)
				require.Equal(t, "source", srcBucket)
				require.Equal(t, "target", dstBucket)
				if mode == "metadata failure" {
					return merr.ErrIoPermissionDenied
				}
				return nil
			}).Build().UnPatch()
			copy := mockey.Mock((*snapshotWholePlanCopier).CopyCrossBucketAndGetSize).To(func(_ *snapshotWholePlanCopier, _ context.Context, srcBucket, srcObject, dstBucket, dstObject string) (int64, error) {
				src := srcObject
				dst := dstObject
				require.Equal(t, "source", srcBucket)
				require.Equal(t, "target", dstBucket)
				require.NotEqual(t, snapshot.ManifestPaths[0], src, "metadata uses unsized copy")
				require.Contains(t, dst, "export/files/")
				if mode == "negative size" {
					return -1, nil
				}
				if mode == "size overflow" {
					return math.MaxInt64, nil
				}
				if src == a {
					return int64(7), nil
				}
				require.Equal(t, b, src)
				require.EqualValues(t, 1, completed, "copy starts before querying later objects")
				if mode == "data failure" {
					return int64(100), merr.ErrIoKeyNotFound
				}
				return 0, nil // Empty objects count as successfully copied files.
			}).Build()
			defer copy.UnPatch()
			err = snapshotstorage.CopySnapshotExportPlan(ctx, items, "source", "target", &snapshotWholePlanCopier{}, make(chan struct{}, 1), func(size int64) { copied += size; completed++ })
			if mode == "negative size" || mode == "size overflow" {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				return
			}
			require.EqualValues(t, 7, copied)
			switch mode {
			case "data failure":
				require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
				require.EqualValues(t, 1, completed)
			case "metadata failure":
				require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
				require.EqualValues(t, 2, completed, "failed metadata copies do not report completion")
			default:
				require.NoError(t, err)
				require.EqualValues(t, 3, completed)
			}
		})
	}
}

func TestSnapshotExportRewrittenV6Manifests(t *testing.T) {
	for _, version := range []int32{0, 1, 2, 3, 4, 5} {
		for _, legacyBundle := range []bool{false, true} {
			t.Run(fmt.Sprintf("version=%d/legacy_bundle=%t", version, legacyBundle), func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				sourceRoot := path.Join(root, "source")
				source := storage.NewLocalChunkManager(objectstorage.RootPath(sourceRoot))
				target := storage.NewLocalChunkManager(objectstorage.RootPath(root))
				dataRoot := sourceRoot
				layout := datapb.SnapshotLayout_SnapshotLayoutReferenced
				if legacyBundle {
					dataRoot = path.Join(sourceRoot, "files")
					layout = datapb.SnapshotLayout_SnapshotLayoutSelfContained
				}
				file := path.Join(dataRoot, "insert/data")
				require.NoError(t, source.Write(ctx, file, []byte("payload")))
				segment := &datapb.SegmentDescription{
					SegmentId: 1001, PartitionId: 1,
					Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: file, LogID: 1}}}},
				}
				schema, err := snapshotio.ManifestSchemaByVersion(int(version))
				require.NoError(t, err)
				rawManifest, err := avro.Marshal(schema, snapshotio.SegmentToManifestEntry(segment))
				require.NoError(t, err)
				manifestDir, metadataPath := snapshotstorage.GetSnapshotPaths(sourceRoot, 100, 1)
				manifestPath := snapshotstorage.GetSegmentManifestPath(manifestDir, 1001)
				require.NoError(t, source.Write(ctx, manifestPath, rawManifest))
				metadata := &datapb.SnapshotMetadata{
					FormatVersion: version, Layout: layout,
					SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, S3Location: metadataPath, PinIds: []int64{10}},
					Collection:   &datapb.CollectionDescription{}, ManifestList: []string{manifestPath}, SegmentIds: []int64{1001},
				}
				data, err := protojson.Marshal(metadata)
				require.NoError(t, err)
				require.NoError(t, source.Write(ctx, metadataPath, data))
				if legacyBundle {
					// Re-export uses the actual root after a legacy bundle moves.
					movedSource := path.Join(root, "moved-source")
					require.NoError(t, os.Rename(sourceRoot, movedSource))
					_, metadataPath = snapshotstorage.GetSnapshotPaths(movedSource, 100, 1)
				}
				mockSnapshotLocalCopy(t, source, target)
				for generation := 0; generation < 2; generation++ {
					snapshot, err := snapshotstorage.NewSnapshotReader(source).ReadSnapshot(ctx, metadataPath, true)
					require.NoError(t, err)
					original := proto.Clone(snapshot.Segments[0])
					destination := path.Join(root, fmt.Sprintf("export-%d", generation))
					plan, err := snapshotstorage.BuildSnapshotExportPlanForTest(ctx, source, target, "bucket", "bucket", snapshot, destination, &indexpb.StorageConfig{}, nil)
					require.NoError(t, err)
					require.Len(t, plan.Items, 2, "data and generated Avro are both counted")
					// The decoded records are sufficient; source Avro is not reread/copied.
					require.NoError(t, source.Remove(ctx, snapshot.ManifestPaths[0]))
					var copiedBytes atomic.Int64
					require.NoError(t, snapshotstorage.CopySnapshotExportPlanForTest(ctx, plan, "bucket", "bucket", &snapshotWholePlanCopier{}, target, make(chan struct{}, 2), func(size int64) { copiedBytes.Add(size) }))
					require.EqualValues(t, len("payload"), copiedBytes.Load())
					err = snapshotstorage.PrepareSnapshotExportMetadataForTest(ctx, target, snapshot, plan.SnapshotExportPlan)
					require.NoError(t, err)
					require.True(t, proto.Equal(original, snapshot.Segments[0]), "export must not mutate the source segment")
					staging := snapshotstorage.GetSnapshotStagingMetadataPath(destination)
					staged, err := target.Read(ctx, staging)
					require.NoError(t, err)
					prepared, err := snapshotio.ParseSnapshotMetadata(staged)
					require.NoError(t, err)
					require.EqualValues(t, snapshotio.SnapshotFormatVersion, prepared.GetFormatVersion())
					require.Empty(t, prepared.GetSnapshotInfo().GetPinIds())
					_, err = snapshotstorage.NewSnapshotWriter(target).CommitStagedMetadata(ctx, staging, plan.MetadataURI, plan.MetadataURI)
					require.NoError(t, err)
					copiedManifest, err := target.Read(ctx, plan.ManifestPaths[0])
					require.NoError(t, err)
					require.NotEqual(t, rawManifest, copiedManifest)
					written, err := snapshotio.ParseSegmentManifest(copiedManifest, snapshotio.SnapshotFormatVersion)
					require.NoError(t, err)
					require.Equal(t, path.Join(destination, "files/insert/data"), written.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
					moved := path.Join(root, fmt.Sprintf("moved-%d", generation))
					require.NoError(t, os.Rename(destination, moved))
					_, metadataPath = snapshotstorage.GetSnapshotPaths(moved, 100, 1)
					restored, err := snapshotstorage.NewSnapshotReader(target).ReadSnapshot(ctx, metadataPath, true)
					require.NoError(t, err)
					resolved := restored.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath()
					require.Equal(t, path.Join(moved, "files/insert/data"), resolved)
					payload, err := target.Read(ctx, resolved)
					require.NoError(t, err)
					require.Equal(t, "payload", string(payload))
				}
			})
		}
	}
}

func manifestReadFixture(t *testing.T, count int) (*storage.LocalChunkManager, string, []string) {
	t.Helper()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshot := &snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{}}
	for i := 0; i < count; i++ {
		snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{SegmentId: int64(i + 1)})
		snapshot.SegmentIDs = append(snapshot.SegmentIDs, int64(i+1))
	}
	metadata, _, err := snapshotstorage.NewSnapshotWriter(cm).SaveToRootWithSize(context.Background(), snapshot, cm.RootPath(), datapb.SnapshotLayout_SnapshotLayoutReferenced)
	require.NoError(t, err)
	manifestDir, _ := snapshotstorage.GetSnapshotPaths(cm.RootPath(), 100, 1)
	paths := make([]string, count)
	for i := range paths {
		paths[i] = snapshotstorage.GetSegmentManifestPath(manifestDir, int64(i+1))
	}
	return cm, metadata, paths
}

type manifestReadResult struct {
	snapshot *snapshotstorage.SnapshotData
	err      error
}

func startManifestRead(ctx context.Context, reader *snapshotstorage.SnapshotReader, metadata string) <-chan manifestReadResult {
	done := make(chan manifestReadResult, 1)
	go func() {
		snapshot, err := reader.ReadSnapshot(ctx, metadata, true)
		done <- manifestReadResult{snapshot, err}
	}()
	return done
}

func TestSnapshotReader_ManifestReadsShareCopyPermits(t *testing.T) {
	cm, metadata, _ := manifestReadFixture(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	permits := make(chan struct{}, 2)
	copyStarted, releaseCopy := make(chan struct{}), make(chan struct{})
	readStarted, releaseReads := make(chan struct{}, 8), make(chan struct{})
	var active, maximum atomic.Int64
	enter := func() {
		n := active.Add(1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
	}
	defer mockey.Mock((*snapshotWholePlanCopier).CopyCrossBucketAndGetSize).To(func(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) (int64, error) {
		enter()
		defer active.Add(-1)
		close(copyStarted)
		select {
		case <-releaseCopy:
			return int64(1), nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}).Build().UnPatch()
	defer mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, key string) ([]byte, error) {
		if strings.HasSuffix(key, ".avro") {
			enter()
			defer active.Add(-1)
			readStarted <- struct{}{}
			select {
			case <-releaseReads:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return os.ReadFile(key)
	}).Build().UnPatch()
	copyDone := make(chan error, 1)
	go func() {
		copyDone <- snapshotstorage.CopySnapshotExportPlan(ctx, []snapshotstorage.FileCopy{{SourcePath: "source", DestinationPath: "target"}}, "source", "target", &snapshotWholePlanCopier{}, permits, nil)
	}()
	awaitSnapshotEvent(t, copyStarted)
	reader := snapshotstorage.NewSnapshotReader(cm, snapshotstorage.WithManifestReadPermits(permits))
	first, second := startManifestRead(ctx, reader, metadata), startManifestRead(ctx, reader, metadata)
	awaitSnapshotEvent(t, readStarted)
	assertSnapshotBlocked(t, readStarted)
	close(releaseReads)
	require.NoError(t, awaitSnapshotEvent(t, first).err)
	require.NoError(t, awaitSnapshotEvent(t, second).err)
	require.Len(t, permits, 1, "only the blocked copy still owns a permit")
	close(releaseCopy)
	require.NoError(t, awaitSnapshotEvent(t, copyDone))
	require.EqualValues(t, 2, maximum.Load())
	require.Zero(t, active.Load())
	require.Empty(t, permits)
}

func TestSnapshotReader_ConcurrentManifestFailure(t *testing.T) {
	for _, mode := range []string{"read failure", "decode failure", "cancel reading", "cancel waiting"} {
		t.Run(mode, func(t *testing.T) {
			cm, metadata, paths := manifestReadFixture(t, 50)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			permits := make(chan struct{}, 2)
			if mode == "cancel waiting" {
				permits <- struct{}{}
				permits <- struct{}{}
			}
			started := make(chan struct{}, 2)
			peerStarted := make(chan struct{})
			var peerOnce sync.Once
			var calls, active atomic.Int64
			defer mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, key string) ([]byte, error) {
				if !strings.HasSuffix(key, ".avro") {
					return os.ReadFile(key)
				}
				calls.Add(1)
				active.Add(1)
				defer active.Add(-1)
				if mode == "cancel reading" || mode == "cancel waiting" {
					started <- struct{}{}
					<-ctx.Done()
					return nil, ctx.Err()
				}
				if key == paths[0] {
					select {
					case <-peerStarted:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if mode == "read failure" {
						return nil, merr.ErrIoPermissionDenied
					}
					return []byte("invalid Avro"), nil
				}
				peerOnce.Do(func() { close(peerStarted) })
				<-ctx.Done()
				return nil, ctx.Err()
			}).Build().UnPatch()
			done := startManifestRead(ctx, snapshotstorage.NewSnapshotReader(cm, snapshotstorage.WithManifestReadPermits(permits)), metadata)
			switch mode {
			case "cancel reading":
				for i := 0; i < 2; i++ {
					awaitSnapshotEvent(t, started)
				}
				cancel()
			case "cancel waiting":
				assertSnapshotBlocked(t, started)
				cancel()
			}
			result := awaitSnapshotEvent(t, done)
			require.Nil(t, result.snapshot)
			switch mode {
			case "read failure":
				require.ErrorIs(t, result.err, merr.ErrIoPermissionDenied)
				require.ErrorContains(t, result.err, paths[0])
			case "decode failure":
				require.ErrorIs(t, result.err, merr.ErrDataIntegrity)
				require.ErrorContains(t, result.err, paths[0])
			default:
				require.ErrorIs(t, result.err, context.Canceled)
			}
			require.Zero(t, active.Load(), "all workers must exit before ReadSnapshot returns")
			if mode == "cancel waiting" {
				require.Zero(t, calls.Load())
				require.Len(t, permits, 2, "other operations retain their permits")
			} else {
				require.EqualValues(t, 2, calls.Load())
				require.Empty(t, permits)
			}
		})
	}
}

func TestSnapshotReader_CancellationWaitsForManifestDecode(t *testing.T) {
	cm, metadata, _ := manifestReadFixture(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	permits := make(chan struct{}, 1)
	decoding, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	defer mockey.Mock(snapshotio.ParseSegmentManifest).To(func([]byte, int) (*datapb.SegmentDescription, error) {
		close(decoding)
		<-release
		return &datapb.SegmentDescription{SegmentId: 1}, nil
	}).Build().UnPatch()
	done := startManifestRead(ctx, snapshotstorage.NewSnapshotReader(cm, snapshotstorage.WithManifestReadPermits(permits)), metadata)
	awaitSnapshotEvent(t, decoding)
	cancel()
	assertSnapshotBlocked(t, done)
	require.Len(t, permits, 1, "decode is included in the permit lifetime")
	once.Do(func() { close(release) })
	result := awaitSnapshotEvent(t, done)
	require.ErrorIs(t, result.err, context.Canceled)
	require.Nil(t, result.snapshot)
	require.Empty(t, permits)
}

// Exercise native manifest serialization and LOB production, then read the
// unchanged manifest after copying and relocating two generations of bundles.
func TestSnapshotExportNativeManifestInventory(t *testing.T) {
	for _, tc := range []struct {
		marker   *bool
		avroWins bool
	}{{nil, false}, {nil, true}, {proto.Bool(false), false}, {proto.Bool(false), true}, {proto.Bool(true), false}, {proto.Bool(true), true}} {
		t.Run(fmt.Sprintf("marker-%v/avro-wins-%t", tc.marker, tc.avroWins), func(t *testing.T) {
			marker, avroWins := tc.marker, tc.avroWins
			paramtable.Init()
			ctx := context.Background()
			root := t.TempDir()
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(path.Join(root, "source")))
			cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: cm.RootPath()}
			base := path.Join(cm.RootPath(), "insert_log/100/1/1001")
			lobRoot := path.Join(cm.RootPath(), "insert_log/100/1/lobs/100")
			schema := arrow.NewSchema([]arrow.Field{{Name: "100", Type: arrow.BinaryTypes.String, Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"100"})}}, nil)
			writer, err := packed.NewFFISegmentWriter(schema, &packed.SegmentWriterConfig{
				SegmentPath: base, WriterFormat: "parquet",
				TextColumns: []packed.TextColumnConfig{{
					FieldID: 100, LobBasePath: lobRoot, InlineThreshold: 1,
					MaxLobFileBytes: 1 << 20, FlushThresholdBytes: 1024,
				}},
			}, cfg, nil)
			require.NoError(t, err)
			builder := array.NewStringBuilder(memory.DefaultAllocator)
			defer builder.Release()
			text := strings.Repeat("snapshot-lob", 1024)
			builder.Append(text)
			values := builder.NewArray()
			defer values.Release()
			record := array.NewRecord(schema, []arrow.Array{values}, 1)
			defer record.Release()
			require.NoError(t, writer.Write(record))
			output, err := writer.Close()
			require.NoError(t, err)
			defer output.Destroy()
			stat := path.Join(base, "_stats/text.100/first")
			delta := path.Join(base, "_delta/first")
			indexRoot := path.Join(base, "_index/500/1")
			indexRelative, err := packed.ManifestIndexRelativePath(base, indexRoot)
			require.NoError(t, err)
			for _, file := range []string{stat, delta, path.Join(indexRoot, "index.bin"), path.Join(base, "unreferenced")} {
				require.NoError(t, cm.Write(ctx, file, []byte(file)))
			}
			manifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, 0), cfg, &packed.ManifestUpdates{
				NewFiles: output, DeltaLogs: []packed.DeltaLogEntry{{Path: delta, NumEntries: 1}},
				Stats: []packed.StatEntry{{Key: "text.100", Files: []string{stat}}},
				Indexes: []packed.ManifestIndexInfo{{
					Path: indexRelative, ColumnName: "100", IndexName: "idx", IndexType: "INVERTED",
					FieldID: 100, IndexID: 10, BuildID: 500, IndexVersion: 1, NumRows: 1,
					IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
					IndexFileKeys:         []string{"index.bin"},
				}},
			})
			require.NoError(t, err)
			_, version, err := packed.UnmarshalManifestPath(manifest)
			require.NoError(t, err)
			_, err = packed.CommitManifestUpdates(packed.MarshalManifestPath(base, version), cfg, &packed.ManifestUpdates{
				Stats: []packed.StatEntry{{Key: "text.100", Files: []string{path.Join(base, "unreferenced")}}},
			})
			require.NoError(t, err)
			original, err := packed.ReadManifestFiles(manifest, cfg)
			require.NoError(t, err)
			require.NotEmpty(t, original.ColumnFiles)
			require.NotEmpty(t, original.LobFiles)
			manifestBytes, err := cm.Read(ctx, original.ManifestFile)
			require.NoError(t, err)
			legacyStat := path.Join(cm.RootPath(), "stats_log/100/1/1001/100/10")
			avroIndex := path.Join(cm.RootPath(), "index_files/600/1/100/1001/legacy.idx")
			for _, file := range []string{legacyStat, avroIndex} {
				require.NoError(t, cm.Write(ctx, file, []byte(file)))
			}
			snapshot := &snapshotstorage.SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{},
				Indexes: []*indexpb.IndexInfo{{IndexID: 10}},
				Segments: []*datapb.SegmentDescription{{
					SegmentId: 1001, PartitionId: 1, StorageVersion: storage.StorageV3, ManifestHasIndex: marker, NumOfRows: 1, ManifestPath: manifest,
					Binlogs:           []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "stale/insert"}}}},
					Statslogs:         []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: legacyStat}}}},
					TextIndexFiles:    map[int64]*datapb.TextIndexStats{100: {Files: []string{"manifest-owned/text"}}},
					JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{101: {Files: []string{"manifest-owned/json"}}},
					IndexFiles:        []*indexpb.IndexFilePathInfo{{IndexID: 10, BuildID: 600, IndexName: "idx", IndexFilePaths: []string{avroIndex}}},
				}},
			}
			if !avroWins {
				snapshot.Segments[0].IndexFiles = nil
			}
			metadata, _, err := snapshotstorage.NewSnapshotWriter(cm).SaveToRootWithSize(ctx, snapshot, cm.RootPath(), datapb.SnapshotLayout_SnapshotLayoutReferenced)
			require.NoError(t, err)
			mockSnapshotLocalCopy(t, cm, cm)
			walk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).Return(merr.ErrServiceInternal).Build()
			defer walk.UnPatch()
			for generation := 0; generation < 2; generation++ {
				source, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadata, true)
				require.NoError(t, err)
				bundle := path.Join(root, fmt.Sprintf("bundle-%d", generation))
				before := proto.Clone(source.Segments[0])
				plan, err := snapshotstorage.BuildSnapshotExportPlanForTest(ctx, cm, cm, "bucket", "bucket", source, bundle, cfg, cfg)
				require.NoError(t, err)
				require.True(t, proto.Equal(before, source.Segments[0]), "planning must not mutate the source segment")
				var hasLegacyStat, hasAvroIndex bool
				for _, item := range plan.Items {
					hasLegacyStat = hasLegacyStat || strings.HasSuffix(item.SourcePath, "stats_log/100/1/1001/100/10")
					hasAvroIndex = hasAvroIndex || strings.HasSuffix(item.SourcePath, "legacy.idx")
					require.NotContains(t, item.SourcePath, "unreferenced")
					require.NotContains(t, item.SourcePath, "manifest-2.avro")
				}
				require.True(t, hasLegacyStat)
				require.Equal(t, avroWins, hasAvroIndex)
				require.NoError(t, snapshotstorage.CopySnapshotExportPlanForTest(ctx, plan, "bucket", "bucket", &snapshotWholePlanCopier{}, cm, make(chan struct{}, 2), nil))
				err = snapshotstorage.PrepareSnapshotExportMetadataForTest(ctx, cm, source, plan.SnapshotExportPlan)
				require.NoError(t, err)
				_, err = snapshotstorage.NewSnapshotWriter(cm).CommitStagedMetadata(ctx, snapshotstorage.GetSnapshotStagingMetadataPath(bundle), plan.MetadataURI, plan.MetadataURI)
				require.NoError(t, err)
				moved := path.Join(root, fmt.Sprintf("moved-%d", generation))
				if generation == 0 {
					// A nested destination must not apply the old root twice.
					moved = path.Join(bundle, "nested")
					temporary := path.Join(root, "relocating")
					require.NoError(t, os.Rename(bundle, temporary))
					require.NoError(t, os.MkdirAll(bundle, 0o755))
					require.NoError(t, os.Rename(temporary, moved))
				} else {
					require.NoError(t, os.Rename(bundle, moved))
				}
				_, err = cm.Read(ctx, plan.MetadataURI)
				require.Error(t, err, "old metadata must be unavailable after relocation")
				_, metadata = snapshotstorage.GetSnapshotPaths(moved, 100, 1)
				got, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadata, true)
				require.NoError(t, err)
				segment := got.Segments[0]
				require.Empty(t, segment.GetBinlogs())
				require.Equal(t, []string{"manifest-owned/text"}, segment.TextIndexFiles[100].Files)
				require.Equal(t, []string{"manifest-owned/json"}, segment.JsonKeyIndexFiles[101].Files)
				infos, _, err := packed.ExploreFilesReturnManifestPath(nil, "milvus-table", path.Join(root, "explore"), metadata,
					&indexpb.StorageConfig{StorageType: "local", BucketName: root, RootPath: root}, packed.ExternalSpecContext{Spec: `{"format":"milvus-table"}`})
				require.NoError(t, err)
				require.Len(t, infos, 1)
				require.Equal(t, segment.GetManifestPath(), infos[0].FilePath, "Restore and milvus-table resolve the same native entry point")
				files, err := packed.ReadManifestFiles(got.Segments[0].GetManifestPath(), cfg)
				require.NoError(t, err)
				copiedManifest, err := cm.Read(ctx, files.ManifestFile)
				require.NoError(t, err)
				require.Equal(t, manifestBytes, copiedManifest)
				paths := append(append(append([]string{}, files.ColumnFiles...), files.DeltaFiles...), files.StatsFiles...)
				for _, lob := range files.LobFiles {
					paths = append(paths, lob.Path)
				}
				for _, index := range files.Indexes {
					exists, err := cm.Exist(ctx, path.Join(index.Path, index.IndexFileKeys[0]))
					require.NoError(t, err)
					require.False(t, exists, "native-only index must not be exported even when Avro is empty")
				}
				for _, file := range paths {
					require.True(t, strings.HasPrefix(file, moved+"/files/"), file)
					_, err := cm.Read(ctx, file)
					require.NoError(t, err, file)
				}
				reader, err := packed.NewFFISegmentReader(got.Segments[0].GetManifestPath(), schema, []string{"100"}, 1024, cfg,
					[]packed.TextColumnConfig{{FieldID: 100, LobBasePath: path.Join(moved, "files/insert_log/100/1/lobs/100")}})
				require.NoError(t, err)
				read, err := reader.ReadNext()
				require.NoError(t, err)
				require.Equal(t, text, read.Column(0).(*array.String).Value(0))
				require.NoError(t, reader.Close())
				if generation == 0 {
					require.NoError(t, os.RemoveAll(cm.RootPath()))
				}
				if generation == 1 {
					// A manifest reference to a missing object must fail copying; it
					// must not disappear silently as it would from a directory walk.
					missing := files.ColumnFiles[0]
					if avroWins {
						missing = got.Segments[0].IndexFiles[0].IndexFilePaths[0]
					}
					require.NoError(t, cm.Remove(ctx, missing))
					req := &datapb.SnapshotExportCopyRequest{
						TaskId: 3, SegmentEnd: 1, SnapshotId: 1, CollectionId: 100, SnapshotMetadataPath: metadata,
						SourceStorageConfig: cfg, TargetUri: path.Join(root, "missing-object"),
					}
					result, err := snapshotstorage.ExecuteSnapshotExport(ctx, req, cm,
						&snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, ForeignStorageConfig: cfg, Copier: &snapshotWholePlanCopier{}}, make(chan struct{}, 2), nil)
					require.Error(t, err)
					require.Nil(t, result)
					exists, err := cm.Exist(ctx, snapshotstorage.GetSnapshotStagingMetadataPath(req.GetTargetUri()))
					require.NoError(t, err)
					require.False(t, exists)
				}
			}
		})
	}
}

func TestSnapshotExportManifestInventoryValidation(t *testing.T) {
	for _, mode := range []string{"native", "empty-nonempty-segment", "lob-outside-partition", "reference-outside-root", "frozen empty inventory"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			base := path.Join(cm.RootPath(), "insert_log/100/1/1001")
			snapshot := &snapshotstorage.SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{}, Indexes: []*indexpb.IndexInfo{{IndexID: 0}},
				Segments: []*datapb.SegmentDescription{{SegmentId: 1001, StorageVersion: storage.StorageV3, ManifestHasIndex: proto.Bool(true), NumOfRows: 1, ManifestPath: packed.MarshalManifestPath(base, 1)}},
			}
			if mode == "frozen empty inventory" {
				snapshot.Segments[0].NumOfRows = 0
			}
			metadata, _, err := snapshotstorage.NewSnapshotWriter(cm).SaveToRootWithSize(ctx, snapshot, cm.RootPath(), datapb.SnapshotLayout_SnapshotLayoutReferenced)
			require.NoError(t, err)
			files := &packed.ManifestFiles{ManifestFile: path.Join(base, "_metadata/manifest-1.avro")}
			var readErr error
			switch mode {
			case "frozen empty inventory":
				files = &packed.ManifestFiles{}
			case "native":
				readErr = merr.ErrServiceUnavailable
			case "empty-nonempty-segment":
				files.ManifestFile = ""
			case "lob-outside-partition":
				files.LobFiles = []packed.LobFileInfo{{Path: path.Join(cm.RootPath(), "insert_log/100/2/lobs/100/lob"), FieldID: 100}}
			case "reference-outside-root":
				files.ColumnFiles = []string{path.Join(path.Dir(cm.RootPath()), "outside/data")}
			}
			cfg := &indexpb.StorageConfig{BucketName: "source", RootPath: "frozen-root", AccessKeyID: "frozen-key"}
			calls := 0
			defer mockey.Mock(packed.ReadManifestFiles).To(func(_ string, config *indexpb.StorageConfig) (*packed.ManifestFiles, error) {
				require.Same(t, cfg, config, "worker must use the frozen source configuration")
				calls++
				return files, readErr
			}).Build().UnPatch()
			var copies atomic.Int64
			defer mockey.Mock((*snapshotWholePlanCopier).CopyCrossBucketAndGetSize).To(func(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string) (int64, error) {
				copies.Add(1)
				return int64(1), nil
			}).Build().UnPatch()
			req := &datapb.SnapshotExportCopyRequest{TaskId: 2, SegmentEnd: 1, SnapshotId: 1, CollectionId: 100, SnapshotMetadataPath: metadata, SourceStorageConfig: cfg, TargetUri: path.Join(cm.RootPath(), "target")}
			result, err := snapshotstorage.ExecuteSnapshotExport(ctx, req, cm, &snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, ForeignStorageConfig: &indexpb.StorageConfig{}, Copier: &snapshotWholePlanCopier{}}, make(chan struct{}, 2), nil)
			require.Equal(t, 1, calls, "reuse the pinned native inventory for LOB validation")
			if mode == "frozen empty inventory" {
				require.NoError(t, err)
				require.EqualValues(t, 1, result.GetTotalFiles(), "only generated Avro is counted")
			} else {
				require.Error(t, err)
				require.Nil(t, result)
			}
			if readErr != nil {
				require.ErrorIs(t, err, readErr)
			}
			require.Zero(t, copies.Load())
		})
	}
}

func TestSnapshotExportManifestInventoryConcurrency(t *testing.T) {
	for _, mode := range []string{"shared-permits", "cancel", "failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			snapshot := &snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{}}
			for i := int64(1); i <= 2; i++ {
				snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{SegmentId: i, StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath(path.Join(cm.RootPath(), fmt.Sprintf("insert_log/100/1/%d", i)), 1)})
			}
			metadata, _, err := snapshotstorage.NewSnapshotWriter(cm).SaveToRootWithSize(ctx, snapshot, cm.RootPath(), datapb.SnapshotLayout_SnapshotLayoutReferenced)
			require.NoError(t, err)
			started, release := make(chan struct{}, 2), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var calls atomic.Int64
			read := mockey.Mock(packed.ReadManifestFiles).To(func(string, *indexpb.StorageConfig) (*packed.ManifestFiles, error) {
				n := calls.Add(1)
				started <- struct{}{}
				if mode == "failure" && n == 2 {
					return nil, merr.ErrServiceUnavailable
				}
				<-release
				return &packed.ManifestFiles{}, nil
			}).Build()
			defer read.UnPatch()
			var copies atomic.Int64
			defer mockey.Mock((*snapshotWholePlanCopier).CopyCrossBucketAndGetSize).To(func(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string) (int64, error) {
				copies.Add(1)
				return int64(1), nil
			}).Build().UnPatch()
			permits := make(chan struct{}, 2)
			if mode == "shared-permits" {
				permits <- struct{}{}
			}
			done := make(chan error, 1)
			go func() {
				req := &datapb.SnapshotExportCopyRequest{TaskId: 2, SegmentEnd: 2, SnapshotId: 1, CollectionId: 100, SnapshotMetadataPath: metadata, SourceStorageConfig: &indexpb.StorageConfig{}, TargetUri: path.Join(cm.RootPath(), "target")}
				_, err := snapshotstorage.ExecuteSnapshotExport(ctx, req, cm, &snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, ForeignStorageConfig: &indexpb.StorageConfig{}, Copier: &snapshotWholePlanCopier{}}, permits, nil)
				done <- err
			}()
			awaitSnapshotEvent(t, started)
			if mode == "shared-permits" {
				assertSnapshotBlocked(t, started)
				<-permits
			}
			awaitSnapshotEvent(t, started)
			if mode == "cancel" {
				cancel()
			}
			assertSnapshotBlocked(t, done)
			unblock()
			err = awaitSnapshotEvent(t, done)
			switch mode {
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			case "failure":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			default:
				require.NoError(t, err)
			}
			require.Empty(t, permits)
			if mode != "shared-permits" {
				require.Zero(t, copies.Load())
			}
		})
	}
}

func TestCollectStorageV3DataFiles(t *testing.T) {
	cfg := &indexpb.StorageConfig{BucketName: "source-bucket"}
	manifest := packed.MarshalManifestPath("root/insert_log/1/2/3", 7)
	logs := func(paths ...string) []*datapb.FieldBinlog {
		logs := &datapb.FieldBinlog{}
		for _, p := range paths {
			logs.Binlogs = append(logs.Binlogs, &datapb.Binlog{LogPath: p})
		}
		return []*datapb.FieldBinlog{logs}
	}
	ctx := context.Background()
	native := &packed.ManifestFiles{
		ManifestFile: "root/insert_log/1/2/3/_metadata/manifest-7.avro",
		ColumnFiles:  []string{"root/insert_log/1/2/3/_data/1"}, DeltaFiles: []string{"root/insert_log/1/2/3/_delta/2"},
		StatsFiles: []string{"root/insert_log/1/2/3/_stats/text_index.100/a", "root/insert_log/1/2/3/_stats/json_stats.101/b"},
		LobFiles:   []packed.LobFileInfo{{Path: "root/insert_log/1/2/lobs/100/_data/1", FieldID: 100}},
		Indexes:    []packed.ManifestIndexInfo{{Path: "unselected-index-metadata"}},
	}
	segment := &datapb.SegmentDescription{
		SegmentId: 3, StorageVersion: storage.StorageV3, NumOfRows: 1, ManifestPath: manifest,
		Binlogs: logs("not-a-physical-v3-insert"), Deltalogs: logs("", native.DeltaFiles[0], "root/delta_log/1/2/3/3"),
		Statslogs: logs("root/stats_log/1/2/3/100/4"), Bm25Statslogs: logs("root/bm25_stats/1/2/3/100/5"),
		TextIndexFiles: map[int64]*datapb.TextIndexStats{100: {Files: []string{"stale-text-index"}}},
		IndexFiles:     []*indexpb.IndexFilePathInfo{{IndexFilePaths: []string{"selected-index"}}},
	}
	before := proto.Clone(segment)
	defer mockey.Mock(packed.ReadManifestFiles).To(func(got string, gotCfg *indexpb.StorageConfig) (*packed.ManifestFiles, error) {
		require.Equal(t, manifest, got)
		require.Same(t, cfg, gotCfg)
		return native, nil
	}).Build().UnPatch()
	got, err := snapshotstorage.CollectStorageV3DataFiles(ctx, segment, cfg)
	require.True(t, proto.Equal(before, segment))
	require.NoError(t, err)
	require.Same(t, native, got.Manifest)
	paths := make([]string, 0, len(got.Files))
	for _, ref := range got.Files {
		require.Equal(t, int64(3), ref.SegmentID)
		paths = append(paths, ref.Path)
	}
	require.ElementsMatch(t, []string{
		native.ManifestFile, native.ColumnFiles[0], native.DeltaFiles[0], native.StatsFiles[0], native.StatsFiles[1], native.LobFiles[0].Path,
		"root/delta_log/1/2/3/3", "root/stats_log/1/2/3/100/4", "root/bm25_stats/1/2/3/100/5",
	}, paths)
}

func TestCollectStorageV3DataFiles_EmptyPinnedManifest(t *testing.T) {
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	segment := &datapb.SegmentDescription{ManifestPath: packed.MarshalManifestPath(path.Join(cfg.RootPath, "insert_log/1/2/3"), 0)}
	got, err := snapshotstorage.CollectStorageV3DataFiles(context.Background(), segment, cfg)
	require.NoError(t, err)
	require.Empty(t, got.Files)
	segment.NumOfRows = 1
	got, err = snapshotstorage.CollectStorageV3DataFiles(context.Background(), segment, cfg)
	require.Error(t, err)
	require.Nil(t, got)
}

func TestSnapshotExportCopyRejectsUnsizedCopier(t *testing.T) {
	permits := make(chan struct{}, 1)
	require.Error(t, snapshotstorage.CopySnapshotExportPlan(context.Background(), nil, "", "", nil, permits, nil))
	require.Error(t, snapshotstorage.CopySnapshotExportPlan(context.Background(), nil, "", "", &struct{ storage.CrossBucketCopier }{}, permits, nil))
}

// snapshotShardChunkManager keeps object keys relative, like a remote bucket.
type snapshotShardChunkManager struct {
	storage.ChunkManager
	mu      sync.Mutex
	objects map[string][]byte
}

func (*snapshotShardChunkManager) RootPath() string { return "export" }
func (s *snapshotShardChunkManager) Write(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = append([]byte(nil), data...)
	return nil
}

func (s *snapshotShardChunkManager) Read(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, merr.WrapErrIoKeyNotFound(key)
	}
	return append([]byte(nil), data...), nil
}

func (s *snapshotShardChunkManager) Exist(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[key]
	return ok, nil
}

func TestSnapshotExportShards_CopyAndFinalize(t *testing.T) {
	ctx := context.Background()
	source := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	target := &snapshotShardChunkManager{objects: make(map[string][]byte)}

	shared := path.Join(source.RootPath(), "shared")
	require.NoError(t, source.Write(ctx, shared, []byte("shared-data")))
	snapshot := &snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{}, SegmentIDs: []int64{1001, 1002}}
	for _, id := range snapshot.SegmentIDs {
		snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{SegmentId: id, PartitionId: 1, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: shared}}}}})
	}
	// Include a V3 segment so top-level native entry points survive range reads
	// and finalization without rewriting either manifest format.
	base := path.Join(source.RootPath(), "insert_log/100/1/1002")
	native := path.Join(base, "manifest")
	require.NoError(t, source.Write(ctx, native, []byte("native manifest")))
	snapshot.Segments[1].StorageVersion = storage.StorageV3
	snapshot.Segments[1].ManifestPath = packed.MarshalManifestPath(base, 1)
	snapshot.Segments[1].Deltalogs = snapshot.Segments[1].Binlogs
	snapshot.Segments[1].Binlogs = nil
	defer mockey.Mock(packed.ReadManifestFiles).Return(&packed.ManifestFiles{ManifestFile: native}, nil).Build().UnPatch()

	cfg := &indexpb.StorageConfig{Address: "s3.us-west-2.amazonaws.com", BucketName: "source", CloudProvider: objectstorage.CloudProviderAWS, Region: "us-west-2", UseSSL: true}
	req, resolved := snapshotExportFixture(t, ctx, source, target, snapshot, cfg)
	copied := mockSnapshotLocalCopy(t, source, target)
	var tasks []*datapb.SnapshotExportCopyTask
	var duplicateBytes int64
	lateManifests := make(map[string][]byte)
	for i := int64(0); i < 2; i++ {
		req.TaskId = 10 + i
		req.SegmentStart = i
		req.SegmentEnd = i + 1
		result, err := snapshotstorage.ExecuteSnapshotExport(ctx, req, source, resolved, make(chan struct{}, 2), nil)
		require.NoError(t, err)
		require.NoError(t, snapshotstorage.ValidateSnapshotExportResult(req, result))
		require.EqualValues(t, 2+i, result.TotalFiles)
		for key := range target.objects {
			require.NotContains(t, key, "/_shards/")
		}
		// A lost assignment and its replacement write the same keys. Keep the
		// old OCF bytes to deliver after the replacement publishes metadata.
		manifestDir, _ := snapshotstorage.GetSnapshotPaths(req.GetTargetUri(), 100, 1)
		manifestKey := snapshotstorage.GetSegmentManifestPath(manifestDir, snapshot.SegmentIDs[i])
		oldBytes, err := target.Read(ctx, manifestKey)
		require.NoError(t, err)
		lateManifests[manifestKey] = oldBytes
		req.TaskId += 100
		retryResult, err := snapshotstorage.ExecuteSnapshotExport(ctx, req, source, resolved, make(chan struct{}, 2), nil)
		require.NoError(t, err)
		require.NoError(t, snapshotstorage.ValidateSnapshotExportResult(req, retryResult))
		require.Equal(t, result.CopiedBytes, retryResult.CopiedBytes)
		require.Equal(t, result.CopiedFiles, retryResult.CopiedFiles)
		newBytes, err := target.Read(ctx, manifestKey)
		require.NoError(t, err)
		require.NotEqual(t, oldBytes, newBytes, "V6 retries use fresh OCF sync markers")
		oldSegment, err := snapshotio.ParseSegmentManifest(oldBytes, snapshotio.SnapshotFormatVersion)
		require.NoError(t, err)
		newSegment, err := snapshotio.ParseSegmentManifest(newBytes, snapshotio.SnapshotFormatVersion)
		require.NoError(t, err)
		require.True(t, proto.Equal(oldSegment, newSegment))
		result = retryResult
		duplicateBytes += result.CopiedBytes
		tasks = append(tasks, &datapb.SnapshotExportCopyTask{TaskId: req.TaskId, SegmentStart: i, SegmentEnd: i + 1, Result: result})
		staged := snapshotstorage.GetSnapshotStagingMetadataPath(req.GetTargetUri())
		exists, err := target.Exist(ctx, staged)
		require.NoError(t, err)
		require.False(t, exists, "shards cannot prepare metadata")
	}
	require.EqualValues(t, 6, copied.Load(), "Avro is generated separately from SDK copies")
	dir, _ := snapshotstorage.GetSnapshotPaths(source.RootPath(), 100, 1)
	for _, id := range snapshot.SegmentIDs {
		require.NoError(t, source.Remove(ctx, snapshotstorage.GetSegmentManifestPath(dir, id)))
	}
	require.NoError(t, source.Remove(ctx, native))
	req.TaskId = 20
	result, err := snapshotstorage.FinalizeSnapshotExport(ctx, req, tasks, source, resolved)
	require.NoError(t, err)
	require.EqualValues(t, 5, result.TotalFiles, "counts sum successful shards, including shared destinations")
	require.Equal(t, duplicateBytes, result.TotalBytes)
	require.EqualValues(t, 2*len("shared-data"), result.TotalBytes, "metadata is excluded; the shared payload counts once per shard")
	require.EqualValues(t, 6, copied.Load(), "finalization does not recopy files")
	replayed, err := snapshotstorage.FinalizeSnapshotExport(ctx, req, tasks, source, resolved)
	require.NoError(t, err)
	require.Equal(t, result.TotalBytes, replayed.TotalBytes)
	require.Equal(t, result.TotalFiles, replayed.TotalFiles)

	data, err := target.Read(ctx, snapshotstorage.GetSnapshotStagingMetadataPath(req.GetTargetUri()))
	require.NoError(t, err)
	var staged datapb.SnapshotMetadata
	require.NoError(t, protojson.Unmarshal(data, &staged))
	require.Len(t, staged.ManifestList, 2)
	require.EqualValues(t, snapshotio.SnapshotFormatVersion, staged.FormatVersion)
	require.Len(t, staged.Storagev2ManifestList, 1)
	_, err = snapshotstorage.NewSnapshotWriter(target).CommitStagedMetadata(ctx, snapshotstorage.GetSnapshotStagingMetadataPath(req.GetTargetUri()), result.MetadataURI, result.MetadataURI)
	require.NoError(t, err)
	restored, err := snapshotstorage.NewSnapshotReader(target).ReadSnapshot(ctx, snapshotstorage.NormalizeSnapshotObjectPath(result.MetadataURI), true)
	require.NoError(t, err)
	require.Len(t, restored.Segments, 2)
	require.Equal(t, packed.MarshalManifestPath(path.Join(req.GetTargetUri(), "files/insert_log/100/1/1002"), 1), restored.Segments[1].GetManifestPath())

	for key, oldBytes := range lateManifests {
		require.NoError(t, target.Write(ctx, key, oldBytes))
	}
	afterLateWrite, err := snapshotstorage.NewSnapshotReader(target).ReadSnapshot(ctx, snapshotstorage.NormalizeSnapshotObjectPath(result.MetadataURI), true)
	require.NoError(t, err)
	for i, segment := range restored.Segments {
		require.True(t, proto.Equal(segment, afterLateWrite.Segments[i]), "late OCF writes must preserve Restore inputs")
	}

	for _, mode := range []string{"gap", "missing shard", "wrong task", "duplicate task", "wrong count", "unfinished result", "empty result", "negative size", "byte overflow", "count overflow", "active writer"} {
		t.Run(mode, func(t *testing.T) {
			broken := make([]*datapb.SnapshotExportCopyTask, len(tasks))
			for i, task := range tasks {
				broken[i] = proto.Clone(task).(*datapb.SnapshotExportCopyTask)
			}
			switch mode {
			case "gap":
				broken[0].SegmentStart = 1
			case "missing shard":
				broken = broken[:1]
			case "wrong task":
				broken[0].TaskId++
			case "duplicate task":
				broken[1].TaskId = broken[0].TaskId
			case "wrong count":
				broken[0].Result.TotalFiles++
			case "unfinished result":
				broken[0].Result.State = datapb.ImportTaskStateV2_InProgress
			case "empty result":
				broken[0].Result.TotalFiles, broken[0].Result.CopiedFiles = 0, 0
			case "negative size":
				broken[0].Result.CopiedBytes = -1
			case "byte overflow":
				broken[0].Result.CopiedBytes = math.MaxInt64
			case "count overflow":
				broken[0].Result.CopiedFiles = math.MaxInt64
				broken[0].Result.TotalFiles = math.MaxInt64
			case "active writer":
				broken[0].NodeId = 7
			}
			_, err := snapshotstorage.FinalizeSnapshotExport(ctx, req, broken, source, resolved)
			require.Error(t, err)
		})
	}
}

func TestDeduplicateFileCopies(t *testing.T) {
	t.Run("source aliases and multiple destinations", func(t *testing.T) {
		items := []snapshotstorage.FileCopy{
			{SourcePath: "s3://bucket/files/lob", DestinationPath: "target/lob"},
			{SourcePath: "files/lob", DestinationPath: "target/lob"},
			{SourcePath: "files/lob", DestinationPath: "other/lob"},
		}
		unique, err := snapshotstorage.DeduplicateFileCopies(items)
		require.NoError(t, err)
		require.Equal(t, []snapshotstorage.FileCopy{
			{SourcePath: "s3://bucket/files/lob", DestinationPath: "target/lob"},
			{SourcePath: "files/lob", DestinationPath: "other/lob"},
		}, unique)
		require.Same(t, &items[0], &unique[0], "reuse the owned plan array")
		require.Empty(t, items[2], "release references in the discarded tail")
	})
	t.Run("conflict returns no usable plan", func(t *testing.T) {
		unique, err := snapshotstorage.DeduplicateFileCopies([]snapshotstorage.FileCopy{
			{SourcePath: "files/one", DestinationPath: "target"},
			{SourcePath: "files/two", DestinationPath: "target"},
		})
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		require.Nil(t, unique)
	})
	t.Run("literal destination keys", func(t *testing.T) {
		items := []snapshotstorage.FileCopy{
			{SourcePath: "one", DestinationPath: "target/a"},
			{SourcePath: "two", DestinationPath: "target//a"},
			{SourcePath: "three", DestinationPath: "target/./a"},
		}
		unique, err := snapshotstorage.DeduplicateFileCopies(items)
		require.NoError(t, err)
		require.Len(t, unique, 3)
	})
	t.Run("empty", func(t *testing.T) {
		unique, err := snapshotstorage.DeduplicateFileCopies(nil)
		require.NoError(t, err)
		require.Nil(t, unique)
	})
}

func TestSnapshotMetadataFingerprint_ScopeAndLegacyValidation(t *testing.T) {
	snapshot := createTestSnapshotData()
	snapshot.ManifestFormatVersion = 5
	snapshot.ManifestPaths = []string{"files/snapshots/100/manifests/1/1001.avro"}
	snapshot.SegmentIDs = []int64{1001}
	snapshot.BuildIDs = []int64{101}
	snapshot.StorageManifests = []*datapb.StorageV2SegmentManifest{{SegmentId: 1001, Manifest: "files/native/manifest"}}

	original, err := snapshotstorage.SnapshotMetadataFingerprint(snapshot)
	require.NoError(t, err)
	full, err := snapshotstorage.SnapshotFingerprint(snapshot)
	require.NoError(t, err)
	require.NotEqual(t, full, original)
	require.NoError(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(snapshot, original))
	require.NoError(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(snapshot, full), "old WAL full fingerprints remain valid")
	require.NoError(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(nil, ""), "legacy absent fingerprints remain optional")

	metadata := *snapshot
	metadata.Segments = nil
	withoutSegments, err := snapshotstorage.SnapshotMetadataFingerprint(&metadata)
	require.NoError(t, err)
	require.Equal(t, original, withoutSegments)
	snapshot.Segments[0].NumOfRows++
	require.NoError(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(snapshot, original), "Avro baseline is captured at job creation")
	require.ErrorIs(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(snapshot, full), merr.ErrDataIntegrity)
	require.ErrorIs(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(snapshot, "metadata-v2:unsupported"), merr.ErrDataIntegrity)

	for _, tc := range []struct {
		name    string
		change  func(*snapshotstorage.SnapshotData)
		invalid bool
	}{
		{"schema", func(s *snapshotstorage.SnapshotData) {
			s.Collection = proto.Clone(s.Collection).(*datapb.CollectionDescription)
			s.Collection.Schema.Name = "changed"
		}, false},
		{"indexes", func(s *snapshotstorage.SnapshotData) { s.Indexes = []*indexpb.IndexInfo{{IndexID: 999}} }, false},
		{"manifests", func(s *snapshotstorage.SnapshotData) { s.ManifestPaths = []string{"files/other.avro"} }, false},
		{"segment IDs", func(s *snapshotstorage.SnapshotData) { s.SegmentIDs = []int64{999} }, false},
		{"build IDs", func(s *snapshotstorage.SnapshotData) { s.BuildIDs = []int64{999} }, false},
		{"storage manifests", func(s *snapshotstorage.SnapshotData) {
			s.StorageManifests = []*datapb.StorageV2SegmentManifest{{SegmentId: 1001, Manifest: "files/other"}}
		}, false},
		{"format", func(s *snapshotstorage.SnapshotData) { s.ManifestFormatVersion++ }, false},
		{"layout", func(s *snapshotstorage.SnapshotData) { s.Layout = datapb.SnapshotLayout_SnapshotLayoutSelfContained }, false},
		{"missing info", func(s *snapshotstorage.SnapshotData) { s.SnapshotInfo = nil }, true},
		{"missing collection", func(s *snapshotstorage.SnapshotData) { s.Collection = nil }, true},
		{"nil storage mapping", func(s *snapshotstorage.SnapshotData) { s.StorageManifests = []*datapb.StorageV2SegmentManifest{nil} }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := *snapshot
			tc.change(&changed)
			if tc.invalid {
				_, err := snapshotstorage.SnapshotMetadataFingerprint(&changed)
				require.Error(t, err)
				require.Error(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(&changed, original))
			} else {
				require.ErrorIs(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(&changed, original), merr.ErrDataIntegrity)
			}
		})
	}
	_, err = snapshotstorage.SnapshotMetadataFingerprint(nil)
	require.Error(t, err)
	require.Error(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(nil, original))
}

func TestSnapshotMetadataFingerprint_MetadataOnlyMatchesFullRead(t *testing.T) {
	for _, mode := range []string{"referenced", "self-contained", "relocated legacy"} {
		t.Run(mode, func(t *testing.T) {
			cm, location, _ := manifestReadFixture(t, 2)
			if mode != "referenced" {
				raw, err := cm.Read(context.Background(), location)
				require.NoError(t, err)
				metadata := &datapb.SnapshotMetadata{}
				require.NoError(t, protojson.Unmarshal(raw, metadata))
				metadata.Layout = datapb.SnapshotLayout_SnapshotLayoutSelfContained
				metadata.SnapshotInfo.S3Location = location
				raw, err = protojson.Marshal(metadata)
				require.NoError(t, err)
				require.NoError(t, cm.Write(context.Background(), location, raw))
				if strings.HasPrefix(mode, "relocated") {
					moved := path.Join(t.TempDir(), "bundle")
					require.NoError(t, os.Rename(cm.RootPath(), moved))
					location = moved + strings.TrimPrefix(location, cm.RootPath())
				}
			}
			reader := snapshotstorage.NewSnapshotReader(cm)
			metadata, err := reader.ReadSnapshot(context.Background(), location, false)
			require.NoError(t, err)
			full, err := reader.ReadSnapshot(context.Background(), location, true)
			require.NoError(t, err)
			fingerprint, err := snapshotstorage.SnapshotMetadataFingerprint(metadata)
			require.NoError(t, err)
			require.NoError(t, snapshotstorage.ValidateSnapshotPreflightFingerprint(full, fingerprint))
		})
	}
}

func TestSnapshotExportShards_MetadataOnlyFinalization(t *testing.T) {
	for _, mode := range []string{"plain native", "reexport", "legacy IDs", "identity", "read failure", "canceled", "write failure", "native decode", "unknown native", "duplicate segment", "missing segment", "invalid path", "URI failure", "missing bundle root", "canceled after staging"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := &snapshotShardChunkManager{objects: make(map[string][]byte)}
			target := &snapshotShardChunkManager{objects: make(map[string][]byte)}
			data := &snapshotstorage.SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{}, SegmentIDs: []int64{2, 1}, ManifestPaths: []string{"source/1.avro", "source/2.avro"}, ManifestFormatVersion: 5,
				StorageManifests: []*datapb.StorageV2SegmentManifest{{SegmentId: 1, Manifest: "source/native"}},
			}
			cfg := &indexpb.StorageConfig{}
			req := &datapb.SnapshotExportCopyRequest{SnapshotId: 1, CollectionId: 100, TargetUri: "destination"}
			var readErr error
			switch mode {
			case "reexport", "missing bundle root":
				data.Layout = datapb.SnapshotLayout_SnapshotLayoutSelfContained
				data.MetadataPath = "old-bundle/snapshots/100/metadata/1.json"
				data.StorageManifests[0].Manifest = packed.MarshalManifestPath("old-bundle/files/native", 7)
				if mode == "missing bundle root" {
					data.MetadataPath = "not-a-bundle.json"
				}
			case "legacy IDs":
				data.SegmentIDs = nil
			case "identity":
				data.SnapshotInfo.Id = 9
			case "read failure":
				readErr = merr.ErrServiceUnavailable
			case "canceled":
				cancel()
			case "native decode":
				data.StorageManifests[0].Manifest = "{broken"
			case "unknown native":
				data.StorageManifests[0].SegmentId = 9
			case "duplicate segment":
				data.SegmentIDs = []int64{1, 1}
			case "missing segment":
				data.SegmentIDs = []int64{1, 3}
			case "invalid path":
				data.ManifestPaths[0] = "source/one.avro"
			case "URI failure":
				cfg = nil
			}
			defer mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).Return(data, readErr).Build().UnPatch()
			if mode == "write failure" {
				defer mockey.Mock((*snapshotShardChunkManager).Write).Return(merr.ErrServiceUnavailable).Build().UnPatch()
			}
			if mode == "canceled after staging" {
				var read func(*snapshotShardChunkManager, context.Context, string) ([]byte, error)
				defer mockey.Mock((*snapshotShardChunkManager).Read).Origin(&read).To(func(cm *snapshotShardChunkManager, ctx context.Context, key string) ([]byte, error) {
					data, err := read(cm, ctx, key)
					cancel()
					return data, err
				}).Build().UnPatch()
			}

			tasks := []*datapb.SnapshotExportCopyTask{{TaskId: 1, SegmentEnd: 2, Result: &datapb.SnapshotExportCopyResponse{TaskId: 1, State: datapb.ImportTaskStateV2_Completed, TotalFiles: 2, CopiedFiles: 2}}}
			result, err := snapshotstorage.FinalizeSnapshotExport(ctx, req, tasks, source, &snapshotstorage.ResolvedForeignStorage{ForeignCM: target, ForeignStorageConfig: cfg})
			if mode != "plain native" && mode != "reexport" && mode != "legacy IDs" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			stagedBytes, err := target.Read(ctx, snapshotstorage.GetSnapshotStagingMetadataPath(req.GetTargetUri()))
			require.NoError(t, err)
			var top datapb.SnapshotMetadata
			require.NoError(t, protojson.Unmarshal(stagedBytes, &top))
			require.Len(t, top.ManifestList, 2)
			require.Zero(t, result.TotalBytes)
			if mode == "reexport" {
				require.Equal(t, packed.MarshalManifestPath("destination/files/native", 7), top.Storagev2ManifestList[0].Manifest)
			}
		})
	}
}

func TestSnapshotExportShards_BindAvroToMetadata(t *testing.T) {
	for _, mode := range []string{"wrong identity", "missing native mapping", "invalid identity path", "duplicate Avro identity"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			snapshot := &snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{}, Segments: []*datapb.SegmentDescription{{SegmentId: 1, NumOfRows: 1, StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath(path.Join(cm.RootPath(), "native"), 1)}}}
			req, resolved := snapshotExportFixture(t, ctx, cm, cm, snapshot, &indexpb.StorageConfig{})
			metadata := req.SnapshotMetadataPath
			bytes, err := cm.Read(ctx, metadata)
			require.NoError(t, err)
			var top datapb.SnapshotMetadata
			require.NoError(t, protojson.Unmarshal(bytes, &top))
			if mode == "wrong identity" || mode == "invalid identity path" {
				original, err := cm.Read(ctx, top.ManifestList[0])
				require.NoError(t, err)
				name := "2.avro"
				if mode == "invalid identity path" {
					name = "one.avro"
				}
				top.ManifestList[0] = path.Join(cm.RootPath(), name)
				require.NoError(t, cm.Write(ctx, top.ManifestList[0], original))
			} else {
				top.Storagev2ManifestList = nil
			}
			if mode == "duplicate Avro identity" {
				top.ManifestList = append(top.ManifestList, top.ManifestList[0])
				req.SegmentEnd = 2
			}
			bytes, err = protojson.Marshal(&top)
			require.NoError(t, err)
			require.NoError(t, cm.Write(ctx, metadata, bytes))
			_, err = snapshotstorage.ExecuteSnapshotExport(ctx, req, cm, resolved, make(chan struct{}, 1), nil)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
		})
	}
}

func TestSnapshotExportAvroWrites_ConcurrencyAndDrain(t *testing.T) {
	for _, mode := range []string{"success", "write failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			source, metadata, _ := manifestReadFixture(t, 6)
			target := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := &datapb.SnapshotExportCopyRequest{
				TaskId:     10,
				SnapshotId: 1, CollectionId: 100, SnapshotMetadataPath: metadata, TargetUri: target.RootPath(), SegmentEnd: 6,
			}
			permits := make(chan struct{}, 2)
			started := make(chan struct{}, 6)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			fail := make(chan struct{})
			failed := make(chan struct{})
			var active, maximum atomic.Int64
			writer := &snapshotAvroControlledWriter{ChunkManager: target}
			writer.write = func(writeCtx context.Context, key string, data []byte) error {
				n := active.Add(1)
				defer active.Add(-1)
				for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
				}
				started <- struct{}{}
				if mode == "write failure" && path.Base(key) == "1.avro" {
					<-fail
					close(failed)
					return merr.ErrIoTooManyRequests
				}
				// Model a provider write that cannot stop immediately on cancellation.
				<-release
				if err := writeCtx.Err(); err != nil {
					return err
				}
				return target.Write(writeCtx, key, data)
			}

			type outcome struct {
				result *datapb.SnapshotExportCopyResponse
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				result, err := snapshotstorage.ExecuteSnapshotExport(ctx, req, source, &snapshotstorage.ResolvedForeignStorage{
					ForeignCM: writer, ForeignStorageConfig: &indexpb.StorageConfig{}, Copier: &snapshotWholePlanCopier{},
				}, permits, nil)
				done <- outcome{result, err}
			}()
			for i := 0; i < 2; i++ {
				awaitSnapshotEvent(t, started)
			}
			require.EqualValues(t, 2, active.Load())
			require.Equal(t, 2, len(permits), "Avro writes consume the shared task permits")
			switch mode {
			case "cancel":
				cancel()
			case "write failure":
				close(fail)
				<-failed
			}
			if mode != "success" {
				assertSnapshotBlocked(t, done)
			}
			unblock()
			got := awaitSnapshotEvent(t, done)
			require.Zero(t, active.Load())
			require.Empty(t, permits)
			require.EqualValues(t, 2, maximum.Load())
			if mode == "success" {
				require.NoError(t, got.err)
				require.Equal(t, datapb.ImportTaskStateV2_Completed, got.result.GetState())
				require.EqualValues(t, 6, got.result.GetCopiedFiles())
				require.Zero(t, got.result.GetCopiedBytes())
				manifestDir, _ := snapshotstorage.GetSnapshotPaths(req.GetTargetUri(), 100, 1)
				for i := int64(1); i <= 6; i++ {
					data, err := target.Read(ctx, snapshotstorage.GetSegmentManifestPath(manifestDir, i))
					require.NoError(t, err)
					segment, err := snapshotio.ParseSegmentManifest(data, snapshotio.SnapshotFormatVersion)
					require.NoError(t, err)
					require.Equal(t, i, segment.GetSegmentId())
				}
			} else {
				require.Nil(t, got.result)
				if mode == "write failure" {
					require.ErrorIs(t, got.err, merr.ErrIoTooManyRequests)
				} else {
					require.ErrorIs(t, got.err, context.Canceled)
				}
			}
			exists, err := target.Exist(context.Background(), snapshotstorage.GetSnapshotStagingMetadataPath(req.GetTargetUri()))
			require.NoError(t, err)
			require.False(t, exists, "workers never publish staging, including on Avro failure")
		})
	}
}

// A controlled provider implementation avoids patching an Origin trampoline
// while parallel callbacks execute, and can model delayed cancellation safely.
type snapshotAvroControlledWriter struct {
	storage.ChunkManager
	write func(context.Context, string, []byte) error
}

func (w *snapshotAvroControlledWriter) Write(ctx context.Context, key string, data []byte) error {
	return w.write(ctx, key, data)
}

// A segment reader can block after peers have already copied and written Avro.
type snapshotSegmentControlledReader struct {
	storage.ChunkManager
	read func(context.Context, string) ([]byte, error)
}

func (r *snapshotSegmentControlledReader) Read(ctx context.Context, key string) ([]byte, error) {
	return r.read(ctx, key)
}

func TestSnapshotExportSegmentPipeline(t *testing.T) {
	// Initialize configuration before starting the bounded execution deadline.
	paramtable.Init()
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("late-read-failure-%t", fail), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			source := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			target := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			shared := path.Join(source.RootPath(), "shared")
			snapshot := &snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{}}
			for id := int64(1); id <= 2; id++ {
				snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
					SegmentId: id,
					Binlogs:   []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: shared}}}},
				})
			}
			require.NoError(t, source.Write(ctx, shared, []byte("shared")))
			req, resolved := snapshotExportFixture(t, ctx, source, target, snapshot, &indexpb.StorageConfig{})
			blocked, release, firstWritten := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			reader := &snapshotSegmentControlledReader{ChunkManager: source}
			reader.read = func(ctx context.Context, key string) ([]byte, error) {
				if path.Base(key) == "2.avro" {
					close(blocked)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if fail {
						return nil, merr.ErrIoKeyNotFound
					}
				}
				return source.Read(ctx, key)
			}
			writer := &snapshotAvroControlledWriter{ChunkManager: target}
			writer.write = func(ctx context.Context, key string, data []byte) error {
				err := target.Write(ctx, key, data)
				if path.Base(key) == "1.avro" {
					close(firstWritten)
				}
				return err
			}
			copies := mockSnapshotLocalCopy(t, source, target)
			resolved.ForeignCM = writer
			permits := make(chan struct{}, 2)
			done := make(chan error, 1)
			drained := make(chan struct{})
			var result *datapb.SnapshotExportCopyResponse
			go func() {
				defer close(drained)
				var err error
				result, err = snapshotstorage.ExecuteSnapshotExport(ctx, req, reader, resolved, permits, nil)
				done <- err
			}()
			defer func() {
				unblock()
				cancel()
				<-drained
			}()
			awaitSnapshotEvent(t, blocked)
			awaitSnapshotEvent(t, firstWritten)
			require.EqualValues(t, 1, copies.Load())
			select {
			case <-done:
				t.Fatal("task completed with an active segment")
			default:
			}
			unblock()
			err := <-done
			require.Empty(t, permits)
			if fail {
				require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 2, copies.Load(), "shared objects may be copied by both segments")
				require.EqualValues(t, 4, result.TotalFiles)
				require.EqualValues(t, 2*len("shared"), result.CopiedBytes)
			}
		})
	}
}

func TestSnapshotExportSegmentStatisticsOverflow(t *testing.T) {
	ctx := context.Background()
	source := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	target := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshot := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100},
		Collection:   &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "test"}},
		SegmentIDs:   []int64{1, 2},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: source.RootPath() + "/data1"}}}}},
			{SegmentId: 2, Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: source.RootPath() + "/data2"}}}}},
		},
	}
	metadata, err := snapshotstorage.NewSnapshotWriter(source).Save(ctx, snapshot)
	require.NoError(t, err)
	copier := &snapshotWholePlanCopier{}
	defer mockey.Mock((*snapshotWholePlanCopier).CopyCrossBucketAndGetSize).Return(int64(math.MaxInt64), nil).Build().UnPatch()
	result, err := snapshotstorage.ExecuteSnapshotExport(ctx, &datapb.SnapshotExportCopyRequest{
		TaskId: 1, SnapshotId: 1, CollectionId: 100, SegmentEnd: 2,
		SnapshotMetadataPath: metadata, TargetUri: target.RootPath(),
		SourceStorageConfig: &indexpb.StorageConfig{BucketName: "source"},
	}, source, &snapshotstorage.ResolvedForeignStorage{
		ForeignCM: target, ForeignBucket: "target", Copier: copier,
		ForeignStorageConfig: &indexpb.StorageConfig{StorageType: "remote", CloudProvider: "aws", BucketName: "target", Address: "s3.us-west-2.amazonaws.com", Region: "us-west-2", UseSSL: true},
	}, make(chan struct{}, 1), nil)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "statistics overflow")
	require.Nil(t, result)
}

func TestSnapshotTaskCopyPermits(t *testing.T) {
	for _, tc := range []struct {
		limit int32
		want  int
	}{
		{0, 128}, {-1, 128}, {3, 3}, {128, 128}, {300, 300},
	} {
		first, second := snapshotstorage.NewTaskCopyPermits(tc.limit), snapshotstorage.NewTaskCopyPermits(tc.limit)
		require.Equal(t, tc.want, cap(first))
		require.Equal(t, tc.want, cap(second))
		first <- struct{}{}
		require.Empty(t, second, "different tasks must not share permits")
	}
}

func TestSnapshotReader_ManifestReadModes(t *testing.T) {
	for _, tc := range []struct {
		name                                                    string
		count                                                   int
		permits                                                 chan struct{}
		configured, metadataOnly, canceled, parallel, wantError bool
	}{
		{name: "default serial", count: 3},
		{name: "serial canceled", count: 3, canceled: true, wantError: true},
		{name: "nil permits", count: 3, configured: true},
		{name: "single", count: 1, configured: true, permits: make(chan struct{}, 2)},
		{name: "empty", configured: true, permits: make(chan struct{}, 2)},
		{name: "metadata only", count: 3, configured: true, permits: make(chan struct{}, 2), metadataOnly: true},
		{name: "unbuffered", count: 3, configured: true, permits: make(chan struct{}), wantError: true},
		{name: "parallel ordered", count: 8, configured: true, permits: make(chan struct{}, 2), parallel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm, metadata, paths := manifestReadFixture(t, tc.count)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var options []snapshotstorage.SnapshotReaderOption
			if tc.configured {
				options = []snapshotstorage.SnapshotReaderOption{snapshotstorage.WithManifestReadPermits(tc.permits)}
			}
			started, release := make(chan struct{}), make(chan struct{})
			var mu sync.Mutex
			var reads []string
			var active, maximum, others atomic.Int64
			defer mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, key string) ([]byte, error) {
				if strings.HasSuffix(key, ".avro") {
					n := active.Add(1)
					defer active.Add(-1)
					for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
					}
					mu.Lock()
					reads = append(reads, key)
					mu.Unlock()
					if tc.parallel && key == paths[0] {
						close(started)
						select {
						case <-release:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					} else {
						others.Add(1)
					}
				}
				return os.ReadFile(key)
			}).Build().UnPatch()
			if tc.canceled {
				cancel()
			}
			done := make(chan manifestReadResult, 1)
			go func() {
				got, err := snapshotstorage.NewSnapshotReader(cm, options...).ReadSnapshot(ctx, metadata, !tc.metadataOnly)
				done <- manifestReadResult{got, err}
			}()
			if tc.parallel {
				awaitSnapshotEvent(t, started)
				require.Eventually(t, func() bool { return others.Load() == 7 }, time.Second, time.Millisecond)
				close(release)
			}
			got := awaitSnapshotEvent(t, done)
			if tc.wantError {
				require.Error(t, got.err)
				if tc.canceled {
					require.ErrorIs(t, got.err, context.Canceled)
				}
				require.Nil(t, got.snapshot)
				require.Empty(t, reads)
				return
			}
			require.NoError(t, got.err)
			if tc.metadataOnly || tc.count == 0 {
				require.Empty(t, reads)
				require.Empty(t, got.snapshot.Segments)
			} else if tc.parallel {
				require.EqualValues(t, 2, maximum.Load())
				require.Equal(t, paths, got.snapshot.ManifestPaths)
				for i, segment := range got.snapshot.Segments {
					require.EqualValues(t, i+1, segment.GetSegmentId())
				}
			} else {
				require.Equal(t, paths, reads)
				require.EqualValues(t, 1, maximum.Load())
			}
			require.Empty(t, tc.permits)
		})
	}
}

func assertSnapshotBlocked[T any](t *testing.T, events <-chan T) {
	t.Helper()
	select {
	case value := <-events:
		t.Errorf("snapshot operation advanced before release: %v", value)
	case <-time.After(20 * time.Millisecond):
	}
}

func awaitSnapshotEvent[T any](t *testing.T, events <-chan T) T {
	t.Helper()
	select {
	case value := <-events:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for snapshot operation")
		var zero T
		return zero
	}
}

func TestSnapshotReader_ReadSnapshot_LegacyVersion(t *testing.T) {
	// Test reading legacy snapshot without FormatVersion field (version 0)
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := snapshotstorage.NewSnapshotReader(cm)

	// Create legacy metadata without FormatVersion (simulating old snapshots)
	// Note: protojson supports snake_case (proto names) and camelCase (JSON names)
	// We use snake_case here to simulate a snapshot created with protojson defaults
	legacyMetadata := map[string]interface{}{
		"snapshot_info": map[string]interface{}{
			"id":   1,
			"name": "legacy_snapshot",
		},
		"collection":    map[string]interface{}{},
		"indexes":       []interface{}{},
		"manifest_list": []string{},
		// Note: No "format_version" field - this is the legacy case (version 0)
	}
	metadataJSON, _ := json.Marshal(legacyMetadata)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataFilePath {
			return metadataJSON, nil
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	// Reading legacy snapshot without manifest should succeed
	// withSegments=false to avoid needing manifest files
	snapshot, err := reader.ReadSnapshot(context.Background(), metadataFilePath, false)
	assert.NoError(t, err) // Should succeed with version 0 (legacy)
	assert.NotNil(t, snapshot)
	assert.Equal(t, "legacy_snapshot", snapshot.SnapshotInfo.GetName())
}

func TestSnapshotReader_ReadSnapshot_FutureVersion(t *testing.T) {
	// A future writer version alone does not prevent metadata reading.
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := snapshotstorage.NewSnapshotReader(cm)

	futureMetadata := &datapb.SnapshotMetadata{
		FormatVersion: 999, // Future version
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1,
			Name: "future_snapshot",
		},
		Collection:   &datapb.CollectionDescription{},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{},
	}
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	metadataJSON, _ := marshalOpts.Marshal(futureMetadata)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataFilePath {
			return metadataJSON, nil
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	snapshot, err := reader.ReadSnapshot(context.Background(), metadataFilePath, false)
	require.NoError(t, err)
	assert.Equal(t, "future_snapshot", snapshot.SnapshotInfo.GetName())
}

func TestSnapshotReader_FutureManifest(t *testing.T) {
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	ctx := context.Background()
	metadataPath := path.Join(root, "metadata.json")
	manifestPath := path.Join(root, "segment.avro")
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: 7,
		SnapshotInfo:  &datapb.SnapshotInfo{Id: 1, CollectionId: 10, Name: "future", S3Location: metadataPath},
		Collection:    &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "source"}},
		ManifestList:  []string{manifestPath},
		SegmentIds:    []int64{123},
	}
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, cm.Write(ctx, metadataPath, data))
	data, err = snapshotio.MarshalSegmentManifest(&datapb.SegmentDescription{SegmentId: 123})
	require.NoError(t, err)
	require.NoError(t, cm.Write(ctx, manifestPath, data))
	snapshot, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
	require.NoError(t, err)
	require.Len(t, snapshot.Segments, 1)
	assert.Equal(t, int64(123), snapshot.Segments[0].GetSegmentId())
}

func TestSnapshotExportCopyErrorContext(t *testing.T) {
	const secret = "source-secret"
	for _, cause := range []error{context.DeadlineExceeded, merr.ErrIoKeyNotFound, merr.Wrapf(merr.ErrIoPermissionDenied, "credential %s", secret)} {
		t.Run(cause.Error(), func(t *testing.T) {
			item := snapshotstorage.FileCopy{SourcePath: "files/source\nobject", DestinationPath: "snapshots/target-object"}
			defer mockey.Mock((*snapshotWholePlanCopier).CopyCrossBucketAndGetSize).Return(int64(0), cause).Build().UnPatch()
			err := snapshotstorage.CopySnapshotExportPlan(context.Background(), []snapshotstorage.FileCopy{item}, "source-bucket", "target-bucket", &snapshotWholePlanCopier{}, make(chan struct{}, 1), func(int64) { t.Error("failed copy must not report completion") })
			require.ErrorIs(t, err, cause)
			require.Equal(t, merr.Code(cause), merr.Code(err))
			reason := snapshotstorage.SanitizeSnapshotExportReason(err, &datapb.SnapshotExportCopyRequest{SourceStorageConfig: &indexpb.StorageConfig{SecretAccessKey: secret}})
			for _, value := range []string{"source-bucket", item.SourcePath, "target-bucket", item.DestinationPath} {
				require.Contains(t, reason, fmt.Sprintf("%q", value))
			}
			require.NotContains(t, reason, secret)
		})
	}
}
