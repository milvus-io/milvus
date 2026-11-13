// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segment

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAllocNewGrowingSegment(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	t.Run("segment is registered as growing", func(t *testing.T) {
		mt, err := meta.NewMemoryMeta(t)
		assert.NoError(t, err)
		mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: meta.NewTestSchema()})
		segmentManager := NewSegmentManager(mt)

		segment, err := segmentManager.AllocNewGrowingSegment(ctx, AllocNewGrowingSegmentRequest{
			CollectionID:         1,
			PartitionID:          100,
			SegmentID:            1000,
			ChannelName:          "c1",
			StorageVersion:       storage.StorageV2,
			IsCreatedByStreaming: true,
			SchemaVersion:        3,
		})
		assert.NoError(t, err)
		assert.EqualValues(t, 1000, segment.GetID())

		got := mt.GetHealthySegment(ctx, 1000)
		assert.NotNil(t, got)
		assert.EqualValues(t, 1, got.GetCollectionID())
		assert.EqualValues(t, 100, got.GetPartitionID())
		assert.Equal(t, "c1", got.GetInsertChannel())
		assert.Equal(t, commonpb.SegmentState_Growing, got.GetState())
		assert.Equal(t, datapb.SegmentLevel_L1, got.GetLevel())
		assert.EqualValues(t, 0, got.GetNumOfRows())
		assert.EqualValues(t, storage.StorageV2, got.GetStorageVersion())
		assert.True(t, got.GetIsCreatedByStreaming())
		assert.EqualValues(t, 3, got.GetSchemaVersion())
		// The row limit of a growing segment is decided by the streaming node.
		assert.EqualValues(t, 0, got.GetMaxRowNum())
	})

	t.Run("save failure is returned and nothing is registered", func(t *testing.T) {
		mt, err := meta.NewMemoryMeta(t)
		assert.NoError(t, err)
		mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: meta.NewTestSchema()})
		segmentManager := NewSegmentManager(mt)

		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		mt.Catalog = &datacoord.Catalog{MetaKv: metakv}

		segment, err := segmentManager.AllocNewGrowingSegment(ctx, AllocNewGrowingSegmentRequest{
			CollectionID:         1,
			PartitionID:          100,
			SegmentID:            1001,
			ChannelName:          "c1",
			StorageVersion:       storage.StorageV2,
			IsCreatedByStreaming: true,
		})
		assert.Error(t, err)
		assert.Nil(t, segment)
		assert.Nil(t, mt.GetHealthySegment(ctx, 1001))
	})
}

func TestAllocNewGrowingSegment_ManifestPath(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: meta.NewTestSchema()})
	segmentManager := NewSegmentManager(mt)

	t.Run("StorageV3 segment has manifest path", func(t *testing.T) {
		segment, err := segmentManager.AllocNewGrowingSegment(ctx, AllocNewGrowingSegmentRequest{
			CollectionID:         1,
			PartitionID:          100,
			SegmentID:            1000,
			ChannelName:          "c1",
			StorageVersion:       storage.StorageV3,
			IsCreatedByStreaming: true,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, segment.ManifestPath)

		// Verify the manifest path can be unmarshalled
		basePath, ver, unmarshalErr := packed.UnmarshalManifestPath(segment.ManifestPath)
		assert.NoError(t, unmarshalErr)
		assert.NotEmpty(t, basePath)
		assert.Equal(t, packed.ManifestEarliest, ver)
	})

	t.Run("StorageV2 segment has no manifest path", func(t *testing.T) {
		segment, err := segmentManager.AllocNewGrowingSegment(ctx, AllocNewGrowingSegmentRequest{
			CollectionID:         1,
			PartitionID:          100,
			SegmentID:            1001,
			ChannelName:          "c2",
			StorageVersion:       storage.StorageV2,
			IsCreatedByStreaming: true,
		})
		assert.NoError(t, err)
		assert.Empty(t, segment.ManifestPath)
	})
}
