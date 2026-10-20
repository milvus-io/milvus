// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"path"

	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestManifestGCRejectsForeignArtifactDirectory(t *testing.T) {
	ctx := context.Background()
	m, dm, err := newMemoryMetaWithSnapshot(t)
	require.NoError(t, err)
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	foreign := path.Join(root, "unrelated", "index.bin")
	require.NoError(t, cm.Write(ctx, foreign, []byte("keep")))
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Dropped,
		StorageVersion: storage.StorageV3, ManifestHasIndex: true,
		ManifestPath: packed.MarshalManifestPath(path.Join(root, "insert_log/1/2/3"), 1),
	})
	require.NoError(t, m.AddSegment(ctx, segment))
	entry := packed.ManifestIndexInfo{
		IndexID: 4, BuildID: 5, IndexName: "idx", IndexType: "HNSW",
		Path: path.Dir(foreign), IndexFileKeys: []string{"index.bin"},
	}
	reader := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{entry}, nil).Build()
	defer reader.UnPatch()
	gc := newGarbageCollector(m, newMockHandler(), gcTestDomainOption(dm, GcOption{cli: cm}))
	gc.recycleDroppedSegment(ctx, newGCManifestReadIO(t), segment.GetID(), segment)
	require.NotNil(t, m.GetSegment(ctx, segment.GetID()))
	content, err := cm.Read(ctx, foreign)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), content)
}
