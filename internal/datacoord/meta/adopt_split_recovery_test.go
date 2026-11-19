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

package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type adoptionPrefixedWalkKV struct{ kv.MetaKv }

func (m *adoptionPrefixedWalkKV) WalkWithPrefix(ctx context.Context, prefix string, n int, fn func([]byte, []byte) error) error {
	return m.MetaKv.WalkWithPrefix(ctx, prefix, n, func(key, value []byte) error { return fn([]byte("/by-dev/meta/"+string(key)), value) })
}

func TestCommittedAdoptionRecoveryPrecedesProjection(t *testing.T) {
	ctx := context.Background()
	original, err := NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, original.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: "s", SplitTarget: "t", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, NumOfRows: 1, Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 1}}}}, TransformStartAfterTimetick: 50})))
	concrete := original.Catalog.(*catalogkv.Catalog)
	concrete.MetaKv = &adoptionPrefixedWalkKV{MetaKv: concrete.MetaKv}
	catalog := original.Catalog.(model.SplitAdoptionCatalog)
	record, err := catalog.PrepareSplitAdoption(ctx, &model.SplitAdoption{TaskID: 7, CollectionID: 100, Source: "s", Moves: []model.SplitAdoptionMove{{SegmentID: 10, PartitionID: 1, Target: "t", TransformStart: 100, SourceTransformStart: 50}}})
	require.NoError(t, err)
	reopen := func() *Meta {
		return BootMetaWithLoader(t, original.Catalog, 100, func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader { return NoDomainMetas })
	}
	before := reopen()
	require.Equal(t, "s", before.GetSegment(ctx, 10).GetInsertChannel(), "prepared batches are invisible during real MetaStore recovery")
	view := &viewpb.DataViewOfCollection{CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, ShardSetEpoch: 3, Shards: []*viewpb.DataViewOfShard{{Vchannel: "t", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 1, SegmentIds: []int64{10}, SegmentManifestVersions: []int64{0}, SegmentTransformStartAfterTimeticks: []uint64{100}}}}}}
	require.NoError(t, catalog.CommitSplitAdoption(ctx, record, view))
	// No canonical SegmentInfo write and no task-state advancement has occurred.
	// The actual startup loader must install effective ownership before callers
	// can invoke the DataView projector or start metadata GC/scheduling.
	recovered := reopen()
	segment := recovered.GetSegment(ctx, 10)
	require.Equal(t, "t", segment.GetInsertChannel())
	require.EqualValues(t, 100, segment.GetTransformStartAfterTimetick())
	projection, err := recovered.LoadableProjection(ctx, 100, view)
	require.NoError(t, err)
	require.Len(t, projection, 1)
	require.Equal(t, "t", projection[0].VChannel)
	require.NoError(t, catalog.SaveSplitAdoptionState(ctx, record, "Cleaning"))
	recovered = reopen()
	require.Equal(t, "t", recovered.GetSegment(ctx, 10).GetInsertChannel(), "interrupted cleaning replays the same committed ownership")
}
