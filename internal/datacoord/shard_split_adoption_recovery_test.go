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

package datacoord

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/segment"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type adoptionUncertainKV struct {
	kv.MetaKv
	committed bool
	reads     int
	once      sync.Once
	entered   chan struct{}
	resume    chan struct{}
}

func (k *adoptionUncertainKV) MultiSaveAndRemove(ctx context.Context, values map[string]string, removals []string, preds ...predicates.Predicate) error {
	if err := k.MetaKv.MultiSaveAndRemove(ctx, values, removals, preds...); err != nil {
		return err
	}
	for key, value := range values {
		if strings.Contains(key, "/split-adoption/") && strings.Contains(value, `"State":"Committed"`) {
			k.committed = true
			return merr.WrapErrServiceUnavailableMsg("commit response lost")
		}
	}
	return nil
}

func (k *adoptionUncertainKV) Load(ctx context.Context, key string) (string, error) {
	if k.committed && strings.Contains(key, "/split-adoption/") {
		k.reads++
		if k.reads <= 2 {
			return "", merr.WrapErrServiceUnavailableMsg("readback temporarily unavailable")
		}
		k.once.Do(func() { close(k.entered); <-k.resume })
	}
	return k.MetaKv.Load(ctx, key)
}

func TestAdoptionUncertainReadbackKeepsProjectorBarrier(t *testing.T) {
	previous := streaming.WAL()
	streaming.SetupNoopWALForTest()
	t.Cleanup(func() { streaming.SetWALForTest(previous) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	store, _ := newSplitTestStore(t)
	_, err = store.Record(ctx, splitTestRecord(7))
	require.NoError(t, err)
	require.NoError(t, store.MarkSourceAcked(ctx, 7, 100, splitTestSource, nil, nil, false))
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskRewriteDone))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: splitTestSource, SplitTarget: splitTestTarget1, Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, NumOfRows: 1, TransformStartAfterTimetick: 50})))
	catalog := mt.Catalog.(*kvdatacoord.Catalog)
	dv, err := dataview.RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	require.NoError(t, err)
	_, err = dv.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 100, VChannels: []string{splitTestSource}, TransformStarts: map[string]uint64{splitTestSource: 50}})
	require.NoError(t, err)
	view, commit, _, err := dv.PrepareFlush(ctx, dataview.FlushDataViewEvent{CollectionID: 100, Segments: []dataview.LoadableSegment{{SegmentID: 10, VChannel: splitTestSource, PartitionID: 1, TransformStartAfterTimetick: 50, RowNum: 1}}})
	require.NoError(t, err)
	require.NoError(t, catalog.SaveDataView(ctx, view))
	commit()
	_, err = dv.DeclareShards(ctx, dataview.DeclareShardsDataViewEvent{CollectionID: 100, VChannels: []string{splitTestTarget1, splitTestTarget2}, TransformStarts: map[string]uint64{splitTestTarget1: 100, splitTestTarget2: 100}})
	require.NoError(t, err)
	fault := &adoptionUncertainKV{MetaKv: catalog.MetaKv, entered: make(chan struct{}), resume: make(chan struct{})}
	catalog.MetaKv = fault
	server := &Server{meta: mt, splitStore: store, dataViewManager: dv}
	adopted := make(chan error, 1)
	go func() { adopted <- server.AdoptShardSplit(ctx, 7) }()
	select {
	case <-fault.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("adoption did not reach readback barrier")
	}
	projected := make(chan error, 1)
	go func() {
		_, err := dv.RecomputeNow(context.Background(), 100, func(context.Context, int64, *viewpb.DataViewOfCollection) ([]dataview.LoadableSegment, error) {
			segment := mt.GetSegment(context.Background(), 10)
			return []dataview.LoadableSegment{{SegmentID: 10, VChannel: segment.GetInsertChannel(), PartitionID: 1, TransformStartAfterTimetick: segment.GetTransformStartAfterTimetick(), RowNum: 1}}, nil
		})
		projected <- err
	}()
	select {
	case <-projected:
		t.Fatal("projector crossed unresolved ownership commit")
	case <-time.After(50 * time.Millisecond):
	}
	// A canceled initiating request is not evidence that its commit was absent.
	cancel()
	close(fault.resume)
	if err := <-adopted; err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}
	require.NoError(t, <-projected)
	require.Equal(t, splitTestTarget1, mt.GetSegment(context.Background(), 10).GetInsertChannel())
	require.GreaterOrEqual(t, fault.reads, 3)
}

func TestDropCollectionAfterCommittedAdoptionPersistsEveryOwnerBeforeRemovingRoot(t *testing.T) {
	ctx := context.Background()
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	store, err := split.NewStore(ctx, mt.Catalog)
	require.NoError(t, err)
	_, err = store.Record(ctx, splitTestRecord(7))
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: splitTestSource, SplitTarget: splitTestTarget1, Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, NumOfRows: 1, TransformStartAfterTimetick: 50})))
	catalog := mt.Catalog.(model.SplitAdoptionCatalog)
	record, err := catalog.PrepareSplitAdoption(ctx, &model.SplitAdoption{TaskID: 7, CollectionID: 100, Source: splitTestSource, Moves: []model.SplitAdoptionMove{{SegmentID: 10, PartitionID: 1, Target: splitTestTarget1, TransformStart: 100, SourceTransformStart: 50}}})
	require.NoError(t, err)
	view := &viewpb.DataViewOfCollection{CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, ShardSetEpoch: 3}
	require.NoError(t, mt.AdoptSplit(ctx, record, view)) // durable committed root, canonical row still S
	segmentManager := segment.NewMockManager(t)
	inspector := compaction.NewMockCompactionInspector(t)
	for _, channel := range []string{splitTestSource, splitTestTarget1} {
		inspector.EXPECT().RemoveTasksByChannel(channel).Return().Once()
	}
	dv := dataview.NewManager(mt.Catalog.(dataview.Catalog), nil)
	server := &Server{meta: mt, splitStore: store, dataViewManager: dv, segmentManager: segmentManager, compactionInspector: inspector}
	failure := merr.WrapErrServiceUnavailableMsg("crash after drop owners/task, before DataView cleanup")
	patch := mockey.Mock(mockey.GetMethod(dv, "OnDropCollection")).Return(nil, failure).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, server.DropCollectionDataView(ctx, 100), failure)
	patch.UnPatch()
	require.Empty(t, store.List())
	recovered := meta.BootMetaWithLoader(t, mt.Catalog, 100, func(metastore.DataCoordCatalog, storage.ChunkManager) meta.DomainMetaLoader {
		return meta.NoDomainMetas
	})
	row := recovered.GetSegment(ctx, 10)
	require.Equal(t, commonpb.SegmentState_Dropped, row.GetState())
	require.Equal(t, splitTestTarget1, row.GetInsertChannel())
	restartedStore, err := split.NewStore(ctx, mt.Catalog)
	require.NoError(t, err)
	restarted := &Server{meta: recovered, splitStore: restartedStore, dataViewManager: dataview.NewManager(mt.Catalog.(dataview.Catalog), nil)}
	require.NoError(t, restarted.DropCollectionDataView(ctx, 100))
	require.Equal(t, commonpb.SegmentState_Dropped, recovered.GetSegment(ctx, 10).GetState())
	roots, err := mt.Catalog.(model.SplitAdoptionCatalog).LoadSplitAdoption(ctx, 100, 7)
	require.NoError(t, err)
	require.Nil(t, roots)
}
