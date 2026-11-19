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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSplitAdoptionReservationReleasesCompactionInputs(t *testing.T) {
	ctx := context.Background()
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	for _, id := range []int64{10, 20, 30} {
		segment := flushedTestSegment(id, 1)
		if id == 30 {
			segment.InsertChannel = "other-channel"
		} else {
			segment.SplitTarget = "t"
		}
		require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(segment)))
	}
	release, err := m.ProtectSplitAdoptionInputs([]split.AdoptionMove{
		{SegmentID: 10, PartitionID: 1, Target: "t", TransformStart: 100},
		{SegmentID: 20, PartitionID: 1, Target: "t", TransformStart: 100},
	})
	require.NoError(t, err)
	release = sync.OnceFunc(release)
	defer release()
	for _, id := range []int64{10, 20} {
		exist, canDo := m.CheckAndSetSegmentsCompacting(ctx, []int64{id})
		require.True(t, exist)
		require.False(t, canDo, "a protected adoption input must not enter compaction")
		require.False(t, m.GetSegment(ctx, id).IsCompacting)
	}
	exist, canDo := m.CheckAndSetSegmentsCompacting(ctx, []int64{30})
	require.True(t, exist)
	require.True(t, canDo, "an unrelated segment must remain available")
	m.SetSegmentsCompacting(ctx, []int64{30}, false)

	// A completed or failed prepare/commit attempt releases the reservation so
	// the next legitimate compaction may replace its former inputs.
	release()
	exist, canDo = m.CheckAndSetSegmentsCompacting(ctx, []int64{10, 20})
	require.True(t, exist)
	require.True(t, canDo)
	for _, id := range []int64{10, 20} {
		require.True(t, m.GetSegment(ctx, id).IsCompacting)
	}
	m.SetSegmentsCompacting(ctx, []int64{10, 20}, false)
}

func TestRecoveredCompactionGroupBlocksSplitAdoptionReservation(t *testing.T) {
	ctx := context.Background()
	original, err := NewMemoryMeta(t)
	require.NoError(t, err)
	key := Params.DataCoordCfg.SegmentChangeEnabled.Key
	old := Params.DataCoordCfg.SegmentChangeEnabled.GetValue()
	require.NoError(t, Params.Save(key, "true"))
	t.Cleanup(func() { require.NoError(t, Params.Save(key, old)) })
	schema := NewTestSchema()
	original.AddCollection(&CollectionInfo{ID: 100, Schema: schema})
	require.NoError(t, original.AddSegment(ctx, NewSegmentInfo(flushedTestSegment(10, 1))))
	exist, canDo := original.CheckAndSetSegmentsCompacting(ctx, []int64{10})
	require.True(t, exist)
	require.True(t, canDo)
	_, err = original.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
		PlanID: 7, CollectionID: 100, PartitionID: 1, Channel: "ch-0",
		Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10}, Schema: schema,
	}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
		SegmentID: 11, NumOfRows: 1, InsertLogs: flushedTestSegment(11, 1).GetBinlogs(),
	}}})
	require.NoError(t, err)
	require.Equal(t, model.SegmentChangeStateStaged, original.GetSegmentChangeGroup(ctx, 100, 7).State)
	require.True(t, original.GetSegment(ctx, 10).IsCompacting)

	// Exercise the real catalog/startup loader, including reconstruction of the
	// durable parent index. The reservation flag itself is deliberately transient.
	concrete := original.Catalog.(*catalogkv.Catalog)
	concrete.MetaKv = &adoptionPrefixedWalkKV{MetaKv: concrete.MetaKv}
	recovered := BootMetaWithLoader(t, original.Catalog, 100, func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader { return NoDomainMetas })
	require.False(t, recovered.GetSegment(ctx, 10).IsCompacting)
	require.True(t, recovered.HasReferencedSegment(ctx, 100, 10))
	unlock, err := recovered.ProtectSplitAdoptionInputs([]split.AdoptionMove{{SegmentID: 10, PartitionID: 1, Target: "t", TransformStart: 100}})
	if unlock != nil {
		unlock()
	}
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	exist, canDo = recovered.CheckAndSetSegmentsCompacting(ctx, []int64{10})
	require.True(t, exist)
	require.False(t, canDo, "a second compaction must respect the same recovered parent claim")
	require.Equal(t, "ch-0", recovered.GetSegment(ctx, 10).GetInsertChannel())
	require.True(t, recovered.GetSegment(ctx, 11).GetIsInvisible())
}

// splitPublicationStore gates the actual MetaStore implementation, rather than
// substituting its atomicity. The captured read is the manifest writer's input
// snapshot; both publication gates belong to the first adoption cache install.
type splitPublicationStore struct {
	metacache.MetaStore
	mu              sync.Mutex
	captureRead     bool
	readCaptured    chan struct{}
	releaseRead     chan struct{}
	insideWrite     chan struct{}
	releaseWrite    chan struct{}
	published       chan struct{}
	releasePublish  chan struct{}
	publicationOnce sync.Once
}

func (s *splitPublicationStore) GetSegment(id int64) (*datapb.SegmentInfo, bool) {
	segment, ok := s.MetaStore.GetSegment(id)
	s.mu.Lock()
	capture := s.captureRead && id == 10
	if capture {
		s.captureRead = false
	}
	s.mu.Unlock()
	if capture {
		close(s.readCaptured)
		<-s.releaseRead
	}
	return segment, ok
}

func (s *splitPublicationStore) ApplySegments(puts []*datapb.SegmentInfo, removes []int64, hook func()) {
	adopting := false
	for _, segment := range puts {
		adopting = adopting || segment.GetInsertChannel() == "t"
	}
	if !adopting {
		s.MetaStore.ApplySegments(puts, removes, hook)
		return
	}
	gated := false
	s.publicationOnce.Do(func() { gated = true })
	if !gated {
		s.MetaStore.ApplySegments(puts, removes, hook)
		return
	}
	s.MetaStore.ApplySegments(puts, removes, func() {
		if hook != nil {
			hook()
		}
		close(s.insideWrite)
		<-s.releaseWrite
	})
	close(s.published)
	<-s.releasePublish
}

func waitSplitSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestSplitAdoptionAtomicPublicationAndManifestWriter(t *testing.T) {
	ctx := context.Background()
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	for _, id := range []int64{10, 20} {
		segment := flushedTestSegment(id, 1)
		segment.InsertChannel, segment.SplitTarget = "s", "t"
		segment.TransformStartAfterTimetick = 50
		segment.StorageVersion = storage.StorageV3
		segment.ManifestPath = packed.MarshalManifestPath("/base", 1)
		require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(segment)))
	}
	moves := []split.AdoptionMove{{SegmentID: 10, PartitionID: 1, Target: "t", TransformStart: 100}, {SegmentID: 20, PartitionID: 1, Target: "t", TransformStart: 100}}
	record, err := m.PrepareSplitAdoption(ctx, &datapb.SplitShardTask{TaskId: 7, CollectionId: 100, SourceVchannel: "s"}, moves)
	require.NoError(t, err)
	view := &viewpb.DataViewOfCollection{CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, ShardSetEpoch: 3, Shards: []*viewpb.DataViewOfShard{{Vchannel: "t", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 1, SegmentIds: []int64{10, 20}, SegmentManifestVersions: []int64{1, 1}, SegmentTransformStartAfterTimeticks: []uint64{100, 100}}}}}}
	store := &splitPublicationStore{
		MetaStore: m.Segments.store, captureRead: true,
		readCaptured: make(chan struct{}), releaseRead: make(chan struct{}),
		insideWrite: make(chan struct{}), releaseWrite: make(chan struct{}),
		published: make(chan struct{}), releasePublish: make(chan struct{}),
	}
	m.Segments.store = store
	openRead := sync.OnceFunc(func() { close(store.releaseRead) })
	openWrite := sync.OnceFunc(func() { close(store.releaseWrite) })
	openPublish := sync.OnceFunc(func() { close(store.releasePublish) })
	t.Cleanup(func() { openRead(); openWrite(); openPublish() })
	manifestDone := make(chan error, 1)
	go func() {
		manifestDone <- m.CommitSegmentManifest(ctx, SegmentManifestCommit{
			SegmentID: 10, ExpectedManifest: packed.MarshalManifestPath("/base", 1),
			Mutation: ManifestMutation{Type: ManifestMutationNoop, ManifestPath: packed.MarshalManifestPath("/base", 2)},
		})
	}()
	waitSplitSignal(t, store.readCaptured, "the manifest writer's pre-adoption snapshot")
	adoptionDone := make(chan error, 1)
	go func() { adoptionDone <- m.AdoptSplit(ctx, record, view) }()
	waitSplitSignal(t, store.insideWrite, "the adoption's real atomic MetaStore write")
	readDone := make(chan map[int64]*SegmentInfo, 1)
	go func() { readDone <- m.Segments.GetSegmentsByIDs([]int64{10, 20}) }()
	select {
	case <-readDone:
		t.Fatal("a snapshot reader escaped the real atomic cache write")
	case <-time.After(100 * time.Millisecond):
	}
	openWrite()
	waitSplitSignal(t, store.published, "cache publication before adoption releases its writer locks")
	select {
	case segments := <-readDone:
		for _, id := range []int64{10, 20} {
			require.Equal(t, "t", segments[id].GetInsertChannel(), "one cache snapshot must contain the complete owner migration")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the snapshot reader did not finish after cache publication")
	}
	// The writer continues from its captured SOURCE snapshot while adoption
	// still owns both writer locks. Its final publication must rebase onto the
	// adopted TARGET record, preserving ownership and the transform lower bound.
	openRead()
	select {
	case err := <-manifestDone:
		t.Fatalf("manifest writer escaped the adoption writer lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	openPublish()
	waitDone(t, adoptionDone, "adoption")
	waitDone(t, manifestDone, "manifest publication")
	require.NoError(t, m.FinishSplitAdoption(ctx, record))
	concrete := m.Catalog.(*catalogkv.Catalog)
	concrete.MetaKv = &adoptionPrefixedWalkKV{MetaKv: concrete.MetaKv}
	recovered := BootMetaWithLoader(t, m.Catalog, 100, func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader { return NoDomainMetas })
	for _, metadata := range []*Meta{m, recovered} {
		for _, id := range []int64{10, 20} {
			segment := metadata.GetSegment(ctx, id)
			require.Equal(t, "t", segment.GetInsertChannel())
			require.Empty(t, segment.GetSplitTarget())
			require.EqualValues(t, 100, segment.GetTransformStartAfterTimetick())
			require.Equal(t, "t", segment.GetStartPosition().GetChannelName())
			require.Equal(t, "t", segment.GetDmlPosition().GetChannelName())
		}
		require.Equal(t, packed.MarshalManifestPath("/base", 2), metadata.GetSegment(ctx, 10).GetManifestPath())
		require.Equal(t, packed.MarshalManifestPath("/base", 1), metadata.GetSegment(ctx, 20).GetManifestPath())
	}
}
