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
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQuotaCacheMatchesFullScan(t *testing.T) {
	paramtable.Init()
	setQuotaTestParam(t, &Params.QuotaConfig.CollectionStatsCacheEnabled, "true")
	store := metacache.NewMetaStore(nil)
	m := &Meta{MetaStore: store, Segments: NewSegmentsInfo(store)}
	random := rand.New(rand.NewSource(73))
	for step := int64(1); step <= 600; step++ {
		id := int64(1 + random.Intn(30))
		segment := BuildSegment(1+id%5, 1+id%3, id, "channel")
		segment.NumOfRows = int64(random.Intn(1000))
		segment.IsImporting = random.Intn(4) == 0
		segment.Stats = &datapb.Statistics{InsertBinlogSize: int64(random.Intn(500)), DeltaBinlogSize: 13, DeleteNumRows: 7, InsertBinlogCount: 1}
		segment.Level = datapb.SegmentLevel_L1
		if id%3 == 0 {
			segment.Level = datapb.SegmentLevel_L0
		}
		segment.State = commonpb.SegmentState_Flushed
		if id%7 == 0 {
			segment.State = commonpb.SegmentState_Dropped
		}
		m.Segments.SetSegment(id, segment)
		if step%3 == 0 {
			m.Segments.ModifyProto(id, func(p *datapb.SegmentInfo) { p.NumOfRows = step })
		}
		if step%5 == 0 {
			m.Segments.DropSegment(id)
		}
		require.Equal(t, m.getQuotaInfoByScan(), m.GetQuotaInfo(), "step %d", step)
		require.Equal(t, m.getAllCollectionNumRowsByScan(), m.GetAllCollectionNumRows(), "step %d", step)
	}
}

// TestQuotaCacheFollowsMetaWrites drives the production writers, which
// publish through the master MetaStore with segment writer locks
// and atomic collection publication rather than the SetSegment seed used above.
func TestQuotaCacheFollowsMetaWrites(t *testing.T) {
	paramtable.Init()
	setQuotaTestParam(t, &Params.QuotaConfig.CollectionStatsCacheEnabled, "true")
	ctx := context.Background()
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	check := func(step string) {
		t.Helper()
		require.Equal(t, m.getQuotaInfoByScan(), m.GetQuotaInfo(), step)
		require.Equal(t, m.getAllCollectionNumRowsByScan(), m.GetAllCollectionNumRows(), step)
	}
	// The first read turns tracking on; every later write must invalidate.
	check("empty")
	for id := int64(1); id <= 4; id++ {
		require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 1 + id%2, PartitionID: 10, InsertChannel: "channel",
			State: commonpb.SegmentState_Growing, NumOfRows: 10 * id,
			Stats: &datapb.Statistics{InsertBinlogSize: 100 * id},
		})))
		check("add " + strconv.FormatInt(id, 10))
	}
	require.NoError(t, m.UpdateSegmentsInfo(ctx,
		UpdateStatusOperator(1, commonpb.SegmentState_Flushed),
		UpdateStatusOperator(2, commonpb.SegmentState_Flushed),
		UpdateImportedRows(3, 777),
		UpdateIsImporting(4, true)))
	check("multi-segment update")
	m.Segments.ModifyProto(1, func(p *datapb.SegmentInfo) { p.NumOfRows = 999 })
	check("local row update")
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(2, commonpb.SegmentState_Dropped)))
	check("mark dropped")
	require.NoError(t, m.DropSegment(ctx, 2))
	check("drop")
}

type quotaFailureCatalog struct {
	metastore.DataCoordCatalog
	err error
}

func (c *quotaFailureCatalog) AlterSegments(context.Context, []*datapb.SegmentInfo, ...metastore.BinlogsIncrement) error {
	return c.err
}

func TestQuotaCacheFailedWriteAndDrop(t *testing.T) {
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, m.AddSegment(ctx, BuildSegment(1, 1, 1, "channel")))
	baseline := m.Segments.ReadQuotaSnapshot(true)
	m.Segments.AcknowledgeQuotaSnapshot(baseline)
	catalog := m.Catalog
	failure := errors.New("catalog write failed")
	m.Catalog = &quotaFailureCatalog{DataCoordCatalog: catalog, err: failure}
	require.ErrorIs(t, m.UpdateSegmentsInfo(ctx, UpdateImportedRows(1, 999)), failure)
	snapshot := m.Segments.ReadQuotaSnapshot(false)
	require.Empty(t, snapshot.Collections)
	require.Zero(t, snapshot.ScannedSegments)
	m.Catalog = catalog
	require.NoError(t, m.DropSegment(ctx, 1))
	snapshot = m.Segments.ReadQuotaSnapshot(false)
	require.Contains(t, snapshot.Collections, int64(1))
	require.False(t, snapshot.Collections[1].Present)
	require.Zero(t, snapshot.TotalBytes)
}

func TestQuotaCacheCollectionSourceSwitch(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	s := NewSegmentsInfo(store)
	s.SetSegment(1, BuildSegment(1, 1, 1, "channel"))
	s.AcknowledgeQuotaSnapshot(s.ReadQuotaSnapshot(true))
	source := metacache.NewCollections()
	store.SetCollectionSource(source)
	source.Put(&metacache.CollectionInfo{ID: 1, DatabaseName: "db"})
	changed := s.ReadQuotaSnapshot(false)
	require.Contains(t, changed.Collections, int64(1))
	s.AcknowledgeQuotaSnapshot(changed)
	source.Remove(1)
	changed = s.ReadQuotaSnapshot(false)
	require.Contains(t, changed.Collections, int64(1))
	require.True(t, changed.Collections[1].Present, "collection removal must retain remaining segment usage")
}

func TestQuotaCacheDirtyAndAcknowledgement(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	for _, id := range []int64{1, 2} {
		segment := BuildSegment(id, id, id, "channel")
		segment.NumOfRows = 10
		s.SetSegment(id, segment)
	}
	baseline := s.ReadQuotaSnapshot(true)
	require.Equal(t, 2, baseline.ScannedSegments)
	s.AcknowledgeQuotaSnapshot(baseline)
	require.Empty(t, s.ReadQuotaSnapshot(false).Collections)
	s.ModifyProto(1, func(p *datapb.SegmentInfo) { p.NumOfRows = 20 })
	first := s.ReadQuotaSnapshot(false)
	require.Equal(t, 1, first.ScannedSegments)
	require.Len(t, first.Collections, 1)
	require.EqualValues(t, 20, first.Collections[1].Rows)
	s.ModifyProto(1, func(p *datapb.SegmentInfo) { p.NumOfRows = 30 })
	s.AcknowledgeQuotaSnapshot(first) // Must not clear metadata waiting to be recomputed.
	second := s.ReadQuotaSnapshot(false)
	require.Equal(t, 1, second.ScannedSegments)
	require.EqualValues(t, 30, second.Collections[1].Rows)
	s.AcknowledgeQuotaSnapshot(first) // Nor a newer published value.
	require.Len(t, s.ReadQuotaSnapshot(false).Collections, 1)
	s.AcknowledgeQuotaSnapshot(second)
	require.Empty(t, s.ReadQuotaSnapshot(false).Collections)
	require.Zero(t, s.ReadQuotaSnapshot(true).ScannedSegments)
	s.DropSegment(1)
	deleted := s.ReadQuotaSnapshot(false)
	require.False(t, deleted.Collections[1].Present)
	require.False(t, deleted.Collections[1].HasRows)
	s.AcknowledgeQuotaSnapshot(deleted)
	require.Empty(t, s.ReadQuotaSnapshot(false).Collections)
	require.Len(t, s.ReadQuotaSnapshot(true).Collections, 1)
}

// A multi-segment mutation (a compaction replacing its input) is published
// atomically: a refresh counts exactly one side of it.
func TestQuotaCacheCompactionPublication(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	first := BuildSegment(1, 1, 1, "channel")
	first.State = commonpb.SegmentState_Flushed
	first.Stats = &datapb.Statistics{InsertBinlogSize: 100}
	s.SetSegment(1, first)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(2); i < 300; i++ {
			dropped := s.GetSegment(i - 1).Clone()
			dropped.State = commonpb.SegmentState_Dropped
			next := BuildSegment(1, 1, i, "channel")
			next.State = commonpb.SegmentState_Flushed
			next.Stats = &datapb.Statistics{InsertBinlogSize: 100}
			s.PublishSegments([]*SegmentInfo{dropped, next})
		}
	}()
	for i := 0; i < 300; i++ {
		require.EqualValues(t, 100, s.ReadQuotaSnapshot(true).TotalBytes)
	}
	wg.Wait()
	require.EqualValues(t, 100, s.ReadQuotaSnapshot(true).TotalBytes)
}

func TestQuotaCachePublishesWhileCollectionKeepsChanging(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	segment := BuildSegment(1, 1, 1, "channel")
	segment.NumOfRows = 10
	s.SetSegment(1, segment)
	var original func([]*SegmentInfo, uint64) *metricsinfo.CollectionQuotaSnapshot
	patch := mockey.Mock(computeCollectionQuota).To(func(segments []*SegmentInfo, generation uint64) *metricsinfo.CollectionQuotaSnapshot {
		// Every reduction observes a new write after its pointer snapshot was
		// captured. Reject-and-retry publication would never make progress.
		s.ModifyProto(1, func(p *datapb.SegmentInfo) { p.NumOfRows = segments[0].GetNumOfRows() + 1 })
		return original(segments, generation)
	}).Origin(&original).Build()
	defer patch.UnPatch()
	for expected := int64(10); expected < 20; expected++ {
		snapshot := s.ReadQuotaSnapshot(false)
		require.Equal(t, expected, snapshot.Collections[1].Rows)
		s.AcknowledgeQuotaSnapshot(snapshot)
		require.NotEmpty(t, s.quota.dirty)
	}
}

// A wholesale rewrite that moves a segment to another collection invalidates
// both collections.
func TestQuotaCacheSegmentChangesCollection(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	s.SetSegment(1, BuildSegment(1, 1, 1, "channel"))
	s.AcknowledgeQuotaSnapshot(s.ReadQuotaSnapshot(true))
	s.SetSegment(1, BuildSegment(2, 1, 1, "channel"))
	changed := s.ReadQuotaSnapshot(false)
	require.False(t, changed.Collections[1].Present)
	require.True(t, changed.Collections[2].Present)
}

func TestQuotaCacheRollbackRequiresNewBaseline(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	s.SetSegment(1, BuildSegment(1, 1, 1, "channel"))
	before := s.ReadQuotaSnapshot(false)
	s.AcknowledgeQuotaSnapshot(before)
	require.False(t, s.ReadQuotaSnapshot(false).Full)
	s.resetQuotaCache()
	s.DropSegment(1)
	s.SetSegment(2, BuildSegment(2, 2, 2, "channel"))
	after := s.ReadQuotaSnapshot(false)
	require.True(t, after.Full)
	require.NotContains(t, after.Collections, int64(1))
	require.Contains(t, after.Collections, int64(2))
	s.AcknowledgeQuotaSnapshot(before)
	require.True(t, s.ReadQuotaSnapshot(false).Full)
	s.AcknowledgeQuotaSnapshot(after)
	require.False(t, s.ReadQuotaSnapshot(false).Full)
	require.Empty(t, s.ReadQuotaSnapshot(false).Collections)
}

func BenchmarkQuotaCacheIdle(b *testing.B) {
	for _, collections := range []int{1000, 100000} {
		b.Run(strconv.Itoa(collections), func(b *testing.B) {
			s := NewSegmentsInfo(metacache.NewMetaStore(nil))
			for i := 1; i <= collections; i++ {
				id := int64(i)
				s.SetSegment(id, BuildSegment(id, id, id, "channel"))
			}
			baseline := s.ReadQuotaSnapshot(true)
			s.AcknowledgeQuotaSnapshot(baseline)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snapshot := s.ReadQuotaSnapshot(false)
				if snapshot.ScannedSegments != 0 {
					b.Fatal("idle snapshot scanned segments")
				}
			}
		})
	}
}

func setQuotaTestParam(t testing.TB, item *paramtable.ParamItem, value string) {
	t.Helper()
	old := item.GetValue()
	require.NoError(t, Params.Save(item.Key, value))
	t.Cleanup(func() { require.NoError(t, Params.Save(item.Key, old)) })
}
