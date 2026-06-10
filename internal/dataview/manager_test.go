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

package dataview

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type fakeDataViewCatalog struct {
	metastore.DataCoordCatalog

	mu                  sync.Mutex
	views               []*viewpb.DataViewOfCollection
	saveErrOnce         error
	blockCollection     int64
	saveStarted         chan struct{}
	saveBlock           chan struct{}
	blockDropCollection int64
	dropStarted         chan struct{}
	dropBlock           chan struct{}
}

func (c *fakeDataViewCatalog) SaveDataView(ctx context.Context, dataView *viewpb.DataViewOfCollection) error {
	if dataView.GetCollectionId() == c.blockCollection && c.saveBlock != nil {
		if c.saveStarted != nil {
			select {
			case <-c.saveStarted:
			default:
				close(c.saveStarted)
			}
		}
		<-c.saveBlock
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.saveErrOnce != nil {
		err := c.saveErrOnce
		c.saveErrOnce = nil
		return err
	}
	c.views = append(c.views, proto.Clone(dataView).(*viewpb.DataViewOfCollection))
	return nil
}

func (c *fakeDataViewCatalog) ListDataViews(ctx context.Context, collectionID int64) ([]*viewpb.DataViewOfCollection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	views := make([]*viewpb.DataViewOfCollection, 0)
	for _, view := range c.views {
		if view.GetCollectionId() == collectionID {
			views = append(views, proto.Clone(view).(*viewpb.DataViewOfCollection))
		}
	}
	return views, nil
}

func (c *fakeDataViewCatalog) DropDataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) error {
	if collectionID == c.blockDropCollection && c.dropBlock != nil {
		if c.dropStarted != nil {
			select {
			case <-c.dropStarted:
			default:
				close(c.dropStarted)
			}
		}
		<-c.dropBlock
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	views := c.views[:0]
	for _, view := range c.views {
		if view.GetCollectionId() == collectionID && compareDataVersion(view.GetDataVersion(), dataVersion) == 0 {
			continue
		}
		views = append(views, view)
	}
	c.views = views
	return nil
}

func (c *fakeDataViewCatalog) DropDataViews(ctx context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	views := c.views[:0]
	for _, view := range c.views {
		if view.GetCollectionId() != collectionID {
			views = append(views, view)
		}
	}
	c.views = views
	return nil
}

type fakeDataViewSegmentStore struct {
	segments map[int64]*Segment
}

func (s *fakeDataViewSegmentStore) GetSegment(ctx context.Context, segID int64) *Segment {
	return s.segments[segID]
}

func (s *fakeDataViewSegmentStore) SelectSegments(ctx context.Context, collectionID int64) []*Segment {
	segments := make([]*Segment, 0, len(s.segments))
	for _, segment := range s.segments {
		if segment.GetCollectionID() == collectionID {
			segments = append(segments, segment)
		}
	}
	return segments
}

func newTestDataViewManager() (*dataViewManager, *fakeDataViewCatalog, *fakeDataViewSegmentStore) {
	catalog := &fakeDataViewCatalog{}
	store := &fakeDataViewSegmentStore{segments: make(map[int64]*Segment)}
	return NewManager(catalog, store).(*dataViewManager), catalog, store
}

func noErrorVersion(_ *viewpb.DataVersion, err error) error {
	return err
}

func requireDataVersion(t *testing.T, version *viewpb.DataVersion, streamingVersion, compactVersion int64) {
	require.NotNil(t, version)
	require.Equal(t, streamingVersion, version.GetStreamingVersion())
	require.Equal(t, compactVersion, version.GetCompactVersion())
}

func newDataViewTestSegment(collectionID, partitionID, segmentID int64, channel string, dmlTs uint64) *Segment {
	return &Segment{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channel,
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: channel,
			Timestamp:   dmlTs,
		},
	}
}

func findDataViewShard(view *viewpb.DataViewOfCollection, vchannel string) (*viewpb.DataViewOfShard, bool) {
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard, true
		}
	}
	return nil, false
}

func TestDataViewManagerOnFlushCreatesVisibleView(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)

	require.Len(t, catalog.views, 1)
	require.Equal(t, int64(1), catalog.views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[0].GetDataVersion().GetCompactVersion())
	require.Zero(t, catalog.views[0].GetShards()[0].GetDeleteApplyStartAfterTimetick())

	view, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, uint64(1000), view.GetShards()[0].GetDeleteApplyStartAfterTimetick())
	require.Equal(t, []int64{100}, view.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerOnFlushSkipsNonLoadableSegments(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[100].State = commonpb.SegmentState_Sealed
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	store.segments[101].IsInvisible = true
	store.segments[102] = newDataViewTestSegment(1, 10, 102, "ch-1", 1200)
	store.segments[102].IsImporting = true
	store.segments[103] = newDataViewTestSegment(1, 10, 103, "ch-1", 1300)
	store.segments[103].Level = datapb.SegmentLevel_L0
	store.segments[104] = newDataViewTestSegment(1, 10, 104, "ch-1", 1400)
	store.segments[104].State = commonpb.SegmentState_Dropped

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101, 102, 103, 104}})
	require.NoError(t, err)
	require.Nil(t, version)

	require.Empty(t, catalog.views)
	view, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, view)
}

func TestDataViewManagerOnFlushExposesVisibleTimeTick(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	_, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)

	timeticks, err := manager.ShardTimeTicks(ctx, []int64{1})
	require.NoError(t, err)
	require.Len(t, timeticks, 1)
	require.Equal(t, "ch-1", timeticks[0].GetVchannel())
	require.Equal(t, uint64(1000), timeticks[0].GetDeleteApplyStartAfterTimetick())
}

func TestDataViewManagerFlushTemporaryThenSortHandoff(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	temp := newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	temp.IsInvisible = true
	store.segments[100] = temp

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:         1,
		SegmentIDs:           []int64{100},
		TemporaryUnavailable: true,
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)
	require.Equal(t, int64(1), catalog.views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[0].GetDataVersion().GetCompactVersion())
	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, visible)

	temp.State = commonpb.SegmentState_Dropped
	final := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	final.CompactionFrom = []int64{100}
	store.segments[101] = final

	version, err = manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{100},
		CompactTo:    []int64{101},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 1)
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetCompactVersion())

	visible, err = manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{101}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
	require.Equal(t, uint64(1100), visible.GetShards()[0].GetDeleteApplyStartAfterTimetick())
}

func TestDataViewManagerImportAndCopySegmentCompleteAdvanceStreamingVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)

	require.NoError(t, noErrorVersion(manager.OnImport(ctx, ImportDataViewEvent{
		CollectionID: 1,
		SegmentIDs:   []int64{100},
	})))
	require.NoError(t, noErrorVersion(manager.OnCopySegmentComplete(ctx, CopySegmentCompleteDataViewEvent{
		CollectionID: 1,
		SegmentIDs:   []int64{101},
	})))

	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[0].GetDataVersion().GetCompactVersion())
	require.Equal(t, int64(2), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[1].GetDataVersion().GetCompactVersion())
	view, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, []int64{100, 101}, view.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerTemporaryFlushKeepsPreviousVisibleView(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	temp := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	temp.IsInvisible = true
	store.segments[101] = temp
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:         1,
		SegmentIDs:           []int64{101},
		TemporaryUnavailable: true,
	})))

	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(2), catalog.views[1].GetDataVersion().GetStreamingVersion())
	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, int64(1), visible.GetDataVersion().GetStreamingVersion())
	require.Equal(t, []int64{100}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerShardTimeTicksUseLatestVisibleView(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	temp := newDataViewTestSegment(1, 10, 101, "ch-1", 800)
	temp.IsInvisible = true
	store.segments[101] = temp
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:         1,
		SegmentIDs:           []int64{101},
		TemporaryUnavailable: true,
	})))

	timeticks, err := manager.ShardTimeTicks(ctx, []int64{1})
	require.NoError(t, err)
	require.Len(t, timeticks, 1)
	require.Equal(t, "ch-1", timeticks[0].GetVchannel())
	require.Equal(t, uint64(1000), timeticks[0].GetDeleteApplyStartAfterTimetick())
}

func TestDataViewManagerSnapshotReturnsLatestVisibleClone(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	temp := newDataViewTestSegment(1, 10, 101, "ch-1", 800)
	temp.IsInvisible = true
	store.segments[101] = temp
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:         1,
		SegmentIDs:           []int64{101},
		TemporaryUnavailable: true,
	})))

	views, err := manager.Snapshot(ctx, []int64{1})
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, int64(1), views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, []int64{100}, views[0].GetShards()[0].GetPartitions()[0].GetSegmentIds())
	require.Equal(t, uint64(1000), views[0].GetShards()[0].GetDeleteApplyStartAfterTimetick())

	views[0].Shards[0].Partitions[0].SegmentIds[0] = 999
	views, err = manager.Snapshot(ctx, []int64{1})
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, []int64{100}, views[0].GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerCompactPendingOutputIsNoopUntilVisible(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	output := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	output.IsInvisible = true
	output.CompactionFrom = []int64{100}
	store.segments[101] = output
	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{100},
		CompactTo:    []int64{101},
	})))

	require.Len(t, catalog.views, 1)
	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{100}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())

	store.segments[100].State = commonpb.SegmentState_Dropped
	output.IsInvisible = false
	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{100},
		CompactTo:    []int64{101},
	})))

	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetCompactVersion())
	visible, err = manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{101}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerL0CompactRefreshesDeleteTimetickWithoutVersionBump(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 900)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101}})))

	store.segments[101].DmlPosition.Timestamp = 800
	version, err := manager.OnL0Compact(ctx, L0CompactDataViewEvent{CollectionID: 1})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)

	view, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, uint64(800), view.GetShards()[0].GetDeleteApplyStartAfterTimetick())
	require.Equal(t, int64(1), view.GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), view.GetDataVersion().GetCompactVersion())
}

func TestDataViewManagerDropPartitionAdvancesCompactVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 11, 101, "ch-1", 900)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101}})))

	require.NoError(t, noErrorVersion(manager.OnDropPartition(ctx, DropPartitionDataViewEvent{CollectionID: 1, PartitionIDs: []int64{10}})))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetCompactVersion())

	view, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, int64(11), view.GetShards()[0].GetPartitions()[0].GetPartitionId())
	require.Equal(t, []int64{101}, view.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerTruncateAdvancesCompactVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	store.segments[200] = newDataViewTestSegment(1, 10, 200, "ch-2", 900)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101, 200}})))

	require.NoError(t, noErrorVersion(manager.OnTruncate(ctx, TruncateDataViewEvent{
		CollectionID: 1,
		VChannel:     "ch-1",
		FlushTs:      1000,
	})))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetCompactVersion())

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	shard1, ok := findDataViewShard(visible, "ch-1")
	require.True(t, ok)
	require.Equal(t, []int64{101}, shard1.GetPartitions()[0].GetSegmentIds())
	shard2, ok := findDataViewShard(visible, "ch-2")
	require.True(t, ok)
	require.Equal(t, []int64{200}, shard2.GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerTruncateUsesCommitTimestamp(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[100].CommitTimestamp = 1200
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101}})))

	require.NoError(t, noErrorVersion(manager.OnTruncate(ctx, TruncateDataViewEvent{
		CollectionID: 1,
		VChannel:     "ch-1",
		FlushTs:      1100,
	})))

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{100}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerDuplicateEventIsNoop(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	version, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)
}

func TestDataViewManagerCompactSameSegmentIDIsNoop(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{100},
		CompactTo:    []int64{100},
	})))
	require.Len(t, catalog.views, 1)
}

func TestDataViewManagerExternalRefreshClassifiesActualMembershipChange(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	require.NoError(t, noErrorVersion(manager.OnExternalRefresh(ctx, ExternalRefreshDataViewEvent{
		CollectionID: 1,
		AddSegments:  []int64{101},
		DropSegments: []int64{999},
	})))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(2), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[1].GetDataVersion().GetCompactVersion())

	store.segments[102] = newDataViewTestSegment(1, 10, 102, "ch-1", 1200)
	require.NoError(t, noErrorVersion(manager.OnExternalRefresh(ctx, ExternalRefreshDataViewEvent{
		CollectionID: 1,
		AddSegments:  []int64{102},
		DropSegments: []int64{100},
	})))
	require.Len(t, catalog.views, 3)
	require.Equal(t, int64(2), catalog.views[2].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[2].GetDataVersion().GetCompactVersion())
}

func TestDataViewManagerRecoverPersistsEmptyViewWhenLatestHadMembership(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = append(catalog.views, &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 3},
		Shards: []*viewpb.DataViewOfShard{
			{
				Vchannel: "ch-1",
				Partitions: []*viewpb.DataViewOfPartition{
					{PartitionId: 10, SegmentIds: []int64{100}},
				},
			},
		},
	})

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(2), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(4), catalog.views[1].GetDataVersion().GetCompactVersion())
	require.Empty(t, catalog.views[1].GetShards())

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Empty(t, visible.GetShards())
}

func TestDataViewManagerRecoverCompactsFailedPublicationIntoOneView(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	catalog.saveErrOnce = errors.New("dataview persistence failed")

	_, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.Error(t, err)
	require.Empty(t, catalog.views)

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 1)
	require.Equal(t, int64(1), catalog.views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[0].GetDataVersion().GetCompactVersion())
	require.Equal(t, []int64{100, 101}, catalog.views[0].GetShards()[0].GetPartitions()[0].GetSegmentIds())

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{100, 101}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerRecoverDoesNotReaddHistoricallyRemovedSegments(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 11, 101, "ch-1", 1100)
	catalog.views = append(catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{100}},
						{PartitionId: 11, SegmentIds: []int64{101}},
					},
				},
			},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 11, SegmentIds: []int64{101}},
					},
				},
			},
		},
	)

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 2)

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, int64(1), visible.GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), visible.GetDataVersion().GetCompactVersion())
	require.Equal(t, []int64{101}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerRecoverDoesNotReaddTruncatedSegments(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	catalog.views = append(catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{100, 101}},
					},
				},
			},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{101}},
					},
				},
			},
		},
	)

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 2)

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, int64(1), visible.GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), visible.GetDataVersion().GetCompactVersion())
	require.Equal(t, []int64{101}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerRecoverRefreshesDeleteTimetickWithoutVersionBump(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{}
	store := &fakeDataViewSegmentStore{segments: make(map[int64]*Segment)}
	manager := NewManager(catalog, store).(*dataViewManager)
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 500)
	catalog.views = append(catalog.views, &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		Shards: []*viewpb.DataViewOfShard{
			{
				Vchannel: "ch-1",
				Partitions: []*viewpb.DataViewOfPartition{
					{PartitionId: 10, SegmentIds: []int64{100}},
				},
			},
		},
	})

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 1)
	timeticks, err := manager.ShardTimeTicks(ctx, []int64{1})
	require.NoError(t, err)
	require.Len(t, timeticks, 1)
	require.Equal(t, "ch-1", timeticks[0].GetVchannel())
	require.Equal(t, uint64(500), timeticks[0].GetDeleteApplyStartAfterTimetick())

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, int64(1), visible.GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), visible.GetDataVersion().GetCompactVersion())
	require.Equal(t, uint64(500), visible.GetShards()[0].GetDeleteApplyStartAfterTimetick())
}

func TestDataViewManagerRecoverAddsNeverPublishedStreamingSegment(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	catalog.views = append(catalog.views, &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		Shards: []*viewpb.DataViewOfShard{
			{
				Vchannel: "ch-1",
				Partitions: []*viewpb.DataViewOfPartition{
					{PartitionId: 10, SegmentIds: []int64{100}},
				},
			},
		},
	})

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(2), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[1].GetDataVersion().GetCompactVersion())

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{100, 101}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerRecoverStreamingAdvanceWinsOverCompactHandoff(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	old := newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	old.State = commonpb.SegmentState_Dropped
	compactOutput := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	compactOutput.CompactionFrom = []int64{100}
	streamingOutput := newDataViewTestSegment(1, 10, 200, "ch-1", 1200)
	store.segments[100] = old
	store.segments[101] = compactOutput
	store.segments[200] = streamingOutput
	catalog.views = append(catalog.views, &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		Shards: []*viewpb.DataViewOfShard{
			{
				Vchannel: "ch-1",
				Partitions: []*viewpb.DataViewOfPartition{
					{PartitionId: 10, SegmentIds: []int64{100}},
				},
			},
		},
	})

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(2), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[1].GetDataVersion().GetCompactVersion())
	require.Equal(t, []int64{101, 200}, catalog.views[1].GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerRecoverTreatsLineageAdditionOutsideCurrentViewAsStreaming(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	importInput := newDataViewTestSegment(1, 10, 200, "ch-1", 1050)
	importInput.State = commonpb.SegmentState_Dropped
	importOutput := newDataViewTestSegment(1, 10, 201, "ch-1", 1100)
	importOutput.CreatedByCompaction = true
	importOutput.CompactionFrom = []int64{200}
	store.segments[200] = importInput
	store.segments[201] = importOutput
	catalog.views = append(catalog.views, &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 2},
		Shards: []*viewpb.DataViewOfShard{
			{
				Vchannel: "ch-1",
				Partitions: []*viewpb.DataViewOfPartition{
					{PartitionId: 10, SegmentIds: []int64{100}},
				},
			},
		},
	})

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(2), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[1].GetDataVersion().GetCompactVersion())
	require.Equal(t, []int64{100, 201}, catalog.views[1].GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerRecoverKeepsTemporaryFlushResidentUnavailable(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	temp := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	temp.IsInvisible = true
	store.segments[101] = temp
	catalog.views = append(catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{100}},
					},
				},
			},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{100, 101}},
					},
				},
			},
		},
	)

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(2), manager.states[1].latestResident.GetDataVersion().GetStreamingVersion())

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, int64(1), visible.GetDataVersion().GetStreamingVersion())
	require.Equal(t, []int64{100}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())

	temp.State = commonpb.SegmentState_Dropped
	final := newDataViewTestSegment(1, 10, 102, "ch-1", 1200)
	final.CompactionFrom = []int64{101}
	store.segments[102] = final
	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{101},
		CompactTo:    []int64{102},
	})))
	require.Len(t, catalog.views, 3)
	require.Equal(t, int64(2), catalog.views[2].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[2].GetDataVersion().GetCompactVersion())
}

func TestDataViewManagerRecoverRetainsCompactionInputUntilOutputVisible(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	input := newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	input.State = commonpb.SegmentState_Dropped
	output := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	output.IsInvisible = true
	output.CompactionFrom = []int64{100}
	store.segments[100] = input
	store.segments[101] = output
	catalog.views = append(catalog.views, &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		Shards: []*viewpb.DataViewOfShard{
			{
				Vchannel: "ch-1",
				Partitions: []*viewpb.DataViewOfPartition{
					{PartitionId: 10, SegmentIds: []int64{100}},
				},
			},
		},
	})

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	require.Len(t, catalog.views, 1)
	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, int64(1), visible.GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), visible.GetDataVersion().GetCompactVersion())
	require.Equal(t, []int64{100}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())

	output.IsInvisible = false
	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{100},
		CompactTo:    []int64{101},
	})))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetCompactVersion())
	visible, err = manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{101}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerDropCollectionDropsStateAndCatalog(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	require.NoError(t, noErrorVersion(manager.OnDropCollection(ctx, 1)))
	require.Empty(t, catalog.views)

	visible, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, visible)
}

func TestDataViewManagerSegmentReferenceUsesRetainedViews(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = append(catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{100}},
					},
				},
			},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{101}},
					},
				},
			},
		},
	)

	referenced, err := manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, referenced)

	referenced, err = manager.IsSegmentReferenced(ctx, 1, 102)
	require.NoError(t, err)
	require.False(t, referenced)
}

func TestDataViewManagerGarbageCollectRetainsLatestAndProtectedViews(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = append(catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 2,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		},
	)

	require.NoError(t, manager.GarbageCollect(ctx, 1, []*viewpb.DataVersion{
		{StreamingVersion: 1, CompactVersion: 0},
	}, 1))

	views, err := catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Len(t, views, 2)
	require.Equal(t, int64(1), views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), views[0].GetDataVersion().GetCompactVersion())
	require.Equal(t, int64(2), views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), views[1].GetDataVersion().GetCompactVersion())

	views, err = catalog.ListDataViews(ctx, 2)
	require.NoError(t, err)
	require.Len(t, views, 1)
}

func TestDataViewManagerDoesNotBlockOtherCollections(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	catalog.blockCollection = 1
	catalog.saveStarted = make(chan struct{})
	catalog.saveBlock = make(chan struct{})
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[200] = newDataViewTestSegment(2, 20, 200, "ch-2", 2000)

	blockedErr := make(chan error, 1)
	go func() {
		_, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
		blockedErr <- err
	}()
	<-catalog.saveStarted

	otherErr := make(chan error, 1)
	go func() {
		_, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 2, SegmentIDs: []int64{200}})
		otherErr <- err
	}()
	select {
	case err := <-otherErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("DataView update for another collection was blocked")
	}

	close(catalog.saveBlock)
	require.NoError(t, <-blockedErr)
}

func TestDataViewManagerGarbageCollectDoesNotBlockOtherCollections(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	catalog.views = append(catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
		},
	)
	catalog.blockDropCollection = 1
	catalog.dropStarted = make(chan struct{})
	catalog.dropBlock = make(chan struct{})
	store.segments[200] = newDataViewTestSegment(2, 20, 200, "ch-2", 2000)

	blockedErr := make(chan error, 1)
	go func() {
		blockedErr <- manager.GarbageCollect(ctx, 1, nil, 1)
	}()
	<-catalog.dropStarted

	otherErr := make(chan error, 1)
	go func() {
		_, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 2, SegmentIDs: []int64{200}})
		otherErr <- err
	}()
	select {
	case err := <-otherErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("DataView update for another collection was blocked by collection GC")
	}

	close(catalog.dropBlock)
	require.NoError(t, <-blockedErr)
}
