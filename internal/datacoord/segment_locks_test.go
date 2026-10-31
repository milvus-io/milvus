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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// gatedCatalog holds a catalog write of a gated segment until the test opens
// the gate, and counts the segment writes it passes on.
type gatedCatalog struct {
	metastore.DataCoordCatalog
	mu      sync.Mutex
	gates   map[int64]chan struct{}
	entered chan int64
	writes  atomic.Int64
}

func newGatedCatalog(inner metastore.DataCoordCatalog) *gatedCatalog {
	return &gatedCatalog{
		DataCoordCatalog: inner,
		gates:            make(map[int64]chan struct{}),
		entered:          make(chan int64, 16),
	}
}

func (c *gatedCatalog) gate(segmentID int64) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	gate := make(chan struct{})
	c.gates[segmentID] = gate
	return gate
}

func (c *gatedCatalog) pass(segments []*datapb.SegmentInfo) {
	c.writes.Inc()
	for _, segment := range segments {
		c.mu.Lock()
		gate := c.gates[segment.GetID()]
		c.mu.Unlock()
		if gate != nil {
			c.entered <- segment.GetID()
			<-gate
		}
	}
}

func (c *gatedCatalog) AlterSegments(ctx context.Context, segments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	c.pass(segments)
	return c.DataCoordCatalog.AlterSegments(ctx, segments, binlogs...)
}

func (c *gatedCatalog) SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error {
	c.pass(segments)
	return c.DataCoordCatalog.SaveDroppedSegmentsInBatch(ctx, segments)
}

func (c *gatedCatalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	var segments []*datapb.SegmentInfo
	for _, action := range actions {
		if entry, ok := action.Entry.(metastore.SegmentEntry); ok {
			segments = append(segments, entry.Segment)
		}
	}
	c.pass(segments)
	return c.DataCoordCatalog.Update(ctx, actions...)
}

func newSegmentLockTestMeta(t *testing.T, segments ...*datapb.SegmentInfo) (*meta, *gatedCatalog) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	for _, segment := range segments {
		require.NoError(t, m.AddSegment(context.Background(), NewSegmentInfo(segment)))
	}
	catalog := newGatedCatalog(m.catalog)
	m.catalog = catalog
	return m, catalog
}

func flushedTestSegment(id, partitionID int64) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:            id,
		CollectionID:  100,
		PartitionID:   partitionID,
		InsertChannel: "ch-0",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, id*100)},
		NumOfRows:     1,
	}
}

func waitDone(t *testing.T, done <-chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err, what)
	case <-time.After(5 * time.Second):
		require.FailNow(t, what+" did not finish")
	}
}

// A write held in its catalog call holds only its segment: a write of another
// segment and every reader finish meanwhile.
func TestSegmentWriteDoesNotBlockOtherSegmentsOrReaders(t *testing.T) {
	ctx := context.Background()
	m, catalog := newSegmentLockTestMeta(t, flushedTestSegment(1, 10), flushedTestSegment(2, 10))
	gate := catalog.gate(1)

	held := make(chan error, 1)
	go func() { held <- m.UpdateSegmentsInfo(ctx, SetSegmentIsInvisible(1, true)) }()
	require.Equal(t, int64(1), <-catalog.entered)

	other := make(chan error, 1)
	go func() { other <- m.UpdateSegmentsInfo(ctx, SetSegmentIsInvisible(2, true)) }()
	waitDone(t, other, "a write of another segment")
	require.True(t, m.GetSegment(ctx, 2).GetIsInvisible())

	readers := make(chan error, 1)
	go func() {
		m.GetSegment(ctx, 1)
		m.SelectSegments(ctx, WithCollection(100))
		m.GetSegmentsByChannel("ch-0")
		m.GetQuotaInfo()
		readers <- nil
	}()
	waitDone(t, readers, "the readers")
	require.False(t, m.GetSegment(ctx, 1).GetIsInvisible(), "a write is published only after its catalog write")

	close(gate)
	waitDone(t, held, "the held write")
	require.True(t, m.GetSegment(ctx, 1).GetIsInvisible())
}

// Read-modify-writes of one segment run one at a time: none is lost.
func TestConcurrentWritesOfOneSegmentAreSerialized(t *testing.T) {
	ctx := context.Background()
	m, _ := newSegmentLockTestMeta(t, flushedTestSegment(1, 10))
	increment := func(pack *updateSegmentPack) bool {
		segment := pack.Get(1)
		segment.MaxRowNum++
		return true
	}
	const writers = 32
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, m.UpdateSegmentsInfo(ctx, increment))
		}()
	}
	wg.Wait()
	require.EqualValues(t, writers, m.GetSegment(ctx, 1).GetMaxRowNum())

	stored, err := m.catalog.ListSegments(ctx, 100)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.EqualValues(t, writers, stored[0].GetMaxRowNum())
}

// An operator that reads a segment it does not write locks it too. Reaching
// segments in ID order, the write waits for the segment's writer, without a
// discarded run, and builds on what that writer published.
func TestUpdateSegmentsInfoWaitsForThePeekedSegment(t *testing.T) {
	ctx := context.Background()
	m, catalog := newSegmentLockTestMeta(t, flushedTestSegment(1, 10), flushedTestSegment(2, 10))

	unlock := m.lockSegments(1)
	var runs atomic.Int64
	derive := func(pack *updateSegmentPack) bool {
		runs.Inc()
		source := pack.Peek(1)
		segment := pack.Get(2)
		segment.MaxRowNum = source.GetMaxRowNum() + 1
		return true
	}
	done := make(chan error, 1)
	writesBefore := catalog.writes.Load()
	go func() { done <- m.UpdateSegmentsInfo(ctx, derive) }()

	select {
	case err := <-done:
		require.FailNow(t, "the write finished while another writer held a segment it reads", "err: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// The holder writes segment 1, then releases it.
	source := m.GetSegment(ctx, 1).Clone()
	source.MaxRowNum = 10
	m.segments.SetSegment(1, source)
	unlock()

	waitDone(t, done, "the write")
	require.EqualValues(t, 11, m.GetSegment(ctx, 2).GetMaxRowNum())
	require.EqualValues(t, 1, runs.Load())
	require.Equal(t, writesBefore+1, catalog.writes.Load())
}

// A write that holds a segment and then reaches a smaller one another writer
// holds does not wait for it there: the run is discarded without any I/O and
// runs again once it holds both, building on what the other writer published.
func TestUpdateSegmentsInfoRerunsWhenItReachesASmallerHeldSegment(t *testing.T) {
	ctx := context.Background()
	m, catalog := newSegmentLockTestMeta(t, flushedTestSegment(1, 10), flushedTestSegment(2, 10))

	unlock := m.lockSegments(1)
	var runs, absent atomic.Int64
	derive := func(pack *updateSegmentPack) bool {
		runs.Inc()
		segment := pack.Get(2)
		source := pack.Peek(1)
		if source == nil {
			// The run that cannot take segment 1's lock reads it as absent.
			absent.Inc()
			return false
		}
		segment.MaxRowNum = source.GetMaxRowNum() + 1
		return true
	}
	done := make(chan error, 1)
	writesBefore := catalog.writes.Load()
	go func() { done <- m.UpdateSegmentsInfo(ctx, derive) }()

	select {
	case err := <-done:
		require.FailNow(t, "the write finished while another writer held a segment it reads", "err: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.Equal(t, writesBefore, catalog.writes.Load(), "a run that missed a lock must not write")
	// Waiting for segment 1 in ID order, the write gave segment 2 up.
	unlock2 := m.lockSegments(2)
	unlock2()

	source := m.GetSegment(ctx, 1).Clone()
	source.MaxRowNum = 10
	m.segments.SetSegment(1, source)
	unlock()

	waitDone(t, done, "the write")
	require.EqualValues(t, 11, m.GetSegment(ctx, 2).GetMaxRowNum())
	require.EqualValues(t, 2, runs.Load())
	require.EqualValues(t, 1, absent.Load())
	require.Equal(t, writesBefore+1, catalog.writes.Load())
}

// Two writes that reach each other's segments in opposite orders do not
// deadlock: one that cannot take a lock gives up its locks before it waits.
func TestCrossingSegmentWritesDoNotDeadlock(t *testing.T) {
	ctx := context.Background()
	m, _ := newSegmentLockTestMeta(t, flushedTestSegment(1, 10), flushedTestSegment(2, 10))
	touch := func(first, second int64) UpdateOperator {
		return func(pack *updateSegmentPack) bool {
			pack.Get(first).MaxRowNum++
			// Give the other write time to take its first segment.
			time.Sleep(time.Millisecond)
			// A run that cannot take the second segment's lock is discarded.
			if segment := pack.Get(second); segment != nil {
				segment.MaxRowNum++
			}
			return true
		}
	}
	const rounds = 50
	done := make(chan error, 2*rounds)
	for i := 0; i < rounds; i++ {
		go func() { done <- m.UpdateSegmentsInfo(ctx, touch(1, 2)) }()
		go func() { done <- m.UpdateSegmentsInfo(ctx, touch(2, 1)) }()
	}
	for i := 0; i < 2*rounds; i++ {
		waitDone(t, done, "a crossing write")
	}
	require.EqualValues(t, 2*rounds, m.GetSegment(ctx, 1).GetMaxRowNum())
	require.EqualValues(t, 2*rounds, m.GetSegment(ctx, 2).GetMaxRowNum())
}

// A partition drop that selected the partition before a compaction published
// its output still drops the output: the compaction holds its inputs until
// the output is published, and the drop selects again once it holds them.
func TestDropPartitionDropsTheOutputOfACompactionCommittedMeanwhile(t *testing.T) {
	ctx := context.Background()
	m, catalog := newSegmentLockTestMeta(t, flushedTestSegment(1, 10), flushedTestSegment(2, 10))
	gate := catalog.gate(1)

	compacted := make(chan error, 1)
	go func() {
		_, err := m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
			InputSegments: []int64{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-0",
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}, &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{
				SegmentID:  3,
				NumOfRows:  2,
				InsertLogs: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 300)},
			}},
		})
		compacted <- err
	}()
	// The compaction holds segments 1, 2 and 3 in its catalog write.
	require.Equal(t, int64(1), <-catalog.entered)

	dropped := make(chan error, 1)
	go func() { dropped <- m.DropSegmentsOfPartition(ctx, []int64{10}) }()
	// Let the drop select segments 1 and 2 and wait for their locks.
	time.Sleep(200 * time.Millisecond)
	require.Nil(t, m.GetSegment(ctx, 3), "the output is published only after the compaction's catalog write")

	close(gate)
	waitDone(t, compacted, "the compaction")
	waitDone(t, dropped, "the partition drop")

	for _, id := range []int64{1, 2, 3} {
		require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, id).GetState(), "segment %d", id)
	}
	stored, err := m.catalog.ListSegments(ctx, 100)
	require.NoError(t, err)
	for _, segment := range stored {
		require.Equal(t, commonpb.SegmentState_Dropped, segment.GetState(), "stored segment %d", segment.GetID())
	}
}

// A compaction that reaches its inputs after a partition drop retired them
// fails and publishes nothing.
func TestCompactionFailsAfterAPartitionDropRetiredItsInputs(t *testing.T) {
	ctx := context.Background()
	m, catalog := newSegmentLockTestMeta(t, flushedTestSegment(1, 10), flushedTestSegment(2, 10))
	gate := catalog.gate(1)

	dropped := make(chan error, 1)
	go func() { dropped <- m.DropSegmentsOfPartition(ctx, []int64{10}) }()
	require.Equal(t, int64(1), <-catalog.entered)

	compacted := make(chan error, 1)
	go func() {
		_, err := m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
			InputSegments: []int64{1, 2},
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-0",
			Schema:        &schemapb.CollectionSchema{Version: 1},
		}, &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{SegmentID: 3, NumOfRows: 2, InsertLogs: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 300)}}},
		})
		compacted <- err
	}()
	time.Sleep(100 * time.Millisecond)
	close(gate)
	waitDone(t, dropped, "the partition drop")
	select {
	case err := <-compacted:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the compaction did not finish")
	}
	require.Nil(t, m.GetSegment(ctx, 3))
}

// A discarded run leaves the operators' inputs as they were: a SaveBinlogPaths
// request listing a field once per sync merges into the same binlogs when the
// write runs again.
func TestRerunOfSaveBinlogPathsOperatorsKeepsTheBinlogs(t *testing.T) {
	ctx := context.Background()
	m, _ := newSegmentLockTestMeta(t, flushedTestSegment(1, 10), &datapb.SegmentInfo{
		ID:            2,
		CollectionID:  100,
		PartitionID:   10,
		InsertChannel: "ch-0",
		State:         commonpb.SegmentState_Growing,
	})

	unlock := m.lockSegments(1)
	binlogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 1001), getFieldBinlogIDs(101, 1002)}
	done := make(chan error, 1)
	go func() {
		done <- m.UpdateSegmentsInfo(ctx,
			UpdateBinlogsFromSaveBinlogPathsOperator(2, binlogs, nil, nil, nil),
			// Reaching the smaller, held segment 1 discards the first run.
			func(pack *updateSegmentPack) bool { return pack.Peek(1) != nil })
	}()
	time.Sleep(100 * time.Millisecond)
	unlock()
	waitDone(t, done, "the write")

	segment := m.GetSegment(ctx, 2)
	require.Len(t, segment.GetBinlogs(), 1)
	logIDs := make([]int64, 0)
	for _, binlog := range segment.GetBinlogs()[0].GetBinlogs() {
		logIDs = append(logIDs, binlog.GetLogID())
	}
	require.Equal(t, []int64{1001, 1002}, logIDs)
	require.Len(t, binlogs[0].GetBinlogs(), 1, "the request is left unchanged")
}
