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
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestPartitionStatsGCCadencePreservesMetaPause(t *testing.T) {
	base := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	var clock atomic.Pointer[time.Time]
	clock.Store(&base)
	clockPatch := mockey.Mock(time.Now).To(func() time.Time { return *clock.Load() }).Build()
	defer clockPatch.UnPatch()

	// Drive the real work callback with a controlled clock; unrelated GC passes
	// are stubbed so the assertions exercise the partition-stats object walk.
	metadataTicks := 0
	droppedPatch := mockey.Mock((*garbageCollector).recycleDroppedSegments).To(
		func(*garbageCollector, context.Context, <-chan gcCmd) { metadataTicks++ }).Build()
	defer droppedPatch.UnPatch()
	for _, fn := range []any{
		(*garbageCollector).recycleChannelCPMeta,
		(*garbageCollector).recycleUnusedIndexes,
		(*garbageCollector).recycleUnusedSegIndexes,
		(*garbageCollector).recycleUnusedAnalyzeFiles,
		(*garbageCollector).recycleUnusedTextIndexFiles,
		(*garbageCollector).recycleUnusedJSONIndexFiles,
		(*garbageCollector).recycleUnusedJSONStatsFiles,
	} {
		patch := mockey.Mock(fn).Return().Build()
		defer patch.UnPatch()
	}
	metaReady := make(chan struct{})
	metaTicks := make(chan time.Time)
	metaDone := make(chan struct{}, 1)
	snapshotPatch := mockey.Mock((*garbageCollector).recycleSnapshots).To(
		func(*garbageCollector, context.Context, <-chan gcCmd) { metaDone <- struct{}{} }).Build()
	defer snapshotPatch.UnPatch()
	lobInterval := Params.DataCoordCfg.GCLOBCheckInterval.GetAsDuration(time.Second)
	tickerPatch := mockey.Mock(time.NewTicker).When(func(interval time.Duration) bool {
		return interval == time.Hour || interval == 7*24*time.Hour || interval == lobInterval
	}).To(func(interval time.Duration) *time.Ticker {
		if interval == time.Hour {
			close(metaReady)
			return &time.Ticker{C: metaTicks}
		}
		return &time.Ticker{}
	}).Build()
	defer tickerPatch.UnPatch()
	controlPatch := mockey.Mock((*garbageCollector).startControlLoop).Return().Build()
	defer controlPatch.UnPatch()

	segments := NewSegmentsInfo()
	segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 200, InsertChannel: "channel", PartitionStatsVersion: 42,
	}))
	mt := &meta{segments: segments}
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root")
	walks := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, "root/part_stats/", true, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ bool, walk storage.ChunkObjectWalkFunc) error {
			walks++
			for _, file := range []string{"root/part_stats/100/200/channel/42", "root/part_stats/100/200/channel/43"} {
				walk(&storage.ChunkObjectInfo{FilePath: file, ModifyTime: base.Add(-time.Hour)})
			}
			return nil
		})
	var removed atomic.Int64
	cm.EXPECT().Remove(mock.Anything, "root/part_stats/100/200/channel/43").RunAndReturn(
		func(context.Context, string) error { removed.Add(1); return nil })
	gc := newGarbageCollector(mt, nil, GcOption{
		cli: cm, checkInterval: time.Hour, scanInterval: 7 * 24 * time.Hour, missingTolerance: time.Minute,
	})
	defer gc.close()
	gc.work(gc.ctx)
	<-metaReady
	tick := func(elapsed time.Duration) {
		now := base.Add(elapsed)
		clock.Store(&now)
		metaTicks <- now
		<-metaDone
	}

	tick(time.Hour)
	tick(2 * time.Hour)
	assert.Equal(t, 2, metadataTicks, "ordinary metadata GC keeps its existing cadence")
	assert.Zero(t, walks, "the full partition-stats prefix must not be walked every metadata tick")

	// The real meta worker still acknowledges pauses, and the collection
	// remains protected when the slow scan becomes due.
	pauseCtx, cancelPause := context.WithCancel(context.Background())
	defer cancelPause()
	paused := make(chan error, 1)
	go func() {
		paused <- gc.pause(gcCmd{
			ctx: pauseCtx, timeout: pauseCtx.Done(), collectionID: 100, ticket: "snapshot", duration: 30 * 24 * time.Hour,
		})
	}()
	require.Eventually(t, func() bool { return gc.collectionGCPaused(100) }, 10*time.Second, time.Millisecond)
	tick(gc.option.scanInterval)
	select {
	case err := <-paused:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the metadata worker did not acknowledge the pause")
	}
	assert.Equal(t, 1, walks)
	assert.Zero(t, removed.Load(), "a paused collection's orphan must be retained")

	gc.resume(gcCmd{collectionID: 100, ticket: "snapshot"})
	tick(gc.option.scanInterval + time.Hour)
	assert.Equal(t, 1, walks, "the next metadata tick must not repeat the prefix scan")
	tick(2 * gc.option.scanInterval)
	assert.Equal(t, 2, walks)
	assert.EqualValues(t, 1, removed.Load(), "only the unowned version is deleted after resume")
	assert.Equal(t, 5, metadataTicks)
}
