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

package delegator

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// TestIDFOracleReady covers the state-derived readiness predicate: only
// loaded-but-unactivated entries in the serving target gate readiness;
// segments with nothing loaded (progressive backfill) never do.
func TestIDFOracleReady(t *testing.T) {
	o := NewIDFOracle("test-channel", nil).(*idfOracle)

	// No target snapshot at all: trivially ready.
	assert.True(t, o.Ready())

	// Target contains segment 1 and 2; the oracle has loaded stats only for
	// segment 1 and has not activated them yet -> not ready. Segment 2 has
	// nothing loadable (e.g. backfill has not produced its stats): it must NOT
	// count against readiness.
	o.next.SetSnapshot(&snapshot{
		dist: []SnapshotItem{{
			NodeID: 1,
			Segments: []SegmentEntry{
				{NodeID: 1, SegmentID: 1, TargetVersion: 1},
				{NodeID: 1, SegmentID: 2, TargetVersion: 1},
			},
		}},
		targetVersion: 1,
	})
	seg1 := &sealedBm25Stats{activate: atomic.NewBool(false), segmentID: 1}
	o.sealed.Insert(1, seg1)
	assert.False(t, o.Ready())

	// Activation flips readiness.
	seg1.activate.Store(true)
	assert.True(t, o.Ready())

	// An unactivated entry OUTSIDE the serving target (deactivated leftover)
	// does not gate readiness.
	o.sealed.Insert(3, &sealedBm25Stats{activate: atomic.NewBool(false), segmentID: 3})
	assert.True(t, o.Ready())
}

func newReadyGateTestDelegator(t *testing.T, version int32) *shardDelegator {
	t.Helper()
	paramtable.Init()
	schema := newFunctionRuntimeTestSchemaWithVersion(version)
	sd := &shardDelegator{
		collectionID:               1000,
		vchannelName:               "test-channel",
		collection:                 segments.NewCollectionWithoutSegcoreForTest(1000, schema),
		lifetime:                   lifetime.NewLifetime(lifetime.Working),
		distribution:               NewDistribution("test-channel", NewChannelQueryView(nil, nil, nil, initialTargetVersion)),
		tsCond:                     syncutil.NewContextCond(&sync.Mutex{}),
		latestRequiredMVCCTimeTick: atomic.NewUint64(0),
	}
	return sd
}

// TestTryPublishReadySchemaPendingBM25Gate: an in-flight BM25 stats load blocks
// publishing; clearing the pending mark lets the next attempt publish.
func TestTryPublishReadySchemaPendingBM25Gate(t *testing.T) {
	sd := newReadyGateTestDelegator(t, 1)

	sd.addPendingBM25Loads([]int64{10})
	require.NoError(t, sd.tryPublishReadySchema(context.Background()))
	assert.Nil(t, sd.schemaReady.load(), "pending BM25 load must gate the publish")
	assert.EqualValues(t, -1, sd.ReadySchemaVersion())

	sd.removePendingBM25Loads([]int64{10})
	require.NoError(t, sd.tryPublishReadySchema(context.Background()))
	snap := sd.schemaReady.load()
	require.NotNil(t, snap)
	assert.EqualValues(t, 1, snap.version)
	assert.EqualValues(t, 1, sd.ReadySchemaVersion())
}

// TestTryPublishReadySchemaOracleGate: a loaded-but-unactivated sealed stats
// entry in the serving target blocks publishing until activated.
func TestTryPublishReadySchemaOracleGate(t *testing.T) {
	sd := newReadyGateTestDelegator(t, 1)

	o := NewIDFOracle("test-channel", nil).(*idfOracle)
	o.next.SetSnapshot(&snapshot{
		dist: []SnapshotItem{{
			NodeID:   1,
			Segments: []SegmentEntry{{NodeID: 1, SegmentID: 1, TargetVersion: 1}},
		}},
		targetVersion: 1,
	})
	seg := &sealedBm25Stats{activate: atomic.NewBool(false), segmentID: 1}
	o.sealed.Insert(1, seg)
	sd.publishIDFOracle(o)

	require.NoError(t, sd.tryPublishReadySchema(context.Background()))
	assert.Nil(t, sd.schemaReady.load(), "unactivated sealed stats in target must gate the publish")

	seg.activate.Store(true)
	require.NoError(t, sd.tryPublishReadySchema(context.Background()))
	require.NotNil(t, sd.schemaReady.load())
}

// TestLoadBM25StatsRequiresOracleWhenSchemaHasBM25: a nil idfOracle is only
// legitimate for collections without BM25 functions. When the (possibly
// load-advanced) schema does carry one, silently skipping the stats load would
// under-represent the segment in BM25 relevance forever, so the load must fail
// and be retried by querycoord.
func TestLoadBM25StatsRequiresOracleWhenSchemaHasBM25(t *testing.T) {
	paramtable.Init()
	bm25Schema := newFunctionRuntimeTestSchemaWithVersion(1, newBM25FunctionSchema())
	sd := &shardDelegator{
		collectionID: 1000,
		vchannelName: "test-channel",
		collection:   segments.NewCollectionWithoutSegcoreForTest(1000, bm25Schema),
	}
	err := sd.loadBM25Stats(context.Background(), []*querypb.SegmentLoadInfo{{SegmentID: 1}}, &querypb.LoadSegmentsRequest{CollectionID: 1000})
	require.Error(t, err)
	require.ErrorContains(t, err, "BM25 oracle")

	// Without BM25 functions a nil oracle is fine: nothing to load.
	plainSchema := newFunctionRuntimeTestSchemaWithVersion(1)
	sd.collection = segments.NewCollectionWithoutSegcoreForTest(1000, plainSchema)
	require.NoError(t, sd.loadBM25Stats(context.Background(), []*querypb.SegmentLoadInfo{{SegmentID: 1}}, &querypb.LoadSegmentsRequest{CollectionID: 1000}))
}

// TestUpdateSchemaNoOpBranchGatedWhilePending is the regression shape of the
// original premature-publish race: a load advanced the collection schema and is
// still downloading BM25 stats (pending) when the (now no-op) UpdateSchema WAL
// event arrives. The no-op branch's publish attempt must be gated; the load's
// completion publishes.
func TestUpdateSchemaNoOpBranchGatedWhilePending(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)
	// Collection already advanced to v2 by the load path.
	advanced := newFunctionRuntimeTestSchemaWithVersion(2)
	workerManager := cluster.NewMockManager(t)
	// The no-op branch's worker fan-out reaches the local worker even with an
	// empty distribution ("don't skip empty").
	worker := cluster.NewMockWorker(t)
	worker.EXPECT().UpdateSchema(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
	workerManager.EXPECT().GetWorker(mock.Anything, mock.Anything).Return(worker, nil).Maybe()
	sd := &shardDelegator{
		collectionID:               1000,
		vchannelName:               "test-channel",
		collection:                 segments.NewCollectionWithoutSegcoreForTest(1000, advanced),
		lifetime:                   lifetime.NewLifetime(lifetime.Working),
		distribution:               NewDistribution("test-channel", NewChannelQueryView(nil, nil, nil, initialTargetVersion)),
		workerManager:              workerManager,
		schemaBarrierTs:            100,
		deleteBuffer:               deletebuffer.NewListDeleteBuffer[*deletebuffer.Item](0, 0, []string{"1", "test-channel"}),
		tsCond:                     syncutil.NewContextCond(&sync.Mutex{}),
		latestRequiredMVCCTimeTick: atomic.NewUint64(0),
	}
	defer sd.Close()

	// The in-flight load's stats are pending.
	sd.addPendingBM25Loads([]int64{7})

	// Stale event (v1 <= current v2): no-op branch runs and attempts a publish,
	// which must be gated by the pending load.
	stale := newFunctionRuntimeTestSchemaWithVersion(1)
	require.NoError(t, sd.UpdateSchema(context.Background(), stale, 200))
	assert.Nil(t, sd.schemaReady.load(), "no-op UpdateSchema must not publish while a stats load is pending")

	// Load completes: pending cleared, deferred attempt publishes v2.
	sd.removePendingBM25Loads([]int64{7})
	require.NoError(t, sd.tryPublishReadySchema(context.Background()))
	snap := sd.schemaReady.load()
	require.NotNil(t, snap)
	assert.EqualValues(t, 2, snap.version)
}

// pendingBM25Loads is a ref count, not a set: two overlapping LoadSegments
// calls for the same segment (querycoord retry racing an abandoned-but-running
// RPC) must not un-mark each other — publishing with a still-in-flight BM25
// stats load would expose searches to half-loaded IDF stats.
func TestPendingBM25LoadsRefCount(t *testing.T) {
	sd := &shardDelegator{}
	assert.False(t, sd.hasPendingBM25Loads())

	// two overlapping loads mark the same segment
	sd.addPendingBM25Loads([]int64{1, 2})
	sd.addPendingBM25Loads([]int64{1})

	// the fast-failing retry returns first: its mark alone must not clear
	sd.removePendingBM25Loads([]int64{1})
	assert.True(t, sd.hasPendingBM25Loads())

	// both loads returned: gate opens
	sd.removePendingBM25Loads([]int64{1, 2})
	assert.False(t, sd.hasPendingBM25Loads())

	// removing an unmarked segment is a no-op, not a panic or negative count
	sd.removePendingBM25Loads([]int64{99})
	assert.False(t, sd.hasPendingBM25Loads())
}

// Every ready publish must fire the registered notifier: a pure schema publish
// changes no segment distribution, and without the notification querycoord's
// unchanged-distribution fast path would never see the new ReadySchemaVersion,
// stalling the DDL readiness handshake indefinitely. No-op attempts must not
// re-fire it.
func TestReadyPublishFiresNotifier(t *testing.T) {
	sd := newReadyGateTestDelegator(t, 1)
	fired := 0
	sd.SetReadyPublishNotifier(func() { fired++ })

	require.NoError(t, sd.tryPublishReadySchema(context.Background()))
	require.NotNil(t, sd.schemaReady.load())
	assert.Equal(t, 1, fired, "publish must fire the notifier")

	// Same version again: fast-path no-op, no publish, no notification.
	require.NoError(t, sd.tryPublishReadySchema(context.Background()))
	assert.Equal(t, 1, fired, "a no-op attempt must not re-fire the notifier")
}
