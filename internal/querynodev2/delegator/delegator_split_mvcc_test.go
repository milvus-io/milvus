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
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// newTSafeTestDelegator builds a delegator with just the state the read-ts
// resolution and the tsafe wait touch. Its query view is not serviceable, like
// an un-adopted split child's.
func newTSafeTestDelegator(vchannel string, tsafe uint64) *shardDelegator {
	return &shardDelegator{
		vchannelName:               vchannel,
		lifetime:                   lifetime.NewLifetime(lifetime.Working),
		tsCond:                     syncutil.NewContextCond(&sync.Mutex{}),
		latestTsafe:                atomic.NewUint64(tsafe),
		latestRequiredMVCCTimeTick: atomic.NewUint64(0),
		catchingUpStreamingData:    atomic.NewBool(false),
		distribution:               NewDistribution(vchannel, NewChannelQueryView(nil, nil, nil, InitialTargetVersion)),
		children:                   make(map[string]ShardDelegator),
	}
}

// mockLocalMVCC answers GetLatestMVCCTimestampIfLocal from a per-vchannel table.
// A vchannel mapped in errs is not local (or its WAL is not ready).
func mockLocalMVCC(t *testing.T, mvcc map[string]uint64, errs map[string]error) (*mock_streaming.MockLocal, *mockey.Mocker) {
	local := mock_streaming.NewMockLocal(t)
	local.EXPECT().GetLatestMVCCTimestampIfLocal(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, vchannel string) (uint64, error) {
			if err, ok := errs[vchannel]; ok {
				return 0, err
			}
			ts, ok := mvcc[vchannel]
			if !ok {
				t.Errorf("unexpected MVCC lookup of vchannel %s", vchannel)
			}
			return ts, nil
		}).Maybe()
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().Local().Return(local).Maybe()
	return local, mockey.Mock(streaming.WAL).Return(wal).Build()
}

// consumeDeleteWhenRequired stands in for a child's pipeline: once a read has
// asked the child for a tsafe at or past the delete, the child consumes the
// delete and advances its tsafe past it. It gives up when ctx ends.
func consumeDeleteWhenRequired(ctx context.Context, child *shardDelegator, deleteTs, nextTSafe uint64) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if child.GetLatestRequiredMVCCTimeTick() >= deleteTs {
				child.UpdateTSafe(nextTSafe)
				return
			}
		}
	}
}

// After a shard-split fence the proxy writes the split key range to the target
// vchannels, which live on other pchannels, while the source vchannel receives
// no DML. A delete acknowledged at deleteTs on target v1 must be visible to a
// Strong read that starts afterwards, even when that read reaches the data
// through the source delegator fronting its in-process children.
//
// The source's own vchannel MVCC (its pchannel's last confirmed time tick) can
// sit below deleteTs, and child v1 has not consumed the delete yet (tsafe below
// deleteTs). The read must still resolve its MVCC timestamp at or past deleteTs:
// an MVCC below it leaves the deleted row visible.
func TestStrongReadThroughSplitSourceSeesChildDeletes(t *testing.T) {
	paramtable.Init()
	const (
		proxyGuaranteeTs = uint64(300) // proxy BeginTs, allocated after the delete was acked
		sourceMVCC       = uint64(50)  // source pchannel: no DML since the fence
		deleteTs         = uint64(120) // delete persisted on target v1
		child1TSafe      = uint64(100) // v1 has not consumed the delete yet
		child1NextTSafe  = uint64(130) // v1's tsafe once it consumes the delete
		child2MVCC       = uint64(60)
		child2TSafe      = uint64(200)
	)
	errPin := errors.New("stop after the read timestamp is resolved")
	cases := strongReadsOnV0(proxyGuaranteeTs)

	// Stage a: fenced, children un-adopted. Stage c: children adopted (and synced,
	// so serviceable) but the source still fronts them until it is released.
	stages := []struct {
		name    string
		adopted bool
	}{
		{"stage a un-adopted children", false},
		{"stage c adopted children", true},
	}

	for _, stage := range stages {
		for _, tc := range cases {
			t.Run(stage.name+"/"+tc.name, func(t *testing.T) {
				_, walMock := mockLocalMVCC(t, map[string]uint64{"v0": sourceMVCC, "v1": deleteTs, "v2": child2MVCC}, nil)
				defer walMock.UnPatch()
				pinMock := mockey.Mock((*shardDelegator).pinReadableSegments).Return(nil, nil, nil, int64(0), errPin).Build()
				defer pinMock.UnPatch()

				source := newTSafeTestDelegator("v0", 40)
				child1 := newTSafeTestDelegator("v1", child1TSafe)
				child2 := newTSafeTestDelegator("v2", child2TSafe)
				for _, child := range []*shardDelegator{child1, child2} {
					child.SetFrontingParent(source)
					if stage.adopted {
						child.MarkAdopted()
						view := NewChannelQueryView(nil, nil, nil, 1)
						view.loadedRatio.Store(1.0)
						view.syncedByCoord = true
						child.distribution = NewDistribution(child.vchannelName, view)
					}
					source.children[child.vchannelName] = child
				}

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go consumeDeleteWhenRequired(ctx, child1, deleteTs, child1NextTSafe)

				mvcc, err := tc.read(ctx, source)
				require.ErrorIs(t, err, errPin)
				assert.GreaterOrEqual(t, mvcc, deleteTs,
					"a Strong read through the split source served at MVCC %d, below the acknowledged delete at %d on child v1", mvcc, deleteTs)
			})
		}
	}
}

// A delegator that fronts no split child resolves the Strong speedup exactly as
// before: one MVCC lookup of its own vchannel, lowered only when below the
// guarantee, and never for non-Strong, iterator or explicit-MVCC reads.
func TestStrongSpeedupUnchangedWithoutSplit(t *testing.T) {
	const guaranteeTs = uint64(300)
	notLocal := errors.New("pchannel is not local")
	cases := []struct {
		name        string
		level       commonpb.ConsistencyLevel
		mvccTs      uint64
		iterator    bool
		walMVCC     uint64
		walErr      error
		want        uint64
		wantLookups int
	}{
		{name: "strong lowers to the vchannel MVCC", level: commonpb.ConsistencyLevel_Strong, walMVCC: 50, want: 50, wantLookups: 1},
		{name: "strong keeps a guarantee not above the MVCC", level: commonpb.ConsistencyLevel_Strong, walMVCC: 400, want: guaranteeTs, wantLookups: 1},
		{name: "strong keeps the guarantee when the MVCC is not local", level: commonpb.ConsistencyLevel_Strong, walErr: notLocal, want: guaranteeTs, wantLookups: 1},
		{name: "bounded", level: commonpb.ConsistencyLevel_Bounded, walMVCC: 50, want: guaranteeTs},
		{name: "eventually", level: commonpb.ConsistencyLevel_Eventually, walMVCC: 50, want: guaranteeTs},
		{name: "session", level: commonpb.ConsistencyLevel_Session, walMVCC: 50, want: guaranteeTs},
		{name: "strong iterator", level: commonpb.ConsistencyLevel_Strong, iterator: true, walMVCC: 50, want: guaranteeTs},
		{name: "strong with explicit mvcc", level: commonpb.ConsistencyLevel_Strong, mvccTs: 10, walMVCC: 50, want: guaranteeTs},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var errs map[string]error
			if tc.walErr != nil {
				errs = map[string]error{"v0": tc.walErr}
			}
			local, walMock := mockLocalMVCC(t, map[string]uint64{"v0": tc.walMVCC}, errs)
			defer walMock.UnPatch()

			sd := newTSafeTestDelegator("v0", 0)
			got := sd.speedupGuranteeTS(context.Background(), nil, tc.level, guaranteeTs, tc.mvccTs, tc.iterator)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, guaranteeTs, sd.GetLatestRequiredMVCCTimeTick())
			local.AssertNumberOfCalls(t, "GetLatestMVCCTimestampIfLocal", tc.wantLookups)
		})
	}
}

// When any vchannel of the family has no locally known MVCC (its pchannel is on
// another streaming node, or its assignment is not ready yet), the read cannot
// prove a lower timestamp covers that vchannel's writes, so it keeps the proxy's
// guarantee and waits for every child to reach it.
func TestFamilySpeedupKeepsTheGuaranteeWhenAChildMVCCIsUnknown(t *testing.T) {
	paramtable.Init()
	const guaranteeTs = uint64(300)
	_, walMock := mockLocalMVCC(t,
		map[string]uint64{"v0": 50, "v1": 120},
		map[string]error{"v2": errors.New("assignment not ready")})
	defer walMock.UnPatch()

	source := newTSafeTestDelegator("v0", 40)
	child1 := newTSafeTestDelegator("v1", 100)
	child2 := newTSafeTestDelegator("v2", 200)
	source.children["v1"] = child1
	source.children["v2"] = child2

	got := source.speedupGuranteeTS(context.Background(), source.frontingChildren(), commonpb.ConsistencyLevel_Strong, guaranteeTs, 0, false)
	assert.Equal(t, guaranteeTs, got)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go consumeDeleteWhenRequired(ctx, child1, guaranteeTs, 310)
	go consumeDeleteWhenRequired(ctx, child2, guaranteeTs, 320)
	tsafe, err := source.waitTSafe(ctx, got)
	require.NoError(t, err)
	assert.Equal(t, uint64(310), tsafe)
}

// A cascaded split fronts grandchildren through a child; their vchannels take
// writes of the same logical shard, so the family MVCC reaches them too.
func TestFamilySpeedupCoversCascadedChildren(t *testing.T) {
	_, walMock := mockLocalMVCC(t, map[string]uint64{"v0": 50, "v1": 60, "v2": 70, "v3": 500}, nil)
	defer walMock.UnPatch()

	source := newTSafeTestDelegator("v0", 0)
	child1 := newTSafeTestDelegator("v1", 0)
	child2 := newTSafeTestDelegator("v2", 0)
	grandchild := newTSafeTestDelegator("v3", 0)
	source.children["v1"] = child1
	source.children["v2"] = child2
	child1.children["v3"] = grandchild

	got := source.speedupGuranteeTS(context.Background(), source.frontingChildren(), commonpb.ConsistencyLevel_Strong, 900, 0, false)
	assert.Equal(t, uint64(500), got)
}

// A fronted child whose tsafe does not reach the read timestamp fails the read
// with the retriable tsafe-stalled System error, never a partial answer, and the
// child was told which timestamp the read needs so its pipeline does not filter
// the time ticks that would lift its tsafe.
func TestFrontedChildTSafeStallIsARetriableError(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QueryNodeCfg.WaitTsafeStallTimeout.Key
	paramtable.Get().Save(key, "20ms")
	defer paramtable.Get().Reset(key)

	source := newTSafeTestDelegator("v0", 40)
	child1 := newTSafeTestDelegator("v1", 100)
	child2 := newTSafeTestDelegator("v2", 200)
	source.children["v1"] = child1
	source.children["v2"] = child2

	_, err := source.waitTSafe(context.Background(), 120)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrChannelTSafeStalled)
	assert.True(t, merr.IsRetryableErr(err))
	assert.Equal(t, uint64(120), child1.GetLatestRequiredMVCCTimeTick())
	assert.Equal(t, uint64(120), child2.GetLatestRequiredMVCCTimeTick())
}

// A fronted child stopped while a read waits on it fails the read with the
// channel-not-available error instead of serving without its data.
func TestFrontedChildStoppedDuringWaitFailsTheRead(t *testing.T) {
	paramtable.Init()
	source := newTSafeTestDelegator("v0", 40)
	child := newTSafeTestDelegator("v1", 100)
	source.children["v1"] = child

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if child.GetLatestRequiredMVCCTimeTick() >= 120 {
					child.lifetime.SetState(lifetime.Stopped)
					child.tsCond.LockAndBroadcast()
					child.tsCond.L.Unlock()
					return
				}
			}
		}
	}()

	_, err := source.waitTSafe(ctx, 120)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrChannelNotAvailable)
}

// familyRead is one public Strong read on v0 that reports the MVCC timestamp
// the read resolved.
type familyRead struct {
	name string
	read func(ctx context.Context, source *shardDelegator) (mvcc uint64, err error)
}

// strongReadsOnV0 lists every public read that resolves an MVCC timestamp:
// query (get by pk and count share it), search and query stream.
func strongReadsOnV0(guaranteeTs uint64) []familyRead {
	return []familyRead{
		{"query (get by pk, count)", func(ctx context.Context, source *shardDelegator) (uint64, error) {
			req := &querypb.QueryRequest{
				Req:         &internalpb.RetrieveRequest{ConsistencyLevel: commonpb.ConsistencyLevel_Strong, GuaranteeTimestamp: guaranteeTs},
				DmlChannels: []string{"v0"},
			}
			_, err := source.Query(ctx, req)
			return req.GetReq().GetMvccTimestamp(), err
		}},
		{"search", func(ctx context.Context, source *shardDelegator) (uint64, error) {
			req := &querypb.SearchRequest{
				Req:         &internalpb.SearchRequest{ConsistencyLevel: commonpb.ConsistencyLevel_Strong, GuaranteeTimestamp: guaranteeTs},
				DmlChannels: []string{"v0"},
			}
			_, err := source.Search(ctx, req)
			return req.GetReq().GetMvccTimestamp(), err
		}},
		{"query stream", func(ctx context.Context, source *shardDelegator) (uint64, error) {
			req := &querypb.QueryRequest{
				Req:         &internalpb.RetrieveRequest{ConsistencyLevel: commonpb.ConsistencyLevel_Strong, GuaranteeTimestamp: guaranteeTs},
				DmlChannels: []string{"v0"},
			}
			err := source.QueryStream(ctx, req, nil)
			return req.GetReq().GetMvccTimestamp(), err
		}},
	}
}

// Source release detaches the children before it closes the source, so a read
// already in flight can lose its children between taking the fan-out snapshot
// and resolving its timestamp. The children it fans out to are still read, so
// the MVCC speedup and the tsafe wait must cover exactly that snapshot, not
// whatever the source fronts by the time they run.
func TestStrongReadCoversChildrenDetachedAfterTheFanOutSnapshot(t *testing.T) {
	paramtable.Init()
	const (
		proxyGuaranteeTs = uint64(300)
		deleteTs         = uint64(120)
	)
	errPin := errors.New("stop after the read timestamp is resolved")

	for _, tc := range strongReadsOnV0(proxyGuaranteeTs) {
		t.Run(tc.name, func(t *testing.T) {
			_, walMock := mockLocalMVCC(t, map[string]uint64{"v0": 50, "v1": deleteTs, "v2": 60}, nil)
			defer walMock.UnPatch()
			pinMock := mockey.Mock((*shardDelegator).pinReadableSegments).Return(nil, nil, nil, int64(0), errPin).Build()
			defer pinMock.UnPatch()

			// the source keeps consuming time ticks after the fence, so its own
			// tsafe is past its own MVCC but below the delete on v1.
			source := newTSafeTestDelegator("v0", 60)
			child1 := newTSafeTestDelegator("v1", 100)
			child2 := newTSafeTestDelegator("v2", 200)
			source.children["v1"] = child1
			source.children["v2"] = child2

			// detach both children right after the source's first snapshot, as a
			// concurrent release would.
			var origin func(*shardDelegator) []*shardDelegator
			detached := atomic.NewBool(false)
			snapshotMock := mockey.Mock((*shardDelegator).frontingChildren).To(func(sd *shardDelegator) []*shardDelegator {
				children := origin(sd)
				if sd == source && detached.CompareAndSwap(false, true) {
					source.DetachSplitChild("v1")
					source.DetachSplitChild("v2")
				}
				return children
			}).Origin(&origin).Build()
			defer snapshotMock.UnPatch()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			go consumeDeleteWhenRequired(ctx, child1, deleteTs, 130)

			mvcc, err := tc.read(ctx, source)
			require.ErrorIs(t, err, errPin)
			require.True(t, detached.Load())
			assert.GreaterOrEqual(t, mvcc, deleteTs,
				"the read fanned out to children detached mid-read but served at MVCC %d, below their delete at %d", mvcc, deleteTs)
		})
	}
}
