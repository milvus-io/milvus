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
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// cascadeFamily is a source v0 fronting children v1 and v2, where v1 was split
// again while still fronted and fronts grandchild v3.
type cascadeFamily struct {
	source, child1, child2, grandchild *shardDelegator
}

func newCascadeFamily(sourceTSafe, child1TSafe, child2TSafe, grandchildTSafe uint64) cascadeFamily {
	f := cascadeFamily{
		source:     newTSafeTestDelegator("v0", sourceTSafe),
		child1:     newTSafeTestDelegator("v1", child1TSafe),
		child2:     newTSafeTestDelegator("v2", child2TSafe),
		grandchild: newTSafeTestDelegator("v3", grandchildTSafe),
	}
	f.child1.SetFrontingParent(f.source)
	f.child2.SetFrontingParent(f.source)
	f.grandchild.SetFrontingParent(f.child1)
	f.source.children["v1"] = f.child1
	f.source.children["v2"] = f.child2
	f.child1.children["v3"] = f.grandchild
	return f
}

// While v0 still fronts v1, v1's own split has moved part of its key range to
// v3. A read through v0 has to reach v3's view too, or rows written to v3 after
// v1's fence are missing from it.
func TestReadThroughSourceReachesGrandchildren(t *testing.T) {
	paramtable.Init()

	var mu sync.Mutex
	var read []string
	record := func(sd *shardDelegator) {
		mu.Lock()
		defer mu.Unlock()
		read = append(read, sd.vchannelName)
	}

	cases := []struct {
		name  string
		mock  func() *mockey.Mocker
		fetch func(ctx context.Context, source *shardDelegator) error
	}{
		{"query", func() *mockey.Mocker {
			return mockey.Mock((*shardDelegator).queryInternal).To(
				func(sd *shardDelegator, _ context.Context, _ *querypb.QueryRequest, _ splitReadScope) ([]*internalpb.RetrieveResults, error) {
					record(sd)
					return nil, nil
				}).Build()
		}, func(ctx context.Context, source *shardDelegator) error {
			_, err := source.Query(ctx, &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{}, DmlChannels: []string{"v0"}})
			return err
		}},
		{"search", func() *mockey.Mocker {
			return mockey.Mock((*shardDelegator).searchInternal).To(
				func(sd *shardDelegator, _ context.Context, _ *querypb.SearchRequest, _ splitReadScope) ([]*internalpb.SearchResults, error) {
					record(sd)
					return nil, nil
				}).Build()
		}, func(ctx context.Context, source *shardDelegator) error {
			_, err := source.Search(ctx, &querypb.SearchRequest{Req: &internalpb.SearchRequest{}, DmlChannels: []string{"v0"}})
			return err
		}},
		{"query stream", func() *mockey.Mocker {
			return mockey.Mock((*shardDelegator).queryStreamInternal).To(
				func(sd *shardDelegator, _ context.Context, _ *querypb.QueryRequest, _ streamrpc.QueryStreamServer, _ splitReadScope) error {
					record(sd)
					return nil
				}).Build()
		}, func(ctx context.Context, source *shardDelegator) error {
			return source.QueryStream(ctx, &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{}, DmlChannels: []string{"v0"}}, nil)
		}},
		{"statistics", func() *mockey.Mocker {
			return mockey.Mock((*shardDelegator).getStatisticsInternal).To(
				func(sd *shardDelegator, _ context.Context, _ *querypb.GetStatisticsRequest, _ splitReadScope) ([]*internalpb.GetStatisticsResponse, error) {
					record(sd)
					return nil, nil
				}).Build()
		}, func(ctx context.Context, source *shardDelegator) error {
			_, err := source.GetStatistics(ctx, &querypb.GetStatisticsRequest{Req: &internalpb.GetStatisticsRequest{}, DmlChannels: []string{"v0"}})
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			read = nil
			mu.Unlock()
			internalMock := tc.mock()
			defer internalMock.UnPatch()

			family := newCascadeFamily(0, 0, 0, 0)
			require.NoError(t, tc.fetch(context.Background(), family.source))

			mu.Lock()
			defer mu.Unlock()
			assert.ElementsMatch(t, []string{"v0", "v1", "v2", "v3"}, read,
				"a read through the source must cover every delegator of its split family")
		})
	}
}

// The I2 refusal applies at every level of the family: a read through v0 is
// refused while v1 is still spawning a grandchild, whether v0 sees the spawn at
// entry or it starts while the read waits.
func TestReadThroughSourceIsRefusedWhileAGrandchildIsSpawning(t *testing.T) {
	paramtable.Init()
	const proxyGuaranteeTs = uint64(300)
	errPin := errors.New("stop after the read timestamp is resolved")

	t.Run("at entry", func(t *testing.T) {
		for _, tc := range familyReadsOnV0(proxyGuaranteeTs) {
			t.Run(tc.name, func(t *testing.T) {
				_, walMock := mockLocalMVCC(t, map[string]uint64{"v0": 50, "v1": 60, "v2": 60}, nil)
				defer walMock.UnPatch()
				pinMock := mockey.Mock((*shardDelegator).pinReadableSegments).Return(nil, nil, nil, int64(0), errPin).Build()
				defer pinMock.UnPatch()

				family := newCascadeFamily(60, 100, 200, 0)
				delete(family.child1.children, "v3")
				markSpawning(family.child1, "v3")

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := tc.read(ctx, family.source)
				assertRetriableFamilyRefusal(t, err)
				assert.Equal(t, 0, pinMock.Times())
			})
		}
	})

	t.Run("during the wait", func(t *testing.T) {
		_, walMock := mockLocalMVCC(t, map[string]uint64{"v0": 50, "v1": 60, "v2": 60}, nil)
		defer walMock.UnPatch()
		pinMock := mockey.Mock((*shardDelegator).pinReadableSegments).Return(nil, nil, nil, int64(0), errPin).Build()
		defer pinMock.UnPatch()

		family := newCascadeFamily(60, 40, 200, 0)
		delete(family.child1.children, "v3")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// v1 consumes its fence while the read waits for v1's tsafe.
		go func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if family.child1.GetLatestRequiredMVCCTimeTick() < 60 {
						continue
					}
					markSpawning(family.child1, "v3")
					family.child1.UpdateTSafe(100)
					return
				}
			}
		}()

		_, err := strongReadsOnV0(proxyGuaranteeTs)[0].read(ctx, family.source)
		assertRetriableFamilyRefusal(t, err)
		assert.Equal(t, 0, pinMock.Times())
	})
}

// One snapshot per level, taken once per read: a grandchild detached after the
// read took its snapshot is still covered by the MVCC speedup and the wait,
// because the read still fans out to it.
func TestStrongReadCoversGrandchildrenDetachedMidRead(t *testing.T) {
	paramtable.Init()
	const (
		proxyGuaranteeTs = uint64(300)
		deleteTs         = uint64(120)
	)
	errPin := errors.New("stop after the read timestamp is resolved")

	for _, tc := range strongReadsOnV0(proxyGuaranteeTs) {
		t.Run(tc.name, func(t *testing.T) {
			_, walMock := mockLocalMVCC(t, map[string]uint64{"v0": 50, "v1": 60, "v2": 60, "v3": deleteTs}, nil)
			defer walMock.UnPatch()
			pinMock := mockey.Mock((*shardDelegator).pinReadableSegments).Return(nil, nil, nil, int64(0), errPin).Build()
			defer pinMock.UnPatch()

			// v1 keeps consuming time ticks after its own fence, so its own tsafe
			// is past its MVCC; v3 holds a delete at 120 it has not consumed yet.
			family := newCascadeFamily(60, 70, 200, 100)

			// detach v3 from v1 as the read starts resolving its timestamp, as a
			// concurrent release of v1 would.
			var origin func(*shardDelegator, uint64)
			detached := atomic.NewBool(false)
			hook := mockey.Mock((*shardDelegator).updateLatestRequiredMVCCTimestamp).To(func(sd *shardDelegator, ts uint64) {
				if sd == family.source && detached.CompareAndSwap(false, true) {
					family.child1.DetachSplitChild("v3")
				}
				origin(sd, ts)
			}).Origin(&origin).Build()
			defer hook.UnPatch()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			go consumeDeleteWhenRequired(ctx, family.grandchild, deleteTs, 130)

			mvcc, err := tc.read(ctx, family.source)
			require.ErrorIs(t, err, errPin)
			require.True(t, detached.Load())
			assert.GreaterOrEqual(t, mvcc, deleteTs,
				"the read covers grandchild v3 but served at MVCC %d, below its delete at %d", mvcc, deleteTs)
		})
	}
}
