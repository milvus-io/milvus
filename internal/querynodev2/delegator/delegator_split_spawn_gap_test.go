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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// markSpawning records a child spawn in flight, as ProcessSplitShard does on
// consuming the fence.
func markSpawning(sd *shardDelegator, vchannel string) {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()
	if sd.spawning == nil {
		sd.spawning = make(map[string]struct{})
	}
	sd.spawning[vchannel] = struct{}{}
}

// publishChild completes a spawn, as spawnChildAsync does.
func publishChild(sd *shardDelegator, child *shardDelegator) {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()
	delete(sd.spawning, child.vchannelName)
	sd.children[child.vchannelName] = child
}

// statisticsGuaranteeTs is the guarantee a statistics read on v0 waits for.
// Statistics wait on the raw guarantee (no Strong speedup), so it is kept below
// the source tsafe the tests end with.
const statisticsGuaranteeTs = uint64(55)

// familyReadsOnV0 is every public read on the source: the Strong reads plus
// statistics. requiredTs is the required MVCC each read records on the source
// before it waits.
func familyReadsOnV0(strongGuaranteeTs uint64) []struct {
	familyRead
	requiredTs uint64
} {
	reads := make([]struct {
		familyRead
		requiredTs uint64
	}, 0, 4)
	for _, read := range strongReadsOnV0(strongGuaranteeTs) {
		reads = append(reads, struct {
			familyRead
			requiredTs uint64
		}{read, strongGuaranteeTs})
	}
	return append(reads, struct {
		familyRead
		requiredTs uint64
	}{familyRead{"statistics", func(ctx context.Context, source *shardDelegator) (uint64, error) {
		req := &querypb.GetStatisticsRequest{
			Req:         &internalpb.GetStatisticsRequest{GuaranteeTimestamp: statisticsGuaranteeTs},
			DmlChannels: []string{"v0"},
		}
		_, err := source.GetStatistics(ctx, req)
		return 0, err
	}}, statisticsGuaranteeTs})
}

func assertRetriableFamilyRefusal(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
	assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

// Between consuming the fence and publishing its children, the source has
// already lost the split key range's new writes to the target vchannels but
// fronts no child for them. A read in that gap would answer from the source's
// view alone, so it is refused with a retriable error; once the child is
// published the same read goes through and covers it.
func TestReadThroughSourceIsRefusedWhileAChildIsSpawning(t *testing.T) {
	paramtable.Init()
	const (
		proxyGuaranteeTs = uint64(300)
		deleteTs         = uint64(120)
	)
	errPin := errors.New("stop after the read timestamp is resolved")

	for _, tc := range familyReadsOnV0(proxyGuaranteeTs) {
		t.Run(tc.name, func(t *testing.T) {
			_, walMock := mockLocalMVCC(t, map[string]uint64{"v0": 50, "v1": deleteTs}, nil)
			defer walMock.UnPatch()
			pinMock := mockey.Mock((*shardDelegator).pinReadableSegments).Return(nil, nil, nil, int64(0), errPin).Build()
			defer pinMock.UnPatch()

			source := newTSafeTestDelegator("v0", 60)
			markSpawning(source, "v1")

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err := tc.read(ctx, source)
			assertRetriableFamilyRefusal(t, err)
			assert.Equal(t, 0, pinMock.Times(), "a refused read must not pin any segment")
			// Refused at entry: the read never recorded a timestamp to wait for.
			// The post-wait re-check alone would refuse it too, but only after
			// resolving and waiting for its timestamp.
			assert.Zero(t, source.GetLatestRequiredMVCCTimeTick(), "the read must be refused before it resolves its timestamp")

			// the spawn completes: the retried read passes the family gate and is
			// served through the child.
			publishChild(source, newTSafeTestDelegator("v1", 130))
			mvcc, err := tc.read(ctx, source)
			require.Error(t, err)
			assert.NotErrorIs(t, err, merr.ErrServiceUnavailable, "a published child must not be refused")
			assert.Positive(t, pinMock.Times(), "the retried read must reach the segment pin")
			// getStatisticsInternal replaces the pin error with its own
			// channel-not-available error, so only the other reads carry errPin.
			if tc.name != "statistics" {
				require.ErrorIs(t, err, errPin)
				assert.GreaterOrEqual(t, mvcc, deleteTs)
			}
		})
	}
}

// A read that finds no child at entry can still be overtaken by the fence: the
// source consumes it (and starts spawning) while the read waits for the
// source's tsafe. The fence's writes are then already below the read timestamp,
// so the read is refused after the wait rather than served from the source
// alone.
func TestReadThroughSourceIsRefusedWhenTheFenceIsConsumedDuringTheWait(t *testing.T) {
	paramtable.Init()
	const proxyGuaranteeTs = uint64(300)
	errPin := errors.New("stop after the read timestamp is resolved")

	cases := []struct {
		name    string
		publish bool // the spawn also completes before the wait ends
	}{
		{"spawn still in flight", false},
		{"child published during the wait", true},
	}
	for _, read := range familyReadsOnV0(proxyGuaranteeTs) {
		for _, tc := range cases {
			t.Run(read.name+"/"+tc.name, func(t *testing.T) {
				_, walMock := mockLocalMVCC(t, map[string]uint64{"v0": 50, "v1": 120}, nil)
				defer walMock.UnPatch()
				pinMock := mockey.Mock((*shardDelegator).pinReadableSegments).Return(nil, nil, nil, int64(0), errPin).Build()
				defer pinMock.UnPatch()

				// the source has not consumed the read's timestamp yet, so the read waits.
				source := newTSafeTestDelegator("v0", 40)

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
							// the read has recorded its timestamp and is about to wait.
							if source.GetLatestRequiredMVCCTimeTick() < read.requiredTs {
								continue
							}
							markSpawning(source, "v1")
							if tc.publish {
								publishChild(source, newTSafeTestDelegator("v1", 130))
							}
							source.UpdateTSafe(60)
							return
						}
					}
				}()

				_, err := read.read(ctx, source)
				assertRetriableFamilyRefusal(t, err)
				assert.Equal(t, 0, pinMock.Times())
			})
		}
	}
}

// The post-wait re-check accepts everything that still leaves the snapshot
// covering the read: a fronted child's own read, a child detached since the
// snapshot, a delegator that cannot be fronted, and no split at all.
func TestCheckReadFamilyAcceptsACoveredRead(t *testing.T) {
	source := newTSafeTestDelegator("v0", 0)
	assert.NoError(t, source.checkReadFamily(frontingSourceScope(nil)), "no split")

	child1 := newTSafeTestDelegator("v1", 0)
	child2 := newTSafeTestDelegator("v2", 0)
	source.children["v1"] = child1
	source.children["v2"] = child2
	scope := frontingSourceScope(source.frontingChildren())
	source.DetachSplitChild("v2")
	assert.NoError(t, source.checkReadFamily(scope), "a detached child is still covered")

	source.children["mock"] = &MockShardDelegator{}
	assert.NoError(t, source.checkReadFamily(scope), "a delegator that cannot be fronted is not read")

	markSpawning(source, "v9")
	assert.NoError(t, source.checkReadFamily(scope.forChild()), "a fronted child's read is covered by its source")
	assertRetriableFamilyRefusal(t, source.checkReadFamily(scope))
}
