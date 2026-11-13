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

package tikv

import (
	"context"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Only the worker's three remote operations are implemented. Any accidental
// extra PD operation hits the nil embedded interface instead of being ignored.
type gcTestClient struct {
	pd.Client
	now             uint64
	pin             uint64
	timestampCalls  int
	serviceCalls    int
	gcCalls         int
	getTimestamp    func(context.Context) (uint64, error)
	serviceError    error
	gcError         error
	globalSafePoint uint64
}

func newGCStub() *gcTestClient {
	return &gcTestClient{now: oracle.ComposeTS(1700000000000, 0), pin: math.MaxUint64}
}

func (c *gcTestClient) GetTimestamp(ctx context.Context) (uint64, error) {
	c.timestampCalls++
	if c.getTimestamp != nil {
		return c.getTimestamp(ctx)
	}
	return c.now, nil
}

func (c *gcTestClient) GetPDClient() pd.Client { return c }

func (c *gcTestClient) UpdateServiceGCSafePoint(_ context.Context, _ string, _ int64, point uint64) (uint64, error) {
	c.serviceCalls++
	return min(point, c.pin), c.serviceError
}

func (c *gcTestClient) GC(_ context.Context, point uint64, _ ...tikvclient.GCOpt) (uint64, error) {
	c.gcCalls++
	return max(point, c.globalSafePoint), c.gcError
}

func TestGCWorkerRejectsInvalidRetention(t *testing.T) {
	defer Params.Reset(Params.TiKVCfg.GCLifeTime.Key)
	for _, value := range []string{"-600", "0", "bad", "NaN", "+Inf", "9223372037", "9223372036854775808", "1e100", "0.0000000001"} {
		t.Run(value, func(t *testing.T) {
			client := newGCStub()
			worker := &gcWorker{client: client}
			// Cover both initial configuration and an invalid runtime refresh.
			for _, afterSuccess := range []bool{false, true} {
				if afterSuccess {
					require.NoError(t, Params.Save(Params.TiKVCfg.GCLifeTime.Key, "60"))
					_, err := worker.advanceSafePoint(t.Context())
					require.NoError(t, err)
				}
				require.NoError(t, Params.Save(Params.TiKVCfg.GCLifeTime.Key, value))
				calls := [3]int{client.timestampCalls, client.serviceCalls, client.gcCalls}
				metric := testutil.ToFloat64(metrics.MetaTiKVGCSafePoint)
				_, err := worker.advanceSafePoint(t.Context())
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Equal(t, calls, [3]int{client.timestampCalls, client.serviceCalls, client.gcCalls})
				require.Equal(t, metric, testutil.ToFloat64(metrics.MetaTiKVGCSafePoint))
			}
		})
	}

	t.Run("retention before timestamp epoch", func(t *testing.T) {
		require.NoError(t, Params.Save(Params.TiKVCfg.GCLifeTime.Key, "9223372036"))
		client := newGCStub()
		_, err := (&gcWorker{client: client}).advanceSafePoint(t.Context())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.Equal(t, [3]int{1, 0, 0}, [3]int{client.timestampCalls, client.serviceCalls, client.gcCalls})
	})

	t.Run("fractional seconds retain a positive window", func(t *testing.T) {
		for _, value := range []string{"0.000000001", "1e-9"} {
			require.NoError(t, Params.Save(Params.TiKVCfg.GCLifeTime.Key, value))
			client := newGCStub()
			point, err := (&gcWorker{client: client}).advanceSafePoint(t.Context())
			require.NoError(t, err)
			require.Equal(t, oracle.ComposeTS(oracle.ExtractPhysical(client.now)-1, 0), point)
		}
	})
}

func TestGCWorkerScanProgress(t *testing.T) {
	require.NoError(t, Params.Save(Params.TiKVCfg.GCLifeTime.Key, "60"))
	defer Params.Reset(Params.TiKVCfg.GCLifeTime.Key)
	client := newGCStub()
	client.pin = client.now - oracle.ComposeTS(time.Hour.Milliseconds(), 0)
	worker := &gcWorker{client: client}

	for range 3 {
		point, err := worker.advanceSafePoint(t.Context())
		require.NoError(t, err)
		require.Equal(t, client.pin, point)
	}
	require.Equal(t, 3, client.serviceCalls, "keep refreshing the service registration")
	require.Equal(t, 1, client.gcCalls, "do not repeat the pinned scan")

	client.pin = math.MaxUint64 // backup released its pin
	client.gcError = merr.ErrServiceUnavailable
	metric := testutil.ToFloat64(metrics.MetaTiKVGCSafePoint)
	_, err := worker.advanceSafePoint(t.Context())
	require.ErrorIs(t, err, client.gcError)
	require.Equal(t, metric, testutil.ToFloat64(metrics.MetaTiKVGCSafePoint))
	client.gcError = nil
	_, err = worker.advanceSafePoint(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, client.gcCalls, "retry a failed scan with the same candidate")
	_, err = worker.advanceSafePoint(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, client.gcCalls)

	_, err = (&gcWorker{client: client}).advanceSafePoint(t.Context())
	require.NoError(t, err)
	require.Equal(t, 4, client.gcCalls, "a new worker must establish its own completed scan")
}

func TestGCWorkerDoesNotCacheOtherWorkersProgress(t *testing.T) {
	require.NoError(t, Params.Save(Params.TiKVCfg.GCLifeTime.Key, "60"))
	defer Params.Reset(Params.TiKVCfg.GCLifeTime.Key)
	client := newGCStub()
	client.globalSafePoint = client.now
	worker := &gcWorker{client: client}
	_, err := worker.advanceSafePoint(t.Context())
	require.NoError(t, err)
	client.now += oracle.ComposeTS(1000, 0)
	_, err = worker.advanceSafePoint(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, client.gcCalls, "a higher global result is not proof that we scanned the next candidate")
}

func TestGCWorkerRemoteErrors(t *testing.T) {
	require.NoError(t, Params.Save(Params.TiKVCfg.GCLifeTime.Key, "60"))
	defer Params.Reset(Params.TiKVCfg.GCLifeTime.Key)
	for _, phase := range []string{"timestamp", "service", "gc"} {
		t.Run(phase, func(t *testing.T) {
			client := newGCStub()
			want := merr.ErrServiceUnavailable
			switch phase {
			case "timestamp":
				client.getTimestamp = func(context.Context) (uint64, error) { return 0, want }
			case "service":
				client.serviceError = want
			case "gc":
				client.gcError = want
			}
			worker := &gcWorker{client: client}
			metric := testutil.ToFloat64(metrics.MetaTiKVGCSafePoint)
			_, err := worker.advanceSafePoint(t.Context())
			require.ErrorIs(t, err, want)
			require.Equal(t, merr.Code(want), merr.Code(err))
			require.Zero(t, worker.lastCandidate)
			require.Equal(t, metric, testutil.ToFloat64(metrics.MetaTiKVGCSafePoint))
			if phase == "timestamp" {
				require.Zero(t, client.serviceCalls)
			}
			if phase != "gc" {
				require.Zero(t, client.gcCalls)
			}
			client.getTimestamp, client.serviceError, client.gcError = nil, nil, nil
			_, err = worker.advanceSafePoint(t.Context())
			require.NoError(t, err)
			require.Positive(t, worker.lastCandidate)
		})
	}
}

func TestGCWorkerIntervalRefresh(t *testing.T) {
	defer Params.Reset(Params.TiKVCfg.GCInterval.Key)
	defer Params.Reset(Params.TiKVCfg.GCEnabled.Key)
	for _, enabled := range []string{"false", "true"} {
		for _, value := range []string{"0", "-1", "bad", "9223372037", "NaN", "+Inf"} {
			t.Run(enabled+"/"+value, func(t *testing.T) {
				require.NoError(t, Params.Save(Params.TiKVCfg.GCEnabled.Key, enabled))
				require.NoError(t, Params.Save(Params.TiKVCfg.GCInterval.Key, value))
				synctest.Test(t, func(t *testing.T) {
					client := newGCStub()
					stop := (&gcWorker{client: client}).start(t.Context())
					defer stop()
					synctest.Wait()
					time.Sleep(defaultGCInterval - time.Nanosecond)
					synctest.Wait()
					require.Zero(t, client.timestampCalls)
					// Refresh while waiting: the already armed timer is preserved.
					require.NoError(t, Params.Save(Params.TiKVCfg.GCInterval.Key, "2"))
					time.Sleep(time.Nanosecond)
					synctest.Wait()
					want := 0
					if enabled == "true" {
						want = 1
					}
					require.Equal(t, want, client.timestampCalls)
					// Refresh back to invalid: the following timer must wait again.
					require.NoError(t, Params.Save(Params.TiKVCfg.GCInterval.Key, value))
					time.Sleep(2 * time.Second)
					synctest.Wait()
					require.Equal(t, want*2, client.timestampCalls)
					time.Sleep(defaultGCInterval - time.Nanosecond)
					synctest.Wait()
					require.Equal(t, want*2, client.timestampCalls)
					stop() // cancellation must interrupt the fallback wait
				})
			})
		}
	}
}

func TestGCWorkerStopDuringTimestamp(t *testing.T) {
	require.NoError(t, Params.Save(Params.TiKVCfg.GCEnabled.Key, "true"))
	defer Params.Reset(Params.TiKVCfg.GCEnabled.Key)
	require.NoError(t, Params.Save(Params.TiKVCfg.GCInterval.Key, "0.001"))
	defer Params.Reset(Params.TiKVCfg.GCInterval.Key)
	synctest.Test(t, func(t *testing.T) {
		client := newGCStub()
		client.getTimestamp = func(ctx context.Context) (uint64, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		stop := (&gcWorker{client: client}).start(t.Context())
		defer stop()
		synctest.Wait()
		time.Sleep(time.Millisecond)
		synctest.Wait()
		require.Equal(t, 1, client.timestampCalls)
		before := time.Now()
		stop()
		require.Equal(t, before, time.Now(), "stop must not wait for a retry timeout")
		require.Zero(t, client.serviceCalls)
		require.Zero(t, client.gcCalls)
	})
}
