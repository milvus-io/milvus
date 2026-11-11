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
	"time"

	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// gcWorkerServiceID is the service safe point ID of the GC worker. PD keeps the
// service safe point of this ID without a TTL, and TiDB's GC worker registers
// itself under the same ID, so the service safe point of a backup, for
// example, still holds the GC safe point back.
const gcWorkerServiceID = "gc_worker"

// StartGCWorker advances the GC safe point of the TiKV cluster of client every
// tikv.gc.interval while tikv.gc.enabled is true, the first time one interval
// after the start, until ctx is done or stop is called; stop waits for a
// running round to end.
//
// TiKV keeps every version of a key until the GC safe point in PD passes it,
// and without a TiDB nothing else advances the safe point, so every overwritten
// or deleted metadata value would stay on disk. Two workers of one Milvus
// cluster, such as two MixCoord replicas during a switchover, compute the same
// safe point and only repeat the lock scan.
func StartGCWorker(ctx context.Context, client *txnkv.Client) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		mlog.Info(ctx, "TiKV GC worker started")
		for {
			select {
			case <-ctx.Done():
				mlog.Info(ctx, "TiKV GC worker stopped")
				return
			case <-time.After(Params.TiKVCfg.GCInterval.GetAsDuration(time.Second)):
			}
			if !Params.TiKVCfg.GCEnabled.GetAsBool() {
				continue
			}
			start := time.Now()
			safePoint, err := advanceGCSafePoint(ctx, client)
			if err != nil {
				mlog.Warn(ctx, "failed to advance the TiKV GC safe point", mlog.Err(err))
				continue
			}
			mlog.Info(ctx, "TiKV GC safe point advanced",
				mlog.Uint64("safePoint", safePoint),
				mlog.Time("safePointTime", oracle.GetTimeFromTS(safePoint)),
				mlog.Duration("elapsed", time.Since(start)))
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// advanceGCSafePoint moves the GC safe point to tikv.gc.lifeTime before the
// current TiKV timestamp, or to the lowest service safe point when that is
// older, and returns the safe point that PD keeps. client-go's GC resolves the
// locks older than the safe point before it moves the safe point.
func advanceGCSafePoint(ctx context.Context, client *txnkv.Client) (uint64, error) {
	now, err := client.CurrentTimestamp(oracle.GlobalTxnScope)
	if err != nil {
		return 0, merr.Wrap(err, "get TiKV timestamp")
	}
	lifeTime := Params.TiKVCfg.GCLifeTime.GetAsDuration(time.Second)
	safePoint := oracle.GoTimeToTS(oracle.GetTimeFromTS(now).Add(-lifeTime))
	minServiceSafePoint, err := client.GetPDClient().UpdateServiceGCSafePoint(ctx, gcWorkerServiceID, math.MaxInt64, safePoint)
	if err != nil {
		return 0, merr.Wrap(err, "update TiKV GC worker service safe point")
	}
	if minServiceSafePoint < safePoint {
		safePoint = minServiceSafePoint
	}
	newSafePoint, err := client.GC(ctx, safePoint)
	if err != nil {
		return 0, merr.Wrap(err, "advance TiKV GC safe point")
	}
	metrics.MetaTiKVGCSafePoint.Set(float64(oracle.GetTimeFromTS(newSafePoint).Unix()))
	return newSafePoint, nil
}
