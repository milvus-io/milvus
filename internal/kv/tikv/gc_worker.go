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
	"strconv"
	"time"

	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	pd "github.com/tikv/pd/client"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// gcWorkerServiceID is the service safe point ID of the GC worker. PD keeps the
// service safe point of this ID without a TTL, and TiDB's GC worker registers
// itself under the same ID, so the service safe point of a backup, for
// example, still holds the GC safe point back.
const gcWorkerServiceID = "gc_worker"

const defaultGCInterval = 10 * time.Minute

type gcClient interface {
	GetTimestamp(context.Context) (uint64, error)
	GetPDClient() pd.Client
	GC(context.Context, uint64, ...tikvclient.GCOpt) (uint64, error)
}

// Each worker has one serial loop. Remember only scans completed by this
// worker; a higher global safe point alone does not prove our scan succeeded.
type gcWorker struct {
	client        gcClient
	lastCandidate uint64
	safePoint     uint64
}

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
	return (&gcWorker{client: client}).start(ctx)
}

func (w *gcWorker) start(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		mlog.Info(ctx, "TiKV GC worker started")
		for {
			interval, ok := parseGCSeconds(Params.TiKVCfg.GCInterval.GetValue())
			if !ok {
				interval = defaultGCInterval
				mlog.RatedWarn(ctx, 1.0/60, "invalid TiKV GC interval, using default", mlog.Duration("interval", interval))
			}
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				mlog.Info(ctx, "TiKV GC worker stopped")
				return
			case <-timer.C:
			}
			if !Params.TiKVCfg.GCEnabled.GetAsBool() {
				continue
			}
			start := time.Now()
			safePoint, err := w.advanceSafePoint(ctx)
			if err != nil {
				mlog.Warn(ctx, "failed to advance the TiKV GC safe point", mlog.Err(err))
				continue
			}
			mlog.Info(ctx, "TiKV GC round completed",
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

// advanceSafePoint moves the GC safe point to tikv.gc.lifeTime before the
// current TiKV timestamp, or to the lowest service safe point when that is
// older, and returns the safe point that PD keeps. client-go's GC resolves the
// locks older than the safe point before it moves the safe point. A candidate
// already scanned successfully only refreshes the service registration.
func (w *gcWorker) advanceSafePoint(ctx context.Context) (uint64, error) {
	lifeTime, ok := parseGCSeconds(Params.TiKVCfg.GCLifeTime.GetValue())
	if !ok {
		return 0, merr.WrapErrParameterInvalidMsg("tikv.gc.lifeTime must be a positive, representable duration in seconds")
	}
	now, err := w.client.GetTimestamp(ctx)
	if err != nil {
		return 0, merr.Wrap(err, "get TiKV timestamp")
	}
	// Round retention up to TiKV's millisecond precision, and reject subtraction
	// before the timestamp epoch rather than wrapping a negative time to uint64.
	lifeTimeMillis := int64((lifeTime-1)/time.Millisecond) + 1
	physical := oracle.ExtractPhysical(now)
	if lifeTimeMillis > physical {
		return 0, merr.WrapErrParameterInvalidMsg("tikv.gc.lifeTime exceeds the current TiKV timestamp")
	}
	safePoint := oracle.ComposeTS(physical-lifeTimeMillis, 0)
	minServiceSafePoint, err := w.client.GetPDClient().UpdateServiceGCSafePoint(ctx, gcWorkerServiceID, math.MaxInt64, safePoint)
	if err != nil {
		return 0, merr.Wrap(err, "update TiKV GC worker service safe point")
	}
	if minServiceSafePoint < safePoint {
		safePoint = minServiceSafePoint
	}
	if safePoint <= w.lastCandidate {
		return w.safePoint, nil
	}
	newSafePoint, err := w.client.GC(ctx, safePoint)
	if err != nil {
		return 0, merr.Wrap(err, "advance TiKV GC safe point")
	}
	w.lastCandidate = safePoint
	w.safePoint = newSafePoint
	metrics.MetaTiKVGCSafePoint.Set(float64(oracle.GetTimeFromTS(newSafePoint).Unix()))
	return newSafePoint, nil
}

// Keep integer and fractional seconds supported by ParamItem, but reject
// invalid values before converting to a duration (which can otherwise wrap).
func parseGCSeconds(value string) (time.Duration, bool) {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 || seconds > math.MaxInt64/int64(time.Second) {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	seconds, err := strconv.ParseFloat(value, 64)
	nanos := seconds * float64(time.Second)
	if err != nil || math.IsNaN(nanos) || nanos < 1 || nanos >= 0x1p63 {
		return 0, false
	}
	return time.Duration(nanos), true
}
