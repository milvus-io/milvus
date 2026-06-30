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

package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// BackfillAtomicGateObserver is the cross-coord revoke driver for bump_defence. It periodically
// sweeps the active rounds and revokes any whose readiness is satisfied (release on
// proxies -> then delete etcd, handled inside Registry.Revoke).
//
// It is modeled on FileResourceObserver and is owned by mixCoordImpl: constructed early,
// Start()ed after all sub-coords are up (dist + meta readable), Stop()ped at the end of
// the Stop sequence. Sub-coords never own its lifecycle.

// defaultBackfillGateSweepInterval paces the revoke sweep AND the gated-set re-push to
// proxies. The re-push is side-effect-free on the proxy (an idempotent map overwrite,
// never a cache invalidation -- see the defence_update early return in proxy
// InvalidateCollectionMetaCache), so the interval only trades revoke latency and
// new-proxy bootstrap convergence against sweep chatter. One minute keeps the gate
// quiet; revoke latency is dominated by the target promote cycle anyway.
const defaultBackfillGateSweepInterval = time.Minute

// backfillGateStore is the registry slice the observer needs.
type backfillGateStore interface {
	List() []*BackfillRound
	Revoke(ctx context.Context, collectionID, roundID int64) error
	RepushAll(ctx context.Context)
}

// backfillReadiness is the readiness slice the observer needs.
type backfillReadiness interface {
	IsRoundReady(ctx context.Context, round *BackfillRound) bool
}

// BackfillAtomicGateObserver periodically reveals satisfied rounds.
type BackfillAtomicGateObserver struct {
	ctx       context.Context
	registry  backfillGateStore
	readiness backfillReadiness
	// collectionDropped answers whether the collection no longer exists (positively
	// dropped, not merely unavailable). Optional: nil skips the check.
	collectionDropped func(ctx context.Context, collectionID int64) bool
	interval          time.Duration

	closeCh   chan struct{}
	wg        sync.WaitGroup
	startOnce sync.Once
	closeOnce sync.Once
}

// NewBackfillAtomicGateObserver builds an observer over the registry + readiness provider. A
// non-positive interval falls back to defaultBackfillGateSweepInterval.
func NewBackfillAtomicGateObserver(ctx context.Context, registry backfillGateStore, readiness backfillReadiness, collectionDropped func(ctx context.Context, collectionID int64) bool, interval time.Duration) *BackfillAtomicGateObserver {
	if interval <= 0 {
		interval = defaultBackfillGateSweepInterval
	}
	return &BackfillAtomicGateObserver{
		ctx:               ctx,
		registry:          registry,
		readiness:         readiness,
		collectionDropped: collectionDropped,
		interval:          interval,
		closeCh:           make(chan struct{}),
	}
}

// Start launches the periodic sweep loop (idempotent).
func (o *BackfillAtomicGateObserver) Start() {
	o.startOnce.Do(func() {
		o.wg.Add(1)
		go o.run()
		mlog.Info(o.ctx, "bump_defence observer started", mlog.Duration("interval", o.interval))
	})
}

// Stop terminates the sweep loop and waits for it to drain (idempotent).
func (o *BackfillAtomicGateObserver) Stop() {
	o.closeOnce.Do(func() {
		close(o.closeCh)
		o.wg.Wait()
		mlog.Info(o.ctx, "bump_defence observer stopped")
	})
}

func (o *BackfillAtomicGateObserver) run() {
	defer o.wg.Done()
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			o.sweep()
		case <-o.closeCh:
			return
		case <-o.ctx.Done():
			return
		}
	}
}

// sweep revokes every round whose readiness is satisfied. A round reveals atomically
// (the readiness provider only returns true once the whole field set is ready on every
// in-scope segment).
func (o *BackfillAtomicGateObserver) sweep() {
	for _, round := range o.registry.List() {
		// A dropped collection never promotes a target again (currentTargetVersion
		// stays 0), so its rounds can never satisfy readiness -- force-revoke them here
		// or they leak (etcd + memory + a pointless RepushAll entry) forever.
		if o.collectionDropped != nil && o.collectionDropped(o.ctx, round.CollectionID) {
			mlog.Info(o.ctx, "revoking bump_defence round of dropped collection",
				mlog.FieldCollectionID(round.CollectionID),
				mlog.Int64("roundID", round.RoundID))
			if err := o.registry.Revoke(o.ctx, round.CollectionID, round.RoundID); err != nil {
				mlog.Warn(o.ctx, "failed to revoke bump_defence round of dropped collection",
					mlog.FieldCollectionID(round.CollectionID),
					mlog.Int64("roundID", round.RoundID),
					mlog.Err(err))
			}
			continue
		}
		if !o.readiness.IsRoundReady(o.ctx, round) {
			continue
		}
		if err := o.registry.Revoke(o.ctx, round.CollectionID, round.RoundID); err != nil {
			mlog.Warn(o.ctx, "failed to revoke satisfied bump_defence round",
				mlog.FieldCollectionID(round.CollectionID),
				mlog.Int64("roundID", round.RoundID),
				mlog.Err(err))
		}
	}
	// Re-push current gated sets so a proxy that started after a register event (and
	// missed its push) converges within one sweep (bootstrap-gap mitigation).
	o.registry.RepushAll(o.ctx)
}
