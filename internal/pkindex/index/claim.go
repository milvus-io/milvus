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

package index

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/snapshot"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// recoveryBatch is the largest number of refs in one GetRecoveryManifests
// call. Tests lower it.
var recoveryBatch = snapshot.MaxRecoveryManifests

// StartClaim starts the claim of the pchannel. The caller calls it once,
// after it added the indexes of the collections that exist at WAL open. Those
// indexes look up their manifests after the claim. An Index added later, by a
// CreateCollection, does not look up: its collection is new. No Index hands
// over before the claim and the lookups are done.
//
// Without a Committer the Channel does not claim, and no Index hands over.
//
// TODO: a later change injects the Committer that the manifest service
// client implements.
func (c *Channel) StartClaim() {
	if c.m.cfg.Committer == nil {
		return
	}
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	c.mu.Lock()
	if c.claimStarted {
		c.mu.Unlock()
		c.lifetime.Done()
		return
	}
	c.claimStarted = true
	recovered := make([]*Index, 0, len(c.indexes))
	for _, idx := range c.indexes {
		idx.recovered = true
		recovered = append(recovered, idx)
	}
	c.mu.Unlock()
	go c.claim(recovered)
}

// claim claims the pchannel for its term once, looks up the manifests of the
// recovered indexes, and then makes the other indexes ready.
func (c *Channel) claim(recovered []*Index) {
	defer c.lifetime.Done()
	ctx := c.ctx
	committer := c.m.cfg.Committer
	var bo snapshot.Backoff
	for {
		err := committer.ClaimPChannel(ctx, c.info.Name, c.info.Term)
		if err == nil {
			break
		}
		if !c.retryable(ctx, &bo, "claim", 1, err) {
			return
		}
	}
	c.logger.Info(ctx, "primary key index pchannel claimed", mlog.Int("recoveredVChannels", len(recovered)))
	if !c.lookupRecovered(ctx, recovered) {
		return
	}

	c.mu.Lock()
	c.claimed = true
	var created []*Index
	for _, idx := range c.indexes {
		if !idx.recovered {
			created = append(created, idx)
		}
	}
	c.mu.Unlock()
	for _, idx := range created {
		idx.markReady(ctx, false)
	}
}

// lookupRecovered looks up the manifests of idxs in calls of at most
// recoveryBatch refs, and marks each Index ready with its answer. It retries
// only the items that failed. It reports false when ctx ends or a failure is
// fatal.
func (c *Channel) lookupRecovered(ctx context.Context, idxs []*Index) bool {
	committer := c.m.cfg.Committer
	var bo snapshot.Backoff
	pending := idxs
	for len(pending) > 0 {
		var failed []*Index
		var lastErr error
		for start := 0; start < len(pending); start += recoveryBatch {
			batch := pending[start:min(start+recoveryBatch, len(pending))]
			refs := make([]snapshot.VChannelRef, 0, len(batch))
			for _, idx := range batch {
				refs = append(refs, snapshot.VChannelRef{CollectionID: idx.collectionID, VChannel: idx.vchannel})
			}
			answers, err := committer.GetRecoveryManifests(ctx, refs)
			if err == nil && len(answers) != len(refs) {
				err = errors.Wrapf(snapshot.ErrRejected, "pchannel %s: %d manifest answers for %d vchannels", c.info.Name, len(answers), len(refs))
			}
			if err != nil {
				if ctx.Err() != nil || isFatal(err) {
					c.fatalLookup(ctx, err)
					return false
				}
				failed, lastErr = append(failed, batch...), err
				continue
			}
			for i, a := range answers {
				if a.Err == nil {
					batch[i].markReady(ctx, a.Found)
					continue
				}
				if isFatal(a.Err) {
					c.fatalLookup(ctx, a.Err)
					return false
				}
				failed, lastErr = append(failed, batch[i]), a.Err
			}
		}
		if len(failed) == 0 {
			return true
		}
		if !c.retryable(ctx, &bo, "manifest lookup", len(failed), lastErr) {
			return false
		}
		pending = failed
	}
	return true
}

// fatalLookup reports a fatal lookup error. It ignores the error of a ctx
// that ended.
func (c *Channel) fatalLookup(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	c.logger.Error(ctx, "primary key index manifest lookup failed, the WAL becomes unavailable", mlog.Err(err))
	c.fatal(err)
}

// retryable handles a failed claim or lookup. A fatal error goes to OnFatal.
// Else it waits for the backoff. It reports whether the caller retries: false
// when the error is fatal or ctx ends.
func (c *Channel) retryable(ctx context.Context, bo *snapshot.Backoff, step string, items int, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if isFatal(err) {
		c.logger.Error(ctx, "primary key index manifest call failed, the WAL becomes unavailable", mlog.String("step", step), mlog.Err(err))
		c.fatal(err)
		return false
	}
	c.fail(bo, time.Now())
	c.logger.Warn(ctx, "primary key index manifest call failed, retry later",
		mlog.String("step", step), mlog.Int("items", items), mlog.Int("failures", bo.Failures()), mlog.Err(err))
	for !bo.Ready(time.Now()) {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(bo.Base):
		}
	}
	return true
}

// fail records a failure in b with the current backoff params.
func (c *Channel) fail(b *snapshot.Backoff, now time.Time) {
	params := c.m.cfg.Params()
	b.Base, b.Max = params.BackoffBase, params.MaxBackoff
	b.Fail(now)
}
