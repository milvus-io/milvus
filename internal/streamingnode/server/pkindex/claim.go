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

package pkindex

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// recoveryBatch is the largest number of refs in one GetRecoveryManifests
// call. Tests lower it.
var recoveryBatch = MaxRecoveryManifests

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
	// The Indexes that are open now are those of the collections that existed
	// at WAL open. Only they look up their manifests.
	recovered := make([]*Index, 0, len(c.indexes))
	for _, idx := range c.indexes {
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
	err := retry.Do(ctx, func() error {
		return c.retryable(ctx, "claim", 1, committer.ClaimPChannel(ctx, c.info.Name, c.info.Term))
	}, c.retryOptions()...)
	if err != nil {
		c.stopClaim(ctx, "claim", err)
		return
	}
	c.logger.Info(ctx, "primary key index pchannel claimed", mlog.Int("recoveredVChannels", len(recovered)))
	if err := c.lookupRecovered(ctx, recovered); err != nil {
		c.stopClaim(ctx, "manifest lookup", err)
		return
	}

	lookedUp := make(map[*Index]struct{}, len(recovered))
	for _, idx := range recovered {
		lookedUp[idx] = struct{}{}
	}
	c.mu.Lock()
	c.claimed = true
	var created []*Index
	for _, idx := range c.indexes {
		if _, ok := lookedUp[idx]; !ok {
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
// only the items that failed. It returns an error when ctx ends or a failure
// is fatal.
func (c *Channel) lookupRecovered(ctx context.Context, idxs []*Index) error {
	committer := c.m.cfg.Committer
	pending := idxs
	return retry.Do(ctx, func() error {
		var failed []*Index
		var lastErr error
		for start := 0; start < len(pending); start += recoveryBatch {
			batch := pending[start:min(start+recoveryBatch, len(pending))]
			refs := make([]VChannelRef, 0, len(batch))
			for _, idx := range batch {
				refs = append(refs, VChannelRef{CollectionID: idx.collectionID, VChannel: idx.vchannel})
			}
			answers, err := committer.GetRecoveryManifests(ctx, refs)
			if err == nil && len(answers) != len(refs) {
				err = errors.Wrapf(ErrRejected, "pchannel %s: %d manifest answers for %d vchannels", c.info.Name, len(answers), len(refs))
			}
			if err != nil {
				if ctx.Err() != nil || isFatal(err) {
					return retry.Unrecoverable(err)
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
					return retry.Unrecoverable(a.Err)
				}
				failed, lastErr = append(failed, batch[i]), a.Err
			}
		}
		pending = failed
		if len(failed) == 0 {
			return nil
		}
		return c.retryable(ctx, "manifest lookup", len(failed), lastErr)
	}, c.retryOptions()...)
}

// retryOptions retries a claim or a lookup until it succeeds, a failure is
// fatal or ctx ends. The wait doubles from the backoff base up to the max.
func (c *Channel) retryOptions() []retry.Option {
	params := c.m.cfg.Params()
	return []retry.Option{
		retry.AttemptAlways(),
		retry.Sleep(params.BackoffBase),
		retry.MaxSleepTime(params.MaxBackoff),
		// retryable decides which errors stop the retry, not the merr type
		retry.RetryErr(func(error) bool { return true }),
	}
}

// retryable handles the result of a claim or lookup call. A fatal error stops
// the retry. Another error is logged and retried.
func (c *Channel) retryable(ctx context.Context, step string, items int, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil || isFatal(err) {
		return retry.Unrecoverable(err)
	}
	c.logger.Warn(ctx, "primary key index manifest call failed, retry later",
		mlog.String("step", step), mlog.Int("items", items), mlog.Err(err))
	return err
}

// stopClaim reports a claim or lookup that stopped. A fatal error goes to
// OnFatal. It ignores the error of a ctx that ended.
func (c *Channel) stopClaim(ctx context.Context, step string, err error) {
	if ctx.Err() != nil {
		return
	}
	c.logger.Error(ctx, "primary key index manifest call failed, the WAL becomes unavailable", mlog.String("step", step), mlog.Err(err))
	c.fatal(err)
}

// newBackoff returns the handover backoff of an Index with the current
// params.
func (c *Channel) newBackoff() *typeutil.BackoffWithInstant {
	params := c.m.cfg.Params()
	return typeutil.NewBackoffWithInstant(typeutil.BackoffTimerConfig{
		Backoff: typeutil.BackoffConfig{
			InitialInterval: params.BackoffBase,
			Multiplier:      2,
			MaxInterval:     params.MaxBackoff,
		},
	})
}
