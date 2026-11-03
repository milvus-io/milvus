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

package indexmanager

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// defaultManifestReadConcurrency is the number of parallel manifest body
// reads when ManagerConfig.ManifestReadConcurrency is at or below 0.
const defaultManifestReadConcurrency = 64

// manifestLookupBatch is the number of refs in one GetRecoveryManifests
// call. The tests lower it.
var manifestLookupBatch = manifest.MaxRecoveryManifests

// RecoverIndexes opens the Index of every spec and reads its manifest: a
// vchannel without a manifest is ready at once; one with a manifest gets its
// view and waits for the install that Start submits. It returns the open
// Indexes, in the order of specs. The WAL recovery calls it at the open,
// before it replays the WAL.
//
// A fenced or rejected lookup ends the WAL open. Every other error is
// retried until ctx, the context of the WAL open, ends.
func (c *pchannel) RecoverIndexes(ctx context.Context, specs []IndexSpec) ([]index.Index, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, errClosed("pchannel " + c.info.Name)
	}
	defer c.lifetime.Done()
	opened := make([]index.Index, 0, len(specs))
	for _, spec := range specs {
		idx, _, err := c.OpenIndex(ctx, spec)
		if err != nil {
			return nil, errors.Wrapf(err, "open the primary key index of vchannel %s", spec.VChannel)
		}
		opened = append(opened, idx)
	}
	refs := make([]manifest.VChannelRef, 0, len(opened))
	for _, idx := range opened {
		refs = append(refs, manifest.VChannelRef{CollectionID: idx.CollectionID(), IndexID: idx.IndexID(), VChannel: idx.VChannel()})
	}
	found := make(map[string]manifest.RecoveryManifest)
	opts := c.retryOptions()
	for start := 0; start < len(refs); start += manifestLookupBatch {
		batch := refs[start:min(start+manifestLookupBatch, len(refs))]
		if err := retry.Do(ctx, func() error {
			answers, err := c.m.cfg.Committer.GetRecoveryManifests(ctx, batch)
			if err != nil {
				return unrecoverableIfFatal(err)
			}
			if len(answers) != len(batch) {
				// A retry gets the same answer: the service broke its contract.
				return retry.Unrecoverable(errors.Wrapf(manifest.ErrRejected, "%d answers for %d refs", len(answers), len(batch)))
			}
			for i, a := range answers {
				if a.Err != nil {
					return unrecoverableIfFatal(a.Err)
				}
				if a.Found {
					found[batch[i].VChannel] = a
				}
			}
			return nil
		}, opts...); err != nil {
			return nil, errors.Wrap(err, "look up the manifests of the primary key index")
		}
	}

	if err := c.readManifests(ctx, found, opts); err != nil {
		return nil, err
	}
	c.mu.Lock()
	for _, idx := range opened {
		if _, ok := found[idx.VChannel()]; ok {
			c.installPending = append(c.installPending, idx)
		}
	}
	c.mu.Unlock()
	for _, idx := range opened {
		if _, ok := found[idx.VChannel()]; !ok {
			// No manifest: the committed set is empty, so the Index is ready
			// before the replay, see Index.MarkReady.
			idx.SetCommitted(nil)
		}
	}
	c.logger.Info(ctx, "primary key index manifests read", mlog.Int("vchannels", len(opened)), mlog.Int("withManifest", len(found)))
	return opened, nil
}

// readManifests reads the bodies of the found manifests in parallel, at most
// the concurrency of the ManagerConfig at a time, and records each view in
// its Index. The first failed read cancels the other reads: the whole Recover
// fails. It returns a fatal error of any read before the error of a canceled
// sibling.
func (c *pchannel) readManifests(ctx context.Context, found map[string]manifest.RecoveryManifest, opts []retry.Option) error {
	concurrency := c.m.cfg.ManifestReadConcurrency
	if concurrency <= 0 {
		concurrency = defaultManifestReadConcurrency
	}
	readCtx, cancelReads := context.WithCancel(ctx)
	defer cancelReads()
	pool := conc.NewPool[struct{}](concurrency)
	defer pool.Release()
	futures := make([]*conc.Future[struct{}], 0, len(found))
	for vchannel, a := range found {
		idx := c.get(vchannel)
		if idx == nil {
			continue
		}
		bodyKey, version := a.BodyKey, a.Version
		futures = append(futures, pool.Submit(func() (struct{}, error) {
			var view manifest.View
			if err := retry.Do(readCtx, func() error {
				v, err := manifest.ReadBody(readCtx, c.m.cfg.Reader, bodyKey)
				if err != nil {
					return unrecoverableIfFatal(err)
				}
				if v.Version != version {
					// The body at a key never changes, so a retry reads the same version.
					return retry.Unrecoverable(errors.Wrapf(manifest.ErrRejected, "manifest body %s has version %d, the pointer says %d", bodyKey, v.Version, version))
				}
				view = v
				return nil
			}, opts...); err != nil {
				cancelReads()
				return struct{}{}, errors.Wrapf(err, "read the manifest of vchannel %s", vchannel)
			}
			idx.SetCommitted(&view)
			return struct{}{}, nil
		}))
	}
	first := conc.BlockOnAll(futures...)
	if first == nil {
		return nil
	}
	for _, f := range futures {
		if err := f.Err(); err != nil && manifest.IsFatal(err) {
			return err
		}
	}
	return first
}

// retryOptions returns the retry options of the recovery: no attempt limit,
// a backoff from BackoffBase up to MaxBackoff.
func (c *pchannel) retryOptions() []retry.Option {
	p := c.m.cfg.Params()
	return []retry.Option{retry.AttemptAlways(), retry.Sleep(p.BackoffBase), retry.MaxSleepTime(p.MaxBackoff)}
}

// unrecoverableIfFatal stops the retry on an error that a retry cannot fix.
func unrecoverableIfFatal(err error) error {
	if err != nil && manifest.IsFatal(err) {
		return retry.Unrecoverable(err)
	}
	return err
}
