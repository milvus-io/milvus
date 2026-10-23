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

package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// defaultManifestReadConcurrency is the number of parallel manifest body
// reads when Param.ManifestReadConcurrency is at or below 0.
const defaultManifestReadConcurrency = 64

// manifestLookupBatch is the number of refs in one GetRecoveryManifests
// call. The tests lower it.
var manifestLookupBatch = pkindex.MaxRecoveryManifests

// lookupManifests reads the manifests of the indexed vchannels. A vchannel
// without a manifest is ready at once; one with a manifest gets its view and
// waits for the install.
//
// Without a Committer nothing is ever committed, and the WAL checkpoint does
// not wait for the index (see PChannelIndexManager.LastAcked), so the writes
// before the checkpoint are gone at a reopen and the engine misses keys.
// Every vchannel that exists at the open stays degraded: every key counts as
// present. Only a collection created later in this WAL lifetime is ready.
//
// A fenced or rejected lookup ends the WAL open. Every other error is
// retried until ctx, the context of the WAL open, ends.
func (r *Recovery) lookupManifests(ctx context.Context, committer pkindex.Committer, concurrency int) error {
	if committer == nil {
		return nil
	}
	if concurrency <= 0 {
		concurrency = defaultManifestReadConcurrency
	}
	opts := r.retryOptions()

	refs := make([]pkindex.VChannelRef, 0, len(r.targets))
	for vchannel, t := range r.targets {
		refs = append(refs, pkindex.VChannelRef{CollectionID: t.idx.CollectionID(), VChannel: vchannel})
	}
	found := make(map[string]pkindex.RecoveryManifest)
	for start := 0; start < len(refs); start += manifestLookupBatch {
		batch := refs[start:min(start+manifestLookupBatch, len(refs))]
		if err := retry.Do(ctx, func() error {
			answers, err := committer.GetRecoveryManifests(ctx, batch)
			if err != nil {
				return unrecoverableIfFatal(err)
			}
			if len(answers) != len(batch) {
				// A retry gets the same answer: the service broke its contract.
				return retry.Unrecoverable(errors.Wrapf(index.ErrRejected, "%d answers for %d refs", len(answers), len(batch)))
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
			return errors.Wrap(err, "look up the manifests of the primary key index")
		}
	}

	if err := r.readManifests(ctx, committer, found, concurrency, opts); err != nil {
		return err
	}
	for vchannel, t := range r.targets {
		if _, ok := found[vchannel]; ok {
			r.pending = append(r.pending, t.idx)
			continue
		}
		// No manifest: the committed set is empty, so the Index is ready
		// before the replay, see Index.MarkReady.
		t.idx.MarkReady(ctx)
	}
	r.logger.Info(ctx, "primary key index manifests read", mlog.Int("vchannels", len(r.targets)), mlog.Int("withManifest", len(found)))
	return nil
}

// readManifests reads the bodies of the found manifests in parallel, at most
// concurrency at a time, and records each view in its Index. The first
// failed read cancels the other reads: the whole Recover fails. It returns a
// fatal error of any read before the error of a canceled sibling.
func (r *Recovery) readManifests(ctx context.Context, committer pkindex.Committer, found map[string]pkindex.RecoveryManifest, concurrency int, opts []retry.Option) error {
	readCtx, cancelReads := context.WithCancel(ctx)
	defer cancelReads()
	pool := conc.NewPool[struct{}](concurrency)
	defer pool.Release()
	futures := make([]*conc.Future[struct{}], 0, len(found))
	for vchannel, a := range found {
		t := r.targets[vchannel]
		bodyKey, version := a.BodyKey, a.Version
		futures = append(futures, pool.Submit(func() (struct{}, error) {
			var view index.ManifestView
			if err := retry.Do(readCtx, func() error {
				v, err := committer.ReadManifest(readCtx, bodyKey)
				if err != nil {
					return unrecoverableIfFatal(err)
				}
				if v.Version != version {
					// The body at a key never changes, so a retry reads the same version.
					return retry.Unrecoverable(errors.Wrapf(index.ErrRejected, "manifest body %s has version %d, the pointer says %d", bodyKey, v.Version, version))
				}
				view = v
				return nil
			}, opts...); err != nil {
				cancelReads()
				return struct{}{}, errors.Wrapf(err, "read the manifest of vchannel %s", vchannel)
			}
			t.idx.SetCommitted(view)
			return struct{}{}, nil
		}))
	}
	first := conc.BlockOnAll(futures...)
	if first == nil {
		return nil
	}
	for _, f := range futures {
		if err := f.Err(); err != nil && index.IsFatal(err) {
			return err
		}
	}
	return first
}

// retryOptions returns the retry options of the recovery: no attempt limit,
// a backoff from BackoffBase up to MaxBackoff.
func (r *Recovery) retryOptions() []retry.Option {
	p := r.mgr.Params()
	return []retry.Option{retry.AttemptAlways(), retry.Sleep(p.BackoffBase), retry.MaxSleepTime(p.MaxBackoff)}
}

// unrecoverableIfFatal stops the retry on an error that a retry cannot fix.
func unrecoverableIfFatal(err error) error {
	if err != nil && index.IsFatal(err) {
		return retry.Unrecoverable(err)
	}
	return err
}
