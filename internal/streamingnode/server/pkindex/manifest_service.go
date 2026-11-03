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

	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ManifestService is the part of the manifest service client that the
// streaming node uses. The client of internal/datacoord/pkindex/client
// implements it. Its methods return merr errors, and a context cancel or
// deadline error unchanged.
type ManifestService interface {
	CommitFlush(ctx context.Context, req *pkindexpb.CommitFlushRequest) (*pkindexpb.Manifest, error)
	GetRecoveryManifests(ctx context.Context, req *pkindexpb.GetRecoveryManifestsRequest) ([]*pkindexpb.ManifestPointerEntry, error)
}

// manifestServiceCommitter is the Committer over the manifest service: it
// translates the index types to the manifest proto and the merr codes of the
// service to the sentinels of the Committer contract.
type manifestServiceCommitter struct {
	svc ManifestService
}

var _ manifest.Committer = (*manifestServiceCommitter)(nil)

// NewManifestServiceCommitter returns the Committer over svc. It panics when
// svc is nil: a Manager without a working Committer must not start (see
// NewManager).
func NewManifestServiceCommitter(svc ManifestService) manifest.Committer {
	if svc == nil {
		panic("the primary key index manifest committer needs a manifest service")
	}
	return &manifestServiceCommitter{svc: svc}
}

// CommitFlush implements manifest.Committer.
func (c *manifestServiceCommitter) CommitFlush(ctx context.Context, args manifest.FlushArgs) (manifest.View, error) {
	req := &pkindexpb.CommitFlushRequest{
		CollectionId:       args.CollectionID,
		Vchannel:           args.VChannel,
		PchannelTerm:       args.PChannelTerm,
		AddSsts:            manifest.SSTRefsOf(args.AddTables),
		CheckpointTimeTick: args.CheckpointTimeTick,
		IndexId:            args.IndexID,
		ResetTimeTick:      args.ResetTimeTick,
	}
	m, err := c.svc.CommitFlush(ctx, req)
	if err != nil {
		return manifest.View{}, fromServiceErr(err)
	}
	if m == nil {
		// A retry gets the same answer: the service broke its contract.
		return manifest.View{}, errors.Wrapf(manifest.ErrRejected, "vchannel %s: the commit returned no manifest", args.VChannel)
	}
	return manifest.ViewOf(m), nil
}

// GetRecoveryManifests implements manifest.Committer.
func (c *manifestServiceCommitter) GetRecoveryManifests(ctx context.Context, refs []manifest.VChannelRef) ([]manifest.RecoveryManifest, error) {
	targets := make([]*pkindexpb.ManifestTarget, len(refs))
	for i, ref := range refs {
		targets[i] = &pkindexpb.ManifestTarget{CollectionId: ref.CollectionID, Vchannel: ref.VChannel, IndexId: ref.IndexID}
	}
	entries, err := c.svc.GetRecoveryManifests(ctx, &pkindexpb.GetRecoveryManifestsRequest{Targets: targets})
	if err != nil {
		return nil, fromServiceErr(err)
	}
	if len(entries) != len(refs) {
		return nil, errors.Wrapf(manifest.ErrRejected, "the manifest service returned %d entries for %d refs", len(entries), len(refs))
	}
	out := make([]manifest.RecoveryManifest, len(entries))
	for i, e := range entries {
		if err := merr.Error(e.GetStatus()); err != nil {
			// A lookup carries no term, so the service never fences it: the
			// fence branch of fromServiceErr matters only for CommitFlush.
			out[i].Err = fromServiceErr(err)
			continue
		}
		if !e.GetFound() {
			continue
		}
		p := e.GetPointer()
		if p.GetVersion() <= 0 || p.GetBodyKey() == "" {
			out[i].Err = errors.Wrapf(manifest.ErrRejected, "vchannel %s: a found manifest with version %d and body key %q",
				refs[i].VChannel, p.GetVersion(), p.GetBodyKey())
			continue
		}
		out[i] = manifest.RecoveryManifest{Found: true, Version: p.GetVersion(), BodyKey: p.GetBodyKey()}
	}
	return out, nil
}

// fromServiceErr translates an error of the manifest service into the errors
// of the Committer contract. The code of a fenced term becomes ErrFenced. Any
// other merr code that is not retryable becomes ErrRejected: the service
// rejected the request for good (an invalid request, a stale term, a
// corrupted manifest, a failed disk of the coordinator). A retryable merr
// code, a context cancel or deadline, and any other error stay as they are:
// the caller retries them. The cause stays in the chain. The rule follows the
// retriable bit of merr, so a code that the coordinator adds later is
// classified without a change here; a code that merr marks permanent by
// mistake ends the WAL instance, which is the safer failure.
func fromServiceErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, merr.ErrChannelNotAvailable):
		return errors.Mark(err, manifest.ErrFenced)
	case merr.IsCanceledOrTimeout(err):
		// A non-retryable merr error that wraps a context error is still a
		// cancel or timeout of this call, not a rejection.
		return err
	case merr.IsMilvusError(err) && !merr.IsRetryableErr(err):
		return errors.Mark(err, manifest.ErrRejected)
	default:
		return err
	}
}
