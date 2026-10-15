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

// Package service implements the gRPC service of the primary key index
// manifest coordinator and translates its errors into merr at that boundary.
package service

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/coordinator/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Service implements PKIndexCoordService. It can be registered on a gRPC
// server before its manager exists; it answers ServiceNotReady until
// SetManager is called.
type Service struct {
	mgr atomic.Pointer[manifest.Manager]
}

var _ pkindexpb.PKIndexCoordServiceServer = (*Service)(nil)

// New returns a Service that has no manager.
func New() *Service { return &Service{} }

// SetManager sets the manager that serves the requests.
func (s *Service) SetManager(m *manifest.Manager) { s.mgr.Store(m) }

func (s *Service) manager() (*manifest.Manager, error) {
	if m := s.mgr.Load(); m != nil {
		return m, nil
	}
	return nil, merr.WrapErrServiceNotReadyMsg("primary key index manifest service is not ready")
}

// ClaimPChannel claims a pchannel for a pchannel term.
func (s *Service) ClaimPChannel(ctx context.Context, req *pkindexpb.ClaimPChannelRequest) (*pkindexpb.ClaimPChannelResponse, error) {
	if err := validatePChannel(req.GetPchannel()); err != nil {
		return &pkindexpb.ClaimPChannelResponse{Status: merr.Status(err)}, nil
	}
	if err := validateTerm(req.GetPchannelTerm()); err != nil {
		return &pkindexpb.ClaimPChannelResponse{Status: merr.Status(err)}, nil
	}
	m, err := s.manager()
	if err != nil {
		return &pkindexpb.ClaimPChannelResponse{Status: merr.Status(err)}, nil
	}
	if err := m.ClaimPChannel(ctx, req.GetPchannel(), req.GetPchannelTerm()); err != nil {
		return &pkindexpb.ClaimPChannelResponse{Status: merr.Status(toMerr(err, req.GetPchannel()))}, nil
	}
	return &pkindexpb.ClaimPChannelResponse{Status: merr.Success()}, nil
}

// CommitFlush adds the uploaded SSTs of a flush and returns the manifest
// after the commit. The first flush of a vchannel creates its manifest.
func (s *Service) CommitFlush(ctx context.Context, req *pkindexpb.CommitFlushRequest) (*pkindexpb.CommitFlushResponse, error) {
	if err := validateFlush(req); err != nil {
		return &pkindexpb.CommitFlushResponse{Status: merr.Status(err)}, nil
	}
	m, err := s.manager()
	if err != nil {
		return &pkindexpb.CommitFlushResponse{Status: merr.Status(err)}, nil
	}
	got, err := m.CommitFlush(ctx, req)
	if err != nil {
		return &pkindexpb.CommitFlushResponse{Status: merr.Status(toMerr(err, req.GetVchannel()))}, nil
	}
	return &pkindexpb.CommitFlushResponse{Status: merr.Success(), Manifest: got}, nil
}

// maxRecoveryTargets is the largest number of targets in one
// GetRecoveryManifests request.
const maxRecoveryTargets = 10000

// GetRecoveryManifests returns the manifest pointer of each vchannel; the
// caller reads the body from object storage. It reads the pointers from the
// meta kv, not from the cache. Each entry has its own status, and the failure
// of one target does not fail the others.
func (s *Service) GetRecoveryManifests(ctx context.Context, req *pkindexpb.GetRecoveryManifestsRequest) (*pkindexpb.GetRecoveryManifestsResponse, error) {
	targets, err := validateRecoveryTargets(req.GetTargets())
	if err != nil {
		return &pkindexpb.GetRecoveryManifestsResponse{Status: merr.Status(err)}, nil
	}
	m, err := s.manager()
	if err != nil {
		return &pkindexpb.GetRecoveryManifestsResponse{Status: merr.Status(err)}, nil
	}
	results := m.LoadPointers(ctx, targets)
	entries := make([]*pkindexpb.ManifestPointerEntry, 0, len(results))
	for i, r := range results {
		if r.Err != nil {
			entries = append(entries, &pkindexpb.ManifestPointerEntry{Status: merr.Status(toMerr(r.Err, targets[i].VChannel))})
			continue
		}
		entries = append(entries, &pkindexpb.ManifestPointerEntry{Status: merr.Success(), Found: r.Found, Pointer: r.Pointer})
	}
	return &pkindexpb.GetRecoveryManifestsResponse{Status: merr.Success(), Entries: entries}, nil
}

// validateRecoveryTargets checks that there are 1 to maxRecoveryTargets
// targets, each valid and none repeated, and returns them.
func validateRecoveryTargets(in []*pkindexpb.ManifestTarget) ([]manifest.Target, error) {
	if len(in) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("targets must not be empty")
	}
	if len(in) > maxRecoveryTargets {
		return nil, merr.WrapErrServiceInternalMsg("at most %d targets in one request, got %d", maxRecoveryTargets, len(in))
	}
	targets := make([]manifest.Target, 0, len(in))
	seen := make(map[manifest.Target]struct{}, len(in))
	for _, t := range in {
		if err := validateTarget(t.GetCollectionId(), t.GetVchannel()); err != nil {
			return nil, err
		}
		target := manifest.Target{CollectionID: t.GetCollectionId(), VChannel: t.GetVchannel()}
		if _, ok := seen[target]; ok {
			return nil, merr.WrapErrServiceInternalMsg("target collection %d vchannel %q is repeated", target.CollectionID, target.VChannel)
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	return targets, nil
}

// validateFlush checks the content of a flush request: a valid target whose
// vchannel name has a pchannel, a positive term, a checkpoint that is not 0,
// and a primary key data type of Int64 or VarChar.
func validateFlush(req *pkindexpb.CommitFlushRequest) error {
	if err := validateTarget(req.GetCollectionId(), req.GetVchannel()); err != nil {
		return err
	}
	if funcutil.IsPhysicalChannel(req.GetVchannel()) {
		return merr.WrapErrServiceInternalMsg("vchannel %q has no pchannel in its name", req.GetVchannel())
	}
	if err := validateTerm(req.GetPchannelTerm()); err != nil {
		return err
	}
	if req.GetCheckpointTimeTick() == 0 {
		return merr.WrapErrServiceInternalMsg("checkpoint_time_tick must not be 0")
	}
	if t := req.GetPkDataType(); t != schemapb.DataType_Int64 && t != schemapb.DataType_VarChar {
		return merr.WrapErrServiceInternalMsg("pk_data_type must be Int64 or VarChar, got %s", t)
	}
	return nil
}

// validateTarget checks that a request names a collection and a vchannel that
// can form a manifest key: a positive collection ID and a valid channel name.
func validateTarget(collectionID int64, vchannel string) error {
	if collectionID <= 0 {
		return merr.WrapErrServiceInternalMsg("collection_id must be positive, got %d", collectionID)
	}
	return validateChannelName("vchannel", vchannel)
}

// validateChannelName checks that a channel name can be part of a key: it is
// not empty, ".", or "..", and does not contain "/".
func validateChannelName(field, name string) error {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") {
		return merr.WrapErrServiceInternalMsg("invalid %s %q", field, name)
	}
	return nil
}

// validatePChannel checks that a pchannel name is a valid channel name and not
// a vchannel name. A vchannel name would claim a key that no flush checks.
func validatePChannel(name string) error {
	if err := validateChannelName("pchannel", name); err != nil {
		return err
	}
	if !funcutil.IsPhysicalChannel(name) {
		return merr.WrapErrServiceInternalMsg("pchannel %q is not a pchannel name", name)
	}
	return nil
}

// validateTerm checks that a pchannel term is positive.
func validateTerm(term int64) error {
	if term <= 0 {
		return merr.WrapErrServiceInternalMsg("pchannel_term must be positive, got %d", term)
	}
	return nil
}

// toMerr translates a manifest error at the service boundary. A fenced caller
// no longer owns the channel and gets a code that is not retryable. Every
// other error translates by its pkerr category.
func toMerr(err error, channel string) error {
	if errors.Is(err, manifest.ErrFenced) {
		return errors.WithSecondaryError(merr.WrapErrChannelNotAvailable(channel, "pchannel term is fenced"), err)
	}
	return pkerr.ToMerr(err)
}
