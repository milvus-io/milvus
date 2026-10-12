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
	"github.com/milvus-io/milvus/internal/pkindex/coordinator/manifestmanager"
	"github.com/milvus-io/milvus/internal/pkindex/core/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Service implements PKIndexCoordService. It can be registered on a gRPC
// server before its manager exists; it answers ServiceNotReady until
// SetManager is called.
type Service struct {
	mgr atomic.Pointer[manifestmanager.Manager]
}

var _ pkindexpb.PKIndexCoordServiceServer = (*Service)(nil)

// New returns a Service that has no manager.
func New() *Service { return &Service{} }

// SetManager sets the manager that serves the requests.
func (s *Service) SetManager(m *manifestmanager.Manager) { s.mgr.Store(m) }

func (s *Service) manager() (*manifestmanager.Manager, error) {
	if m := s.mgr.Load(); m != nil {
		return m, nil
	}
	return nil, merr.WrapErrServiceNotReadyMsg("primary key index manifest service is not ready")
}

// ClaimManifest claims a vchannel for a pchannel term and returns the
// manifest after the claim.
func (s *Service) ClaimManifest(ctx context.Context, req *pkindexpb.ClaimManifestRequest) (*pkindexpb.ClaimManifestResponse, error) {
	if err := validateTarget(req.GetCollectionId(), req.GetVchannel()); err != nil {
		return &pkindexpb.ClaimManifestResponse{Status: merr.Status(err)}, nil
	}
	if t := req.GetPkDataType(); t != schemapb.DataType_Int64 && t != schemapb.DataType_VarChar {
		return &pkindexpb.ClaimManifestResponse{Status: merr.Status(
			merr.WrapErrParameterInvalidMsg("pk_data_type must be Int64 or VarChar, got %s", t))}, nil
	}
	m, err := s.manager()
	if err != nil {
		return &pkindexpb.ClaimManifestResponse{Status: merr.Status(err)}, nil
	}
	got, err := m.Claim(ctx, manifest.Claim{
		CollectionID: req.GetCollectionId(),
		VChannel:     req.GetVchannel(),
		PChannelTerm: req.GetPchannelTerm(),
		PKDataType:   req.GetPkDataType(),
	})
	if err != nil {
		return &pkindexpb.ClaimManifestResponse{Status: merr.Status(toMerr(err, req.GetVchannel()))}, nil
	}
	return &pkindexpb.ClaimManifestResponse{Status: merr.Success(), Manifest: got}, nil
}

// CommitFlush adds the uploaded SSTs of a flush and returns the manifest
// after the commit.
func (s *Service) CommitFlush(ctx context.Context, req *pkindexpb.CommitFlushRequest) (*pkindexpb.CommitFlushResponse, error) {
	if err := validateTarget(req.GetCollectionId(), req.GetVchannel()); err != nil {
		return &pkindexpb.CommitFlushResponse{Status: merr.Status(err)}, nil
	}
	if req.GetCheckpointTimeTick() == 0 {
		return &pkindexpb.CommitFlushResponse{Status: merr.Status(
			merr.WrapErrParameterInvalidMsg("checkpoint_time_tick must not be 0"))}, nil
	}
	m, err := s.manager()
	if err != nil {
		return &pkindexpb.CommitFlushResponse{Status: merr.Status(err)}, nil
	}
	got, err := m.CommitFlush(ctx, req.GetCollectionId(), req.GetVchannel(), manifest.Flush{
		PChannelTerm:       req.GetPchannelTerm(),
		AddSSTs:            req.GetAddSsts(),
		CheckpointTimeTick: req.GetCheckpointTimeTick(),
	})
	if err != nil {
		return &pkindexpb.CommitFlushResponse{Status: merr.Status(toMerr(err, req.GetVchannel()))}, nil
	}
	return &pkindexpb.CommitFlushResponse{Status: merr.Success(), Manifest: got}, nil
}

// GetManifest returns the current manifest of a vchannel. Found is false and
// the manifest is empty when the vchannel has no manifest.
func (s *Service) GetManifest(ctx context.Context, req *pkindexpb.GetManifestRequest) (*pkindexpb.GetManifestResponse, error) {
	if err := validateTarget(req.GetCollectionId(), req.GetVchannel()); err != nil {
		return &pkindexpb.GetManifestResponse{Status: merr.Status(err)}, nil
	}
	m, err := s.manager()
	if err != nil {
		return &pkindexpb.GetManifestResponse{Status: merr.Status(err)}, nil
	}
	got, found, err := m.Get(ctx, req.GetCollectionId(), req.GetVchannel())
	if err != nil {
		return &pkindexpb.GetManifestResponse{Status: merr.Status(toMerr(err, req.GetVchannel()))}, nil
	}
	return &pkindexpb.GetManifestResponse{Status: merr.Success(), Found: found, Manifest: got}, nil
}

// validateTarget checks that a request names a collection and a vchannel that
// can form a manifest key: a positive collection ID and a vchannel name that
// is not empty, ".", or "..", and does not contain "/".
func validateTarget(collectionID int64, vchannel string) error {
	if collectionID <= 0 {
		return merr.WrapErrParameterInvalidMsg("collection_id must be positive, got %d", collectionID)
	}
	if vchannel == "" || vchannel == "." || vchannel == ".." || strings.Contains(vchannel, "/") {
		return merr.WrapErrParameterInvalidMsg("invalid vchannel %q", vchannel)
	}
	return nil
}

// toMerr translates a manifest error at the service boundary. A fenced caller
// no longer owns the vchannel and gets a code that is not retryable. Every
// other error translates by its pkerr category.
func toMerr(err error, vchannel string) error {
	if errors.Is(err, manifest.ErrFenced) {
		return errors.WithSecondaryError(merr.WrapErrChannelNotAvailable(vchannel, "pchannel term is fenced"), err)
	}
	return pkerr.ToMerr(err)
}
