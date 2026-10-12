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
	"sync/atomic"

	"github.com/cockroachdb/errors"

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

// toMerr translates a manifest error at the service boundary. A fenced caller
// no longer owns the vchannel and gets a code that is not retryable. Every
// other error translates by its pkerr category.
func toMerr(err error, vchannel string) error {
	if errors.Is(err, manifest.ErrFenced) {
		return merr.WrapErrChannelNotAvailable(vchannel, err.Error())
	}
	return pkerr.ToMerr(err)
}
