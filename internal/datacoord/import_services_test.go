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

package datacoord

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ================================
// Import Services Test Suite
// ================================

type ImportServicesSuite struct {
	suite.Suite
}

func TestImportServicesSuite(t *testing.T) {
	suite.Run(t, new(ImportServicesSuite))
}

// --------------------------------
// ImportV2 Tests
// --------------------------------

func (s *ImportServicesSuite) TestImportV2_ServerNotHealthyReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Initializing)

	resp, err := server.ImportV2(ctx, nil)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceNotReady))
}

func (s *ImportServicesSuite) TestImportV2_InvalidTimeoutReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "invalid_format"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
}

func (s *ImportServicesSuite) TestImportV2_InvalidBinlogPathsAreNotRetryable() {
	paramtable.Init()
	for _, paths := range [][]string{nil, {"insert", "delta", "extra"}} {
		for _, test := range []struct {
			name  string
			files []*internalpb.ImportFile
		}{
			{"single", []*internalpb.ImportFile{{Paths: paths}}},
			{"invalid_first", []*internalpb.ImportFile{{Paths: paths}, {Paths: []string{"valid"}}}},
			{"invalid_last", []*internalpb.ImportFile{{Paths: []string{"valid"}}, {Paths: paths}}},
		} {
			s.Run(fmt.Sprintf("paths_%d/%s", len(paths), test.name), func() {
				server := &Server{meta: &meta{}}
				server.stateCode.Store(commonpb.StateCode_Healthy)
				// A supplied job ID skips allocation; a nil chunk manager proves
				// invalid path counts are rejected before accessing object storage.
				resp, err := server.ImportV2(context.Background(), &internalpb.ImportRequestInternal{
					JobID:   1,
					Files:   test.files,
					Options: []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}},
				})
				s.Require().NoError(err)
				s.Require().NotNil(resp)
				status := resp.GetStatus()
				s.Equal(merr.Code(merr.ErrImportFailed), status.GetCode())
				s.False(status.GetRetriable())
				s.ErrorIs(merr.Error(status), merr.ErrImportFailed)
				s.Equal(merr.InputError, merr.GetErrorType(merr.Error(status)))
			})
		}
	}
}

func (s *ImportServicesSuite) TestImportV2_L0ImportDisabledReturnsError() {
	paramtable.Init()
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	// enableL0Import defaults to false, so an l0_import request must be rejected
	// before reaching allocation (allocator is nil here, proving the early reject).
	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
			{Key: "l0_import", Value: "true"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrImportFailed))
	s.Contains(resp.GetStatus().GetReason(), "l0 import is disabled")
}

func (s *ImportServicesSuite) TestImportV2_L0ImportEnabledPassesGate() {
	paramtable.Init()
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	params := paramtable.Get()
	params.Save(params.DataCoordCfg.EnableL0Import.Key, "true")
	defer params.Reset(params.DataCoordCfg.EnableL0Import.Key)

	// With enableL0Import=true the same request must get past the L0 gate and
	// fail later on the nil allocator instead — proof the gate was skipped.
	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
			{Key: "l0_import", Value: "true"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
	s.Contains(resp.GetStatus().GetReason(), "allocator not initialized")
}

func (s *ImportServicesSuite) TestImportV2_AllocatorNilReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	server.allocator = nil

	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
	s.Contains(resp.GetStatus().GetReason(), "allocator not initialized")
}

func (s *ImportServicesSuite) TestImportV2_AllocatorFailsReturnsError() {
	ctx := context.Background()
	server := &Server{}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), merr.WrapErrServiceUnavailable("allocation failed"))
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
	s.Contains(resp.GetStatus().GetReason(), "failed to allocate job ID")
}

func (s *ImportServicesSuite) TestImportV2_BroadcastFailsReturnsError() {
	ctx := context.Background()

	// Mock validation to pass but broadcast to fail
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancerInst := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancerInst, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock broker.DescribeCollectionInternal (called once in startBroadcastWithCollectionID, which will fail at StartBroadcastWithResourceKeys)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil)

	// Mock StartBroadcastWithResourceKeys to fail
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return nil, merr.WrapErrServiceUnavailable("broadcast failed")
		}).Build()
	defer mockBroadcast.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
		meta:       newTestMetaWithChunkManager(s.T()),
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1001), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema: &schemapb.CollectionSchema{
			Name:   "test_collection",
			DbName: "test_db",
		},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable))
	s.Contains(resp.GetStatus().GetReason(), "broadcast")
}

func (s *ImportServicesSuite) TestImportV2_SuccessReturnsJobID() {
	ctx := context.Background()

	// Mock validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancerInst := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancerInst, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock StartBroadcastWithResourceKeys to succeed
	mockBroadcastAPI := newMockBroadcastAPIImpl()
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockBroadcastAPI, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	// Mock broker: DescribeCollectionInternal is called twice
	// First call in startBroadcastWithCollectionID, second call in broadcastImport
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil).Times(2)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
		meta:       newTestMetaWithChunkManager(s.T()),
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1001), nil)
	server.allocator = mockAllocator

	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema: &schemapb.CollectionSchema{
			Name:   "test_collection",
			DbName: "test_db",
		},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	resp, err := server.ImportV2(ctx, req)

	s.NoError(err)
	s.NotNil(resp)
	s.Equal(int32(0), resp.GetStatus().GetCode())
	s.Equal("1000", resp.GetJobID())
}

// importV2RequestFilePath is the single file newImportV2IdempotentRequest asks to
// import.
const importV2RequestFilePath = "/test/file.json"

// importV2RequestCollectionID is the collection newImportV2IdempotentRequest targets.
const importV2RequestCollectionID = int64(100)

// newDuplicatedImportBroadcastResult builds the result the broadcaster returns on an
// idempotency-key hit: no append results, plus the original broadcast message, which
// carries the original jobID, the collection that job targeted, and the files it was
// created from. The collectionID must match the request's, or the duplicate is rejected
// as belonging to another collection -- see newDuplicatedImportBroadcastResultForCollection.
func newDuplicatedImportBroadcastResult(originalJobID int64, originalPaths ...string) *types.BroadcastAppendResult {
	return newDuplicatedImportBroadcastResultForCollection(originalJobID, importV2RequestCollectionID, originalPaths...)
}

func newDuplicatedImportBroadcastResultForCollection(originalJobID int64, originalCollectionID int64, originalPaths ...string) *types.BroadcastAppendResult {
	duplicated := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{
			JobID:        originalJobID,
			CollectionID: originalCollectionID,
			Files: lo.Map(originalPaths, func(path string, _ int) *msgpb.ImportFile {
				return &msgpb.ImportFile{Paths: []string{path}}
			}),
		}).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(originalCollectionID, "run-1")).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	return &types.BroadcastAppendResult{
		BroadcastID: 12345,
		Duplicated:  duplicated,
	}
}

// setupImportV2DuplicateBroadcast wires the mocks ImportV2 needs so that the broadcast
// comes back deduplicated, and returns the server under test together with the
// broadcast mock, so a caller can inspect the message that was actually broadcast.
// importMeta is supplied by the caller so it can decide whether the original job still
// exists.
func (s *ImportServicesSuite) setupImportV2DuplicateBroadcast(importMeta ImportMeta, originalJobID int64, originalPaths ...string) (*Server, *mockBroadcastAPIImpl) {
	mockBalancerInst := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancerInst, nil
	}).Build()
	s.T().Cleanup(func() { mockBalance.UnPatch() })

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	s.T().Cleanup(func() { mockAssignment.UnPatch() })

	mockBroadcastAPI := newMockBroadcastAPIImpl()
	mockBroadcastAPI.broadcastResult = newDuplicatedImportBroadcastResult(originalJobID, originalPaths...)
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockBroadcastAPI, nil
		}).Build()
	s.T().Cleanup(func() { mockBroadcast.UnPatch() })

	mockBroker := broker.NewMockBroker(s.T())
	// Maybe rather than Times(2): a request rejected by validateImportRequest -- the
	// job-count limit, say -- returns before either describe call.
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil).Maybe()

	server := &Server{
		importMeta: importMeta,
		broker:     mockBroker,
		meta:       newTestMetaWithChunkManager(s.T()),
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1001), nil)
	server.allocator = mockAllocator
	return server, mockBroadcastAPI
}

func newImportV2IdempotentRequest() *internalpb.ImportRequestInternal {
	return &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema: &schemapb.CollectionSchema{
			Name:   "test_collection",
			DbName: "test_db",
		},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{importV2RequestFilePath}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}
}

// newImportV2IdempotentContext carries the client key the way a real call does:
// in the gRPC incoming metadata, not in the request body.
func newImportV2IdempotentContext(key string) context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(util.HeaderIdempotencyKey, key))
}

// A retry whose idempotency key still resolves must get the ORIGINAL jobID back,
// not the freshly allocated one (1000 here, which stays unused).
func (s *ImportServicesSuite) TestImportV2_DuplicateReturnsOriginalJobID() {
	ctx := newImportV2IdempotentContext("run-1")

	importMeta := NewMockImportMeta(s.T())

	// validateImportRequest runs before the broadcaster's idempotency lookup, so even a
	// request that will deduplicate is counted against the job limit first.
	importMeta.EXPECT().CountJobBy(mock.Anything, mock.Anything).Return(0)
	// The duplicate branch logs the original job's state, so it looks the job up.
	// nil is the "gone" case: the key outlived the job's own metadata retention.
	importMeta.EXPECT().GetJob(mock.Anything, int64(4242)).Return(nil).Once()

	server, _ := s.setupImportV2DuplicateBroadcast(importMeta, 4242, importV2RequestFilePath)

	resp, err := server.ImportV2(ctx, newImportV2IdempotentRequest())

	s.NoError(err)
	s.NotNil(resp)
	s.Equal(int32(0), resp.GetStatus().GetCode())
	s.Equal("4242", resp.GetJobID())
}

// The duplicate branch logs the ORIGINAL job's state, not just its ID. A key whose
// original ended Failed resolves to that same job for the rest of the window, so a
// client retrying it never makes progress; without the state in the log that is
// indistinguishable from a key waiting on a healthy job. GetJob is expected exactly
// once, so this asserts the lookup actually runs rather than assuming it.
func (s *ImportServicesSuite) TestImportV2_DuplicateLooksUpAFailedOriginal() {
	ctx := newImportV2IdempotentContext("run-1")

	importMeta := NewMockImportMeta(s.T())
	importMeta.EXPECT().CountJobBy(mock.Anything, mock.Anything).Return(0)
	importMeta.EXPECT().GetJob(mock.Anything, int64(4242)).Return(&importJob{
		ImportJob: &datapb.ImportJob{JobID: 4242, State: internalpb.ImportJobState_Failed},
	}).Once()

	server, _ := s.setupImportV2DuplicateBroadcast(importMeta, 4242, importV2RequestFilePath)

	resp, err := server.ImportV2(ctx, newImportV2IdempotentRequest())

	s.NoError(err)
	s.Equal(int32(0), resp.GetStatus().GetCode())
	// A failed original still resolves to its own jobID: the key names an attempt that
	// did happen. Recovering from it is the client's call, which is why the state is logged.
	s.Equal("4242", resp.GetJobID())
}

// A duplicate is reported by an explicit flag, never by a non-zero jobID, so a
// duplicated broadcast carrying jobID 0 must still take the duplicate branch and
// return that 0 — not fall through to the freshly allocated 1000.
func (s *ImportServicesSuite) TestImportV2_DuplicateWithZeroJobIDStaysDuplicate() {
	ctx := newImportV2IdempotentContext("run-1")

	importMeta := NewMockImportMeta(s.T())

	// validateImportRequest runs before the broadcaster's idempotency lookup, so even a
	// request that will deduplicate is counted against the job limit first.
	importMeta.EXPECT().CountJobBy(mock.Anything, mock.Anything).Return(0)
	// The duplicate branch logs the original job's state, so it looks the job up.
	// nil is the "gone" case: the key outlived the job's own metadata retention.
	importMeta.EXPECT().GetJob(mock.Anything, int64(0)).Return(nil).Once()

	server, _ := s.setupImportV2DuplicateBroadcast(importMeta, 0, importV2RequestFilePath)

	resp, err := server.ImportV2(ctx, newImportV2IdempotentRequest())

	s.NoError(err)
	s.NotNil(resp)
	s.Equal(int32(0), resp.GetStatus().GetCode())
	s.NotEqual("1000", resp.GetJobID())
	s.Equal("0", resp.GetJobID())
}

func (s *ImportServicesSuite) TestImportV2_ForwardsIdempotencyKeyUnmodifiedAtTheLimit() {
	importMeta := NewMockImportMeta(s.T())

	// validateImportRequest runs before the broadcaster's idempotency lookup, so even a
	// request that will deduplicate is counted against the job limit first.
	importMeta.EXPECT().CountJobBy(mock.Anything, mock.Anything).Return(0)
	// The duplicate branch logs the original job's state, so it looks the job up.
	// nil is the "gone" case: the key outlived the job's own metadata retention.
	importMeta.EXPECT().GetJob(mock.Anything, int64(4242)).Return(nil).Once()

	server, broadcastAPI := s.setupImportV2DuplicateBroadcast(importMeta, 4242, importV2RequestFilePath)

	limit := paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.GetAsInt()
	s.Equal(256, limit, "this test asserts the boundary of the DEFAULT limit, the one advertised to clients")
	atLimit := strings.Repeat("k", limit)

	ctx := newImportV2IdempotentContext(atLimit)
	resp, err := server.ImportV2(ctx, newImportV2IdempotentRequest())

	s.NoError(err)
	s.Equal(int32(0), resp.GetStatus().GetCode())

	s.Require().NotNil(broadcastAPI.capturedMsg)
	forwarded := message.IdempotencyKeyOf(broadcastAPI.capturedMsg)
	// DataCoord must hand the client key through untouched. It is scoped on the way --
	// that is the mechanism, and the broadcaster bounds the client portion, not the
	// encoded value -- but nothing may alter, truncate or re-prefix the bytes the
	// client sent, or a key sitting on the advertised limit would stop deduplicating.
	s.Equal(atLimit, forwarded.ClientKey())
	s.Equal(message.NewCollectionScopedIdempotencyKey(importV2RequestCollectionID, atLimit), forwarded)
}

func (s *ImportServicesSuite) TestImportV2_UsesDefaultDbNameWhenEmpty() {
	ctx := context.Background()

	// Mock validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancerInst := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancerInst, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Capture the dbName passed to broadcastImport
	var capturedDbName string
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			// Check if default db name resource key is used
			for _, key := range keys {
				if key.String() != "" {
					capturedDbName = "default" // This indicates default db was used
				}
			}
			return nil, errors.New("stop here for test")
		}).Build()
	defer mockBroadcast.UnPatch()

	// Mock broker.DescribeCollectionInternal to return empty dbName (called in startBroadcastWithCollectionID)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "", // Empty - should use default
		CollectionName: "test_collection",
	}, nil)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
		meta:       newTestMetaWithChunkManager(s.T()),
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)

	mockAllocator := allocator.NewMockAllocator(s.T())
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1001), nil)
	server.allocator = mockAllocator

	// Request with empty DbName in schema (not used anymore, broker is the source of truth)
	req := &internalpb.ImportRequestInternal{
		CollectionID:   100,
		CollectionName: "test_collection",
		PartitionIDs:   []int64{1},
		ChannelNames:   []string{"v1"},
		Schema: &schemapb.CollectionSchema{
			Name:   "test_collection",
			DbName: "", // Empty - not used anymore
		},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file.json"}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		},
	}

	server.ImportV2(ctx, req)

	// Verify that default db name was used (test stops at broadcast mock)
	s.Equal("default", capturedDbName)
}

// --------------------------------
// createImportJobFromAck Tests
// --------------------------------

func (s *ImportServicesSuite) TestCreateImportJobFromAck() {
	paramtable.Init()
	type ackAllocator struct{ allocator.Allocator }
	type ackHandler struct{ Handler }
	type ackCatalog struct{ metastore.DataCoordCatalog }

	for _, tc := range []struct {
		name    string
		wantErr error
	}{
		{"server_not_healthy", merr.ErrServiceNotReady},
		{"invalid_timeout", merr.ErrImportFailed},
		{"allocator_failure", merr.ErrServiceUnavailable},
		{"collection_not_found", merr.ErrCollectionNotFound},
		{"collection_nil", merr.ErrCollectionNotFound},
		{"collection_unavailable", merr.ErrServiceNotReady},
		{"save_job_failure", merr.ErrServiceUnavailable},
		{"provided_job_id", nil},
		{"allocated_job_id", nil},
		{"assign_file_ids", nil},
		{"nil_schema", nil},
		{"l0_disabled", nil},
		{"l0_enabled", nil},
	} {
		s.Run(tc.name, func() {
			ctx := context.Background()
			body := &msgpb.ImportMsg{
				DbName: "test_db", CollectionID: 100, CollectionName: "test_collection",
				PartitionIDs: []int64{1}, JobID: 2000,
				Schema:  &schemapb.CollectionSchema{Name: "test_collection", DbName: "stale_db"},
				Files:   []*msgpb.ImportFile{{Id: 99, Paths: []string{"/test/file.json"}}},
				Options: map[string]string{"timeout": "300s", "auto_commit": "false"},
			}
			coll := &collectionInfo{ID: 100, VChannelNames: []string{"v1", "v2"}}
			var allocErr, collectionErr, saveErr error
			switch tc.name {
			case "invalid_timeout":
				body.Options["timeout"] = "invalid_format"
			case "allocator_failure":
				allocErr = tc.wantErr
			case "collection_not_found", "collection_unavailable":
				coll, collectionErr = nil, tc.wantErr
			case "collection_nil":
				coll = nil
			case "save_job_failure":
				saveErr = tc.wantErr
			case "allocated_job_id":
				body.JobID = 0
			case "assign_file_ids":
				body.Files = append(body.Files,
					&msgpb.ImportFile{Paths: []string{"/test/file2.json"}},
					&msgpb.ImportFile{Paths: []string{"/test/file3.json"}})
			case "nil_schema":
				body.Schema = nil
			case "l0_disabled", "l0_enabled":
				body.Options["l0_import"] = "true"
			}
			params := paramtable.Get()
			s.Require().NoError(params.Save(params.DataCoordCfg.EnableL0Import.Key, fmt.Sprint(tc.name == "l0_enabled")))
			defer params.Reset(params.DataCoordCfg.EnableL0Import.Key)

			allocate := mockey.Mock((*ackAllocator).AllocN).To(func(_ *ackAllocator, n int64) (int64, int64, error) {
				s.Equal(int64(len(body.Files)+1), n)
				return 1000, 1000 + n, allocErr
			}).Build()
			defer allocate.UnPatch()
			get := mockey.Mock((*ackHandler).GetCollection).To(func(_ *ackHandler, _ context.Context, id int64) (*collectionInfo, error) {
				s.Equal(body.CollectionID, id)
				return coll, collectionErr
			}).Build()
			defer get.UnPatch()
			var saved *datapb.ImportJob
			save := mockey.Mock((*ackCatalog).SaveImportJob).To(func(_ *ackCatalog, _ context.Context, job *datapb.ImportJob) error {
				if saveErr == nil {
					saved = job
				}
				return saveErr
			}).Build()
			defer save.UnPatch()
			server := &Server{
				allocator: &ackAllocator{}, handler: &ackHandler{},
				importMeta: &importMeta{jobs: make(map[int64]ImportJob), catalog: &ackCatalog{}},
			}
			server.stateCode.Store(commonpb.StateCode_Healthy)

			control := funcutil.GetControlChannel("import-test")
			wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{}).
				WithBody(body).WithBroadcast([]string{"v1", "v2", control}).MustBuildBroadcast()
			result := message.BroadcastResultImportMessageV1{
				Message: message.MustAsBroadcastImportMessageV1(wal),
				Results: map[string]*message.AppendResult{
					"v1": {TimeTick: 100}, "v2": {TimeTick: 200}, control: {TimeTick: 300},
				},
			}
			if tc.name == "server_not_healthy" {
				server.stateCode.Store(commonpb.StateCode_Initializing)
				result = message.BroadcastResultImportMessageV1{}
			}
			resp, err := server.createImportJobFromAck(ctx, result)
			if tc.wantErr != nil {
				s.ErrorIs(merr.CheckRPCCall(resp, err), tc.wantErr)
				s.Empty(resp.GetJobID())
				s.Nil(saved)
				if tc.name == "allocator_failure" {
					s.Contains(resp.GetStatus().GetReason(), "alloc id failed")
				}
				if tc.name == "save_job_failure" {
					s.Contains(resp.GetStatus().GetReason(), "add import job failed")
				}
				return
			}
			s.Require().NoError(merr.CheckRPCCall(resp, err))
			s.Require().NotNil(saved)
			wantID := body.JobID
			if wantID == 0 {
				wantID = 1000
			}
			s.Equal(fmt.Sprint(wantID), resp.JobID)
			s.Equal(wantID, saved.JobID)
			s.Equal(body.CollectionID, saved.CollectionID)
			s.Equal(body.CollectionName, saved.CollectionName)
			s.Equal(body.PartitionIDs, saved.PartitionIDs)
			s.ElementsMatch([]string{"v1", "v2"}, saved.Vchannels)
			s.ElementsMatch([]string{"v1", "v2"}, saved.ReadyVchannels)
			s.Equal(uint64(300), saved.DataTs, "the control channel still contributes to the maximum tick")
			s.False(saved.AutoCommit)
			s.Equal(body.Options, funcutil.KeyValuePair2Map(saved.Options))
			if body.Schema == nil {
				s.Nil(saved.Schema)
			} else {
				s.Equal(body.DbName, saved.Schema.DbName)
				s.Equal(body.Schema.Name, saved.Schema.Name)
			}
			s.Require().Len(saved.Files, len(body.Files))
			for i, file := range saved.Files {
				s.Equal(int64(1001+i), file.Id)
				s.Equal(body.Files[i].Paths, file.Paths)
			}
			if tc.name == "l0_disabled" {
				s.Equal(internalpb.ImportJobState_Failed, saved.State)
				s.Contains(saved.Reason, "l0 import is disabled")
				s.NotEqual(uint64(math.MaxUint64), saved.CleanupTs)
			} else {
				s.Equal(internalpb.ImportJobState_Pending, saved.State)
			}
		})
	}
}

// Helper types are defined in import_callbacks_test.go (mockBalancerImpl, mockBroadcastAPIImpl, newMockBroadcastAPIImpl)

// The dedup scope carries the collection's ID, so a duplicate resolving to another
// collection is unreachable through the normal path: dropping db1.c1 and recreating a
// same-named db1.c1 changes the ID, hence the scope, hence the lookup misses and a
// fresh job is created. This pins the invariant behind that reasoning at the ImportV2
// level -- if an encoding or scoping bug ever did hand back another collection's
// broadcast, the request must fail rather than return that jobID. Returning it would be
// silent and unrecoverable: checkCollection leaves a Completed job alone when its
// collection vanishes and GetImportProgress does not re-check collection existence, so
// the client would poll that jobID and read Completed while the new collection stays
// empty.
func (s *ImportServicesSuite) TestImportV2_DuplicateFromAnotherCollectionIsRejected() {
	importMeta := NewMockImportMeta(s.T())
	importMeta.EXPECT().CountJobBy(mock.Anything, mock.Anything).Return(0)

	server, broadcastAPI := s.setupImportV2DuplicateBroadcast(importMeta, 4242, importV2RequestFilePath)
	// The original broadcast targeted collection 99; the request targets 100.
	broadcastAPI.broadcastResult = newDuplicatedImportBroadcastResultForCollection(4242, 99, importV2RequestFilePath)

	ctx := newImportV2IdempotentContext("run-1")
	resp, err := server.ImportV2(ctx, newImportV2IdempotentRequest())

	s.NoError(err)
	s.NotEqual(int32(0), resp.GetStatus().GetCode())
	s.Contains(resp.GetStatus().GetReason(), "idempotency scope resolved to an import into collection 99")
	s.NotEqual("4242", resp.GetJobID(), "another collection's jobID must not be handed back")
}

// The documented edge of the retry contract. Everything datacoord validates runs before
// the broadcaster's idempotency lookup, and the job-count limit is one of those checks,
// so a retry sent while dataCoord.import.maxImportJobNum is saturated -- by the original
// job among others -- is rejected before it can resolve. The broadcast never happens, so
// no duplicate import is created either; retrying the same key once a slot frees up
// resolves normally. This is pinned as a test rather than left to chance, because a
// client that answers the rejection by minting a fresh key is what imports twice.
func (s *ImportServicesSuite) TestImportV2_DuplicateIsRejectedWhileJobLimitIsReached() {
	old := paramtable.Get().DataCoordCfg.MaxImportJobNum.SwapTempValue("1")
	defer paramtable.Get().DataCoordCfg.MaxImportJobNum.SwapTempValue(old)

	importMeta := NewMockImportMeta(s.T())
	importMeta.EXPECT().CountJobBy(mock.Anything, mock.Anything).Return(1)

	server, broadcastAPI := s.setupImportV2DuplicateBroadcast(importMeta, 4242, importV2RequestFilePath)

	ctx := newImportV2IdempotentContext("run-1")
	resp, err := server.ImportV2(ctx, newImportV2IdempotentRequest())

	s.NoError(err)
	s.NotEqual(int32(0), resp.GetStatus().GetCode())
	s.Contains(resp.GetStatus().GetReason(), "number of jobs has reached the limit")
	s.Nil(broadcastAPI.capturedMsg, "the request must be rejected before it reaches the broadcaster")
}

// The other half: the limit still applies to a request that actually creates a job.
func (s *ImportServicesSuite) TestImportV2_JobLimitStillRejectsANewJob() {
	old := paramtable.Get().DataCoordCfg.MaxImportJobNum.SwapTempValue("1")
	defer paramtable.Get().DataCoordCfg.MaxImportJobNum.SwapTempValue(old)

	importMeta := NewMockImportMeta(s.T())
	importMeta.EXPECT().CountJobBy(mock.Anything, mock.Anything).Return(1)

	server, broadcastAPI := s.setupImportV2DuplicateBroadcast(importMeta, 4242, importV2RequestFilePath)
	// This key resolves to nothing, so the broadcast would create a new task.
	broadcastAPI.broadcastResult = &types.BroadcastAppendResult{BroadcastID: 12345}

	ctx := newImportV2IdempotentContext("run-2")
	resp, err := server.ImportV2(ctx, newImportV2IdempotentRequest())

	s.NoError(err)
	s.NotEqual(int32(0), resp.GetStatus().GetCode())
	s.Contains(resp.GetStatus().GetReason(), "number of jobs has reached the limit")
}
