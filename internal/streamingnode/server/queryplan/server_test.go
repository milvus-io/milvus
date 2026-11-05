//go:build test && dynamic

package queryplan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const testVChannel = "by-dev-rootcoord-dml_0_100v0"

type fakeWALManager struct {
	pchannel string
	channel  types.PChannelInfo
	wal      wal.WAL
	err      error
}

func (m *fakeWALManager) GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	m.channel = channel
	return m.wal, m.err
}

func (m *fakeWALManager) GetAvailableRawWALByPChannel(pchannel string) (wal.WAL, error) {
	m.pchannel = pchannel
	return m.wal, m.err
}

type fakeProviderWAL struct {
	wal.WAL
	inlineResponse *viewpb.GrowingInlineResponse
	plan           *viewpb.QueryPlan
	err            error
	mvcc           *viewpb.QueryPlanMVCC
}

func (w *fakeProviderWAL) GetQueryPlan(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	return &viewpb.GetQueryPlanResponse{Plan: w.plan, GrowingInlineResponse: w.inlineResponse}, w.err
}

func (w *fakeProviderWAL) GetMVCCTimestamp(context.Context, *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	if w.err != nil {
		return nil, w.err
	}
	return &viewpb.GetMVCCTimestampResponse{Mvcc: w.mvcc}, nil
}

type wrappedProviderWAL struct {
	wal.WAL
	raw wal.WAL
}

func (w wrappedProviderWAL) UnwrapWAL() wal.WAL {
	return w.raw
}

func TestServerGetQueryPlanDelegatesToLocalWALProvider(t *testing.T) {
	manager := &fakeWALManager{wal: &fakeProviderWAL{plan: &viewpb.QueryPlan{Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 99}}}}
	server := NewServer(manager)

	resp, err := server.GetQueryPlan(newIncomingPChannelContext("by-dev-rootcoord-dml_0"), &viewpb.GetQueryPlanRequest{
		ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: testVChannel},
	})

	require.NoError(t, err)
	assert.Equal(t, uint64(100), resp.GetPlan().GetMvcc().GetGrowingTimetick())
	assert.Equal(t, uint64(99), resp.GetPlan().GetMvcc().GetTransformingTimetick())
	assert.Equal(t, types.PChannelInfo{Name: "by-dev-rootcoord-dml_0", Term: 7, AccessMode: types.AccessModeRW}, manager.channel)
}

func TestServerGetQueryPlanDelegatesToWrappedLocalWALProvider(t *testing.T) {
	raw := &fakeProviderWAL{
		plan: &viewpb.QueryPlan{Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 99}},
		inlineResponse: &viewpb.GrowingInlineResponse{Response: &viewpb.GrowingInlineResponse_StatisticsResponse{
			StatisticsResponse: &viewpb.StatisticsOnViewResponse{},
		}},
	}
	manager := &fakeWALManager{wal: wrappedProviderWAL{WAL: raw, raw: raw}}
	server := NewServer(manager)

	resp, err := server.GetQueryPlan(newIncomingPChannelContext("by-dev-rootcoord-dml_0"), &viewpb.GetQueryPlanRequest{
		ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: testVChannel},
	})

	require.NoError(t, err)
	require.Same(t, raw.inlineResponse, resp.GetGrowingInlineResponse())
	require.NotNil(t, resp.GetGrowingInlineResponse().GetStatisticsResponse())
	assert.Equal(t, uint64(100), resp.GetPlan().GetMvcc().GetGrowingTimetick())
	assert.Equal(t, uint64(99), resp.GetPlan().GetMvcc().GetTransformingTimetick())
	assert.Equal(t, types.PChannelInfo{Name: "by-dev-rootcoord-dml_0", Term: 7, AccessMode: types.AccessModeRW}, manager.channel)
}

func TestServerGetMVCCTimestampDelegatesToLocalWALProvider(t *testing.T) {
	manager := &fakeWALManager{wal: &fakeProviderWAL{mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 100}}}
	server := NewServer(manager)

	resp, err := server.GetMVCCTimestamp(newIncomingPChannelContext("by-dev-rootcoord-dml_0"), &viewpb.GetMVCCTimestampRequest{
		Vchannel: testVChannel,
	})

	require.NoError(t, err)
	assert.Equal(t, uint64(101), resp.GetMvcc().GetGrowingTimetick())
	assert.Equal(t, uint64(100), resp.GetMvcc().GetTransformingTimetick())
	assert.Equal(t, types.PChannelInfo{Name: "by-dev-rootcoord-dml_0", Term: 7, AccessMode: types.AccessModeRW}, manager.channel)
}

func TestServerGetQueryPlanPreservesViewError(t *testing.T) {
	manager := &fakeWALManager{wal: &fakeProviderWAL{err: viewerror.NewViewNotFound("missing view")}}
	server := NewServer(manager)

	_, err := server.GetQueryPlan(newIncomingPChannelContext("by-dev-rootcoord-dml_0"), &viewpb.GetQueryPlanRequest{
		ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: testVChannel},
	})

	require.Error(t, err)
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
}

func TestServerGetQueryPlanProjectsReleasedLoadInfoAsViewInvalidated(t *testing.T) {
	loadInfoErr := merr.CheckRPCCall(merr.Status(merr.WrapErrCollectionNotLoaded(100)), nil)
	require.ErrorIs(t, loadInfoErr, merr.ErrCollectionNotLoaded)
	planErr := merr.Wrap(loadInfoErr, "get sealed BM25 resources for data version 6/5")
	manager := &fakeWALManager{wal: &fakeProviderWAL{err: planErr}}
	server := NewServer(manager)

	_, err := server.GetQueryPlan(newIncomingPChannelContext("by-dev-rootcoord-dml_0"), &viewpb.GetQueryPlanRequest{
		ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: testVChannel},
	})

	require.Error(t, err)
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
	viewErr := viewerror.AsViewError(err)
	require.True(t, viewErr.IsViewInvalidated())
	require.True(t, viewErr.IsRetryable())
	require.Contains(t, viewErr.Cause, "get sealed BM25 resources for data version 6/5")
	require.Contains(t, viewErr.Cause, "collection not loaded")
}

func TestNormalizeQueryPlanErrorPreservesBusinessError(t *testing.T) {
	err := normalizeQueryPlanError(merr.WrapErrServiceInternalMsg("BM25 resource response is invalid"))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestServerGetQueryPlanRejectsMismatchedPChannelMetadata(t *testing.T) {
	manager := &fakeWALManager{wal: &fakeProviderWAL{plan: &viewpb.QueryPlan{}}}
	server := NewServer(manager)

	_, err := server.GetQueryPlan(newIncomingPChannelContext("other-pchannel"), &viewpb.GetQueryPlanRequest{
		ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: testVChannel},
	})

	require.Error(t, err)
	assert.Equal(t, viewpb.ViewCode_VIEW_CODE_UNKNOWN, viewerror.AsViewError(err).Code)
}

func TestNormalizeQueryPlanErrorPreservesContextError(t *testing.T) {
	assert.ErrorIs(t, normalizeQueryPlanError(context.Canceled), context.Canceled)
	assert.ErrorIs(t, normalizeQueryPlanError(context.DeadlineExceeded), context.DeadlineExceeded)
}

func newIncomingPChannelContext(pchannel string) context.Context {
	outgoing := worknodehandler.EncodeQueryViewPChannelToOutgoingContext(context.Background(), types.PChannelInfo{
		Name:       pchannel,
		Term:       7,
		AccessMode: types.AccessModeRW,
	})
	md, _ := metadata.FromOutgoingContext(outgoing)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestQueryPlanServiceRejectsUnavailableInputs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		server  *Server
		ctx     context.Context
		channel string
		code    viewpb.ViewCode
	}{
		{"shutdown", NewServer(nil), context.Background(), testVChannel, viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN},
		{"missing channel", NewServer(&fakeWALManager{}), context.Background(), "", viewpb.ViewCode_VIEW_CODE_UNKNOWN},
		{"missing routing", NewServer(&fakeWALManager{}), context.Background(), testVChannel, viewpb.ViewCode_VIEW_CODE_UNKNOWN},
		{"unavailable WAL", NewServer(&fakeWALManager{err: context.Canceled}), newIncomingPChannelContext("by-dev-rootcoord-dml_0"), testVChannel, viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN},
		{"old WAL", NewServer(&fakeWALManager{}), newIncomingPChannelContext("by-dev-rootcoord-dml_0"), testVChannel, viewpb.ViewCode_VIEW_CODE_UNKNOWN},
		{"provider error", NewServer(&fakeWALManager{wal: &fakeProviderWAL{err: viewerror.NewNotPrimaryError("fenced")}}), newIncomingPChannelContext("by-dev-rootcoord-dml_0"), testVChannel, viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.server.GetMVCCTimestamp(tc.ctx, &viewpb.GetMVCCTimestampRequest{Vchannel: tc.channel})
			require.Equal(t, tc.code, viewerror.AsViewError(err).Code)
		})
	}
	require.NoError(t, normalizeQueryPlanError(nil))
}
