//go:build test && dynamic

package queryplan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
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
	plan *viewpb.QueryPlan
	err  error
	mvcc *viewpb.QueryPlanMVCC
}

func (w *fakeProviderWAL) GetQueryPlan(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlan, error) {
	return w.plan, w.err
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
	raw := &fakeProviderWAL{plan: &viewpb.QueryPlan{Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 99}}}
	manager := &fakeWALManager{wal: wrappedProviderWAL{WAL: raw, raw: raw}}
	server := NewServer(manager)

	resp, err := server.GetQueryPlan(newIncomingPChannelContext("by-dev-rootcoord-dml_0"), &viewpb.GetQueryPlanRequest{
		ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: testVChannel},
	})

	require.NoError(t, err)
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

func TestServerGetQueryPlanProjectsViewErrorToGRPCStatus(t *testing.T) {
	manager := &fakeWALManager{wal: &fakeProviderWAL{err: viewerror.NewViewNotFound("missing view")}}
	server := NewServer(manager)

	_, err := server.GetQueryPlan(newIncomingPChannelContext("by-dev-rootcoord-dml_0"), &viewpb.GetQueryPlanRequest{
		ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: testVChannel},
	})

	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestServerGetQueryPlanRejectsMismatchedPChannelMetadata(t *testing.T) {
	manager := &fakeWALManager{wal: &fakeProviderWAL{plan: &viewpb.QueryPlan{}}}
	server := NewServer(manager)

	_, err := server.GetQueryPlan(newIncomingPChannelContext("other-pchannel"), &viewpb.GetQueryPlanRequest{
		ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: testVChannel},
	})

	require.Error(t, err)
	assert.Equal(t, codes.Unknown, status.Code(err))
}

func TestToRPCErrorPreservesContextError(t *testing.T) {
	assert.ErrorIs(t, toRPCError(context.Canceled), context.Canceled)
	assert.ErrorIs(t, toRPCError(context.DeadlineExceeded), context.DeadlineExceeded)
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
