package snmanager

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestWALLocationUnavailableIsNotChannelRetirement(t *testing.T) {
	b := &mock_balancer.MockBalancer{}
	ready := mockey.Mock(balance.GetWithContext).Return(b, nil).Build()
	defer ready.UnPatch()
	assignment := mockey.Mock(mockey.GetMethod(b, "GetLatestWALLocated")).Return(int64(0), false).Build()
	defer assignment.UnPatch()
	m := &StreamingNodeManager{}
	_, err := m.GetLatestWALLocated(context.Background(), "p1_1v0")
	require.ErrorIs(t, err, merr.ErrChannelNotAvailable)
	require.NotErrorIs(t, err, merr.ErrChannelNotFound)
	assignment.Return(int64(12), true)
	owner, err := m.GetLatestWALLocated(context.Background(), "p1_1v0")
	require.NoError(t, err)
	require.Equal(t, int64(12), owner)
	ready.Return(nil, context.DeadlineExceeded)
	_, err = m.GetLatestWALLocated(context.Background(), "p1_1v0")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
