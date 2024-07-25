package componentutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func TestComponentStateService(t *testing.T) {
	ctx := context.Background()
	s := NewComponentStateService("role")
	resp, err := s.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_StandBy, resp.State.StateCode)

	s.OnInitializing()
	resp, err = s.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Initializing, resp.State.StateCode)

	s.OnInitialized(1)
	resp, err = s.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, resp.State.StateCode)
	assert.Equal(t, "role", resp.State.Role)
	assert.Equal(t, int64(1), resp.State.NodeID)

	s.OnAbnormal()
	resp, err = s.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Abnormal, resp.State.StateCode)

	s.OnHealthy()
	resp, err = s.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, resp.State.StateCode)

	s.OnStopping()
	resp, err = s.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Stopping, resp.State.StateCode)
}
