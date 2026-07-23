package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestManagerServiceValidateRuntime(t *testing.T) {
	manager := NewManagerService(mock_walmanager.NewMockManager(t))

	t.Run("analyzer validation success", func(t *testing.T) {
		resp, err := manager.ValidateRuntime(context.Background(), &streamingpb.StreamingNodeManagerValidateRuntimeRequest{
			Validation: &streamingpb.StreamingNodeManagerValidateRuntimeRequest_Analyzer{
				Analyzer: &streamingpb.StreamingNodeRuntimeAnalyzerValidation{
					AnalyzerInfos: []*streamingpb.StreamingNodeRuntimeAnalyzerInfo{
						{
							Field:  "test_field",
							Name:   "test_analyzer",
							Params: `{}`,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("analyzer validation failure", func(t *testing.T) {
		resp, err := manager.ValidateRuntime(context.Background(), &streamingpb.StreamingNodeManagerValidateRuntimeRequest{
			Validation: &streamingpb.StreamingNodeManagerValidateRuntimeRequest_Analyzer{
				Analyzer: &streamingpb.StreamingNodeRuntimeAnalyzerValidation{
					AnalyzerInfos: []*streamingpb.StreamingNodeRuntimeAnalyzerInfo{
						{
							Field:  "test_field",
							Name:   "test_analyzer",
							Params: `{"invalid": "params"}`,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}
