// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDescribeProjectionPreservesIngestionBinding(t *testing.T) {
	plan := &schemapb.FunctionChain{Name: "ingestion", Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion, Ops: []*schemapb.FunctionChainOp{{Op: "select", Inputs: []string{"text"}}}}
	schema := &schemapb.CollectionSchema{FunctionChains: []*schemapb.FunctionChain{plan}}
	for _, shared := range []bool{false, true} {
		projected, err := projectDescribeCollectionSchema(schema, shared)
		require.NoError(t, err)
		require.True(t, proto.Equal(plan, projected.GetFunctionChains()[0]))
		projected.FunctionChains[0].Name = "changed"
		require.Equal(t, "ingestion", plan.GetName(), "response mutation must not change cached collection metadata")
	}
}

func TestCreateIngestionBindingRequiresCoordinatorCapability(t *testing.T) {
	for _, version := range []string{"", "unsupported", common.FunctionChainTransformVersion} {
		t.Run("version="+version, func(t *testing.T) {
			coord := mocks.NewMockMixCoordClient(t)
			req := &milvuspb.CreateCollectionRequest{CollectionName: "with_ingestion"}
			task := &createCollectionTask{
				CreateCollectionRequest: req,
				mixCoord:                coord,
				schema: &schemapb.CollectionSchema{FunctionChains: []*schemapb.FunctionChain{{
					Name: "ingestion", Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion,
				}}},
			}
			info := &milvuspb.ComponentInfo{}
			if version != "" {
				info.ExtraInfo = []*commonpb.KeyValuePair{{Key: common.FunctionChainTransformVersionKey, Value: version}}
			}
			coord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
				Status: merr.Success(), State: info,
			}, nil).Once()
			if version == common.FunctionChainTransformVersion {
				coord.EXPECT().CreateCollection(mock.Anything, req).Return(merr.Success(), nil).Once()
				require.NoError(t, task.Execute(context.Background()))
			} else {
				require.ErrorIs(t, task.Execute(context.Background()), merr.ErrOperationNotSupported)
				coord.AssertNotCalled(t, "CreateCollection", mock.Anything, mock.Anything)
			}
		})
	}

	t.Run("capability RPC failure preserves cause and prevents creation", func(t *testing.T) {
		coord := mocks.NewMockMixCoordClient(t)
		cause := merr.WrapErrServiceNotReadyMsg("coordinator unavailable")
		coord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, cause).Once()
		task := &createCollectionTask{
			mixCoord: coord,
			schema: &schemapb.CollectionSchema{FunctionChains: []*schemapb.FunctionChain{{
				Name: "ingestion", Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion,
			}}},
		}
		require.ErrorIs(t, task.Execute(context.Background()), cause)
		coord.AssertNotCalled(t, "CreateCollection", mock.Anything, mock.Anything)
	})
}
