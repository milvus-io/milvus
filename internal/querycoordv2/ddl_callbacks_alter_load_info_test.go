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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadCallbacksRecordDemandAfterBroadcast(t *testing.T) {
	enableAutoscalePrecheckForTest(t)
	setAutoscaleCapacityRatiosForTest(t, "0.9", "0.5")
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false"))
	t.Cleanup(func() { params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key) })

	for _, loadPartitions := range []bool{false, true} {
		operation := "LoadCollection"
		if loadPartitions {
			operation = "LoadPartitions"
		}
		for _, scenario := range []string{"precheck rejection", "broadcast failure", "success", "tiered success", "metrics failure", "metrics and broadcast failure"} {
			mockey.PatchConvey(operation+"/"+scenario, t, func() {
				tiered := scenario == "tiered success"
				if tiered {
					require.NoError(t, params.Save(params.QueryNodeCfg.TieredEvictionEnabled.Key, "true"))
					defer params.Save(params.QueryNodeCfg.TieredEvictionEnabled.Key, "false")
				}
				ctx := context.Background()
				cluster := session.NewMockCluster(t)
				server := newAutoscalePrecheckTestServer(t, cluster)
				broker := meta.NewMockBroker(t)
				server.broker = broker
				broker.EXPECT().DescribeCollection(mock.Anything, autoscalePrecheckTestCollectionID).
					Return(newAutoscalePrecheckTestRequest().CollectionInfo, nil).Once()
				if !loadPartitions {
					broker.EXPECT().GetPartitions(mock.Anything, autoscalePrecheckTestCollectionID).
						Return([]int64{autoscalePrecheckTestPartitionID}, nil).Once()
				}
				totalMemory := uint64(8192)
				if scenario == "precheck rejection" {
					totalMemory = 1024
				}
				if scenario == "metrics failure" || scenario == "metrics and broadcast failure" {
					cluster.EXPECT().GetMetrics(mock.Anything, int64(1), mock.Anything).
						Return(nil, errors.New("metrics unavailable")).Once()
				} else if !tiered {
					cluster.EXPECT().GetMetrics(mock.Anything, int64(1), mock.Anything).
						Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: newAutoscalePrecheckMetrics(t, totalMemory, 0, 1)}, nil).Once()
				}
				mockey.Mock((*Server).requiredLoadResourceByRG).Return(map[string]autoscaleResourceUsage{
					meta.DefaultResourceGroupName: {memoryBytes: 1024, diskBytes: 2048},
				}, nil).Build()
				msg := message.NewAlterLoadConfigMessageBuilderV2().
					WithHeader(&messagespb.AlterLoadConfigMessageHeader{CollectionId: autoscalePrecheckTestCollectionID}).
					WithBody(&messagespb.AlterLoadConfigMessageBody{}).
					WithBroadcast([]string{"_ctrl_channel"}).MustBuildBroadcast()
				mockey.Mock(job.GenerateAlterLoadConfigMessage).Return(msg, nil).Build()
				bapi := mock_broadcaster.NewMockBroadcastAPI(t)
				bapi.EXPECT().Close().Return().Once()
				mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(bapi, nil).Build()

				memoryCounter := metrics.QueryCoordLoadDemandMemoryBytes.WithLabelValues(meta.DefaultResourceGroupName)
				diskCounter := metrics.QueryCoordLoadDemandDiskBytes.WithLabelValues(meta.DefaultResourceGroupName)
				memoryBefore := testutil.ToFloat64(memoryCounter)
				diskBefore := testutil.ToFloat64(diskCounter)
				broadcastErr := errors.New("broadcast failed")
				if scenario != "precheck rejection" {
					bapi.EXPECT().Broadcast(mock.Anything, msg).
						RunAndReturn(func(context.Context, message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
							assert.Equal(t, memoryBefore, testutil.ToFloat64(memoryCounter))
							assert.Equal(t, diskBefore, testutil.ToFloat64(diskCounter))
							if scenario == "broadcast failure" || scenario == "metrics and broadcast failure" {
								return nil, broadcastErr
							}
							return &types.BroadcastAppendResult{}, nil
						}).Once()
				}

				var err error
				if loadPartitions {
					err = server.broadcastAlterLoadConfigCollectionV2ForLoadPartitions(ctx, &querypb.LoadPartitionsRequest{
						CollectionID: autoscalePrecheckTestCollectionID, ReplicaNumber: 1,
						PartitionIDs: []int64{autoscalePrecheckTestPartitionID},
					})
				} else {
					err = server.broadcastAlterLoadConfigCollectionV2ForLoadCollection(ctx, &querypb.LoadCollectionRequest{
						CollectionID: autoscalePrecheckTestCollectionID, ReplicaNumber: 1,
					})
				}
				switch scenario {
				case "precheck rejection":
					var precheckErr *loadResourcePrecheckError
					require.ErrorAs(t, err, &precheckErr)
					bapi.AssertNotCalled(t, "Broadcast", mock.Anything, mock.Anything)
				case "broadcast failure", "metrics and broadcast failure":
					require.ErrorIs(t, err, broadcastErr)
				case "success", "tiered success", "metrics failure":
					require.NoError(t, err)
					memoryBefore += 1024
					diskBefore += 2048
				}
				assert.Equal(t, memoryBefore, testutil.ToFloat64(memoryCounter))
				assert.Equal(t, diskBefore, testutil.ToFloat64(diskCounter))
				if tiered {
					cluster.AssertNotCalled(t, "GetMetrics", mock.Anything, mock.Anything, mock.Anything)
				}
			})
		}
	}
}

func buildAlterLoadConfigBroadcastResult(collectionID int64) message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := "_ctrl_channel"
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.AlterLoadConfigMessageHeader{
			CollectionId: collectionID,
			Replicas: []*messagespb.LoadReplicaConfig{
				{ReplicaId: 1, ResourceGroupName: "__default_resource_group"},
			},
		}).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()

	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{
			controlChannel: {},
		},
	}
}

// TestAlterLoadConfigV2AckCallback verifies that the ack callback swallows the
// dropped-sentinel error (so the broadcaster stops retrying forever) while still
// propagating any other error from the load job.
func TestAlterLoadConfigV2AckCallback(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	s := &Server{}
	result := buildAlterLoadConfigBroadcastResult(1000)

	mockey.PatchConvey("dropped sentinel is acked with no-op", t, func() {
		mockey.Mock((*job.LoadCollectionJob).Execute).
			Return(merr.WrapErrChannelDroppedSentinel("_ctrl_channel")).Build()

		err := s.alterLoadConfigV2AckCallback(ctx, result)
		assert.NoError(t, err)
	})

	mockey.PatchConvey("generic error is propagated", t, func() {
		expectedErr := errors.New("broker unavailable")
		mockey.Mock((*job.LoadCollectionJob).Execute).Return(expectedErr).Build()

		err := s.alterLoadConfigV2AckCallback(ctx, result)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr))
	})
}
