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

package job

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadJobPersistsReplicaTarget(t *testing.T) {
	previous := meta.GlobalFailedLoadCache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	t.Cleanup(func() { meta.GlobalFailedLoadCache = previous })
	msg := message.NewAlterLoadConfigMessageBuilderV2().WithHeader(&messagespb.AlterLoadConfigMessageHeader{
		CollectionId: 100, PartitionIds: []int64{10}, UserSpecifiedReplicaMode: true,
		Replicas: []*messagespb.LoadReplicaConfig{{ReplicaId: 1, ResourceGroupName: "A"}, {ReplicaId: 2, ResourceGroupName: "A"}, {ReplicaId: 3, ResourceGroupName: "B"}},
	}).WithBody(&messagespb.AlterLoadConfigMessageBody{}).WithBroadcast([]string{"control"}).MustBuildBroadcast()
	result := message.BroadcastResultAlterLoadConfigMessageV2{Message: message.MustAsBroadcastAlterLoadConfigMessageV2(msg)}
	defer mockey.Mock((*meta.CoordinatorBroker).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Build().UnPatch()
	defer mockey.Mock(utils.SpawnReplicasWithReplicaConfig).Return(nil, nil).Build().UnPatch()
	failure := merr.WrapErrServiceUnavailableMsg("stop at metadata persistence")
	var saved *querypb.CollectionLoadInfo
	defer mockey.Mock((*meta.CollectionManager).PutCollection).To(func(_ *meta.CollectionManager, _ context.Context, c *meta.Collection, _ ...*meta.Partition) error {
		saved = proto.Clone(c.CollectionLoadInfo).(*querypb.CollectionLoadInfo)
		return failure
	}).Build().UnPatch()
	m := meta.NewMeta(nil, nil, nil)
	job := NewLoadCollectionJob(context.Background(), result, nil, m, &meta.CoordinatorBroker{}, nil, nil, nil, nil, nil, nil)
	require.ErrorIs(t, job.Execute(), failure)
	require.NotNil(t, saved)
	require.Equal(t, map[string]int32{"A": 2, "B": 1}, saved.GetResourceGroupReplicaNumbers())
	require.Equal(t, int32(3), saved.GetReplicaNumber())
	require.True(t, saved.GetUserSpecifiedReplicaMode())
}
