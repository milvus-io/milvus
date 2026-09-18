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

package querynodev2

import (
	"context"
	"time"

	"github.com/bytedance/mockey"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// splitWatchRequest is the smallest WatchDmChannels request that creates (or,
// for an existing split child, adopts) a delegator on vchannel.
func (suite *ServiceSuite) splitWatchRequest(vchannel string) *querypb.WatchDmChannelsRequest {
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	return &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_WatchDmChannels,
			TargetID: suite.node.session.ServerID,
		},
		NodeID:       suite.node.session.ServerID,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
		Infos: []*datapb.VchannelInfo{{
			CollectionID: suite.collectionID,
			ChannelName:  vchannel,
			SeekPosition: suite.position,
		}},
		Schema: schema,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: suite.partitionIDs,
			MetricType:   defaultMetricType,
		},
		IndexInfoList: mock_segcore.GenTestIndexInfoList(suite.collectionID, schema),
	}
}

// awaitSplitChild waits until parent fronts a child on vchannel and returns the
// child the node registered for it.
func (suite *ServiceSuite) awaitSplitChild(parent delegator.ShardDelegator, vchannel string) delegator.ShardDelegator {
	suite.Require().Eventually(func() bool {
		return lo.Contains(parent.SplitChildVChannels(), vchannel)
	}, 10*time.Second, 10*time.Millisecond, "%s never fronted a child for %s", "parent", vchannel)
	child, ok := suite.node.delegators.Get(vchannel)
	suite.Require().True(ok, "the fronted child %s is not registered on the node", vchannel)
	return child
}

// A shard-split child is a shard like any other: once its own key range grows
// past the split threshold, a second split fences it. Whether the child is
// still un-adopted and fronted by its source, or already adopted by querycoord,
// it must front its own targets when it consumes that fence, exactly as a
// delegator created by WatchDmChannels does; otherwise rows written to the
// grandchild vchannels after the fence are served by no delegator until they
// are adopted.
func (suite *ServiceSuite) TestSplitChildFrontsItsOwnSplit() {
	ctx := context.Background()
	const (
		child1      = "by-dev-rootcoord-dml_1_111v1"
		grandchild3 = "by-dev-rootcoord-dml_2_111v3"
		grandchild4 = "by-dev-rootcoord-dml_3_111v4"
	)

	recovery := mockey.Mock((*QueryNode).waitSplitTargetRecovery).To(
		func(_ *QueryNode, _ int64, vchannel string) (*msgpb.MsgPosition, error) {
			return &msgpb.MsgPosition{ChannelName: vchannel, MsgID: suite.position.GetMsgID()}, nil
		}).Build()
	defer recovery.UnPatch()

	status, err := suite.node.WatchDmChannels(ctx, suite.splitWatchRequest(suite.vchannel))
	suite.Require().NoError(merr.CheckRPCCall(status, err))
	source, ok := suite.node.delegators.Get(suite.vchannel)
	suite.Require().True(ok)

	// the source consumes the first fence and spawns child v1.
	suite.Require().NoError(source.ProcessSplitShard(ctx, []string{child1}))
	child := suite.awaitSplitChild(source, child1)
	suite.Require().True(child.IsUnadoptedSplitChild())

	// the spawned, still un-adopted child consumes its own fence.
	suite.NoError(child.ProcessSplitShard(ctx, []string{grandchild3}),
		"a spawned split child must be able to front its own split")
	suite.awaitSplitChild(child, grandchild3)

	// querycoord adopts the child; the promoted child consumes a fence too.
	status, err = suite.node.WatchDmChannels(ctx, suite.splitWatchRequest(child1))
	suite.Require().NoError(merr.CheckRPCCall(status, err))
	suite.Require().False(child.IsUnadoptedSplitChild())
	suite.NoError(child.ProcessSplitShard(ctx, []string{grandchild4}),
		"an adopted split child must be able to front its own split")
	suite.awaitSplitChild(child, grandchild4)
}
