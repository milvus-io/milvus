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

package assign

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestChannelAssignmentDefersMissingWALAcrossPolicies(t *testing.T) {
	paramtable.Init()
	t.Setenv(streamingutil.MilvusStreamingServiceEnabled, "1")
	sqns := mockey.Mock((*snmanager.StreamingNodeManager).GetStreamingQueryNodeIDs).Return(typeutil.NewUniqueSet(1, 2)).Build()
	defer sqns.UnPatch()
	lookup := mockey.Mock((*snmanager.StreamingNodeManager).GetLatestWALLocated).To(func(_ *snmanager.StreamingNodeManager, ctx context.Context, channel string) (int64, error) {
		if err := ctx.Err(); err != nil {
			return -1, err
		}
		switch channel {
		case "missing_1v0":
			return -1, merr.WrapErrChannelNotAvailable(channel)
		case "known_1v0":
			return 1, nil
		default:
			return 3, nil // Known WAL belongs to another replica.
		}
	}).Build()
	defer lookup.UnPatch()
	for _, withLedger := range []bool{false, true} {
		for _, name := range []string{PolicyTypeRoundRobin, PolicyTypeRowCount, PolicyTypeScoreBased} {
			suffix := "legacy"
			if withLedger {
				suffix = "ledger"
			}
			t.Run(name+"/"+suffix, func(t *testing.T) {
				nodes, scheduler, dist := newHeapRegressionAssignmentFixture(t, 2)
				factory := NewAssignPolicyFactory(scheduler, nodes, dist, meta.NewMeta(nil, nil, nodes), nil)
				policy := factory.GetPolicy(name)
				ctx := context.Background()
				if withLedger {
					ctx = WithPlanningLedger(ctx, NewPlanningLedgerState(map[int64]PlanningNode{
						1: {Ready: true}, 2: {Ready: true},
					}, nil, nil, nil, 8))
				}
				channels := []*meta.DmChannel{
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "missing_1v0"}},
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "known_1v0"}},
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "other_1v0"}},
				}
				plans := policy.AssignChannel(ctx, 1, channels, []int64{1, 2}, true)
				require.Len(t, plans, 2)
				require.Equal(t, "known_1v0", plans[0].Channel.GetChannelName())
				require.EqualValues(t, 1, plans[0].To)
				require.Equal(t, "other_1v0", plans[1].Channel.GetChannelName())
				require.Contains(t, []int64{1, 2}, plans[1].To)
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				require.Empty(t, policy.AssignChannel(canceled, 1, channels, []int64{1, 2}, true))
			})
		}
	}
}
