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

package balancer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCheckWALPlacement(t *testing.T) {
	for _, tc := range []struct {
		name, state, primary string
		unknown, ro          bool
		want                 map[string]bool
	}{
		{name: "not configured", primary: "", want: map[string]bool{}},
		{name: "assigned primary", state: "assigned", primary: "B", want: map[string]bool{"B": true}},
		{name: "assigned old owner", state: "old", primary: "B", want: map[string]bool{"A": false, "B": false}},
		{name: "assigning remembers source", state: "assigning", primary: "B", want: map[string]bool{"A": false, "B": false}},
		{name: "unavailable", state: "unavailable", primary: "B", want: map[string]bool{"B": false}},
		{name: "uninitialized", state: "initial", primary: "B", want: map[string]bool{"B": false}},
		{name: "unknown old owner", state: "assigning", primary: "B", unknown: true, want: map[string]bool{"": false, "B": false}},
		{name: "RO outside placement scope", state: "old", primary: "B", ro: true, want: map[string]bool{"B": true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode := types.AccessModeRW
			if tc.ro {
				mode = types.AccessModeRO
			}
			ch := channel.NewPChannelMeta("p0", mode).CopyForWrite()
			if tc.state != "initial" {
				ch.TryAssignToServerID(mode, types.StreamingNodeInfo{ServerID: 1})
				ch.AssignToServerDone()
				if tc.state != "old" {
					ch.TryAssignToServerID(mode, types.StreamingNodeInfo{ServerID: 2})
				}
				if tc.state == "assigned" || tc.state == "unavailable" {
					ch.AssignToServerDone()
				}
				if tc.state == "unavailable" {
					ch.MarkAsUnavailable(ch.CurrentTerm())
				}
			}
			nodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{
				1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1}, ResourceGroup: "A"},
				2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2}, ResourceGroup: "B"},
			}
			if tc.unknown {
				delete(nodes, 1)
			}
			snapshot := &WatchChannelAssignmentsCallbackParam{PChannelView: &channel.PChannelView{Channels: map[types.ChannelID]*channel.PChannelMeta{ch.ChannelInfo().ChannelID(): ch.PChannelMeta}}}
			defer mockey.Mock((*balancerImpl).GetLatestChannelAssignment).Return(snapshot, nil).Build().UnPatch()
			defer mockey.Mock((*balancerImpl).GetAllStreamingNodes).Return(nodes, nil).Build().UnPatch()
			result, err := CheckWALPlacement(context.Background(), &balancerImpl{}, tc.primary)
			require.NoError(t, err)
			require.Len(t, result, len(tc.want))
			for rg, ready := range tc.want {
				reason, ok := result[rg]
				require.True(t, ok, rg)
				require.Equal(t, ready, reason == "", reason)
			}
		})
	}
}

func TestCheckWALPlacementReadErrors(t *testing.T) {
	failure := merr.WrapErrServiceUnavailableMsg("unavailable")
	for _, tc := range []struct {
		name                 string
		snapshot             *WatchChannelAssignmentsCallbackParam
		snapshotErr, nodeErr error
	}{
		{name: "channel read", snapshotErr: failure},
		{name: "missing channel view", snapshot: &WatchChannelAssignmentsCallbackParam{}},
		{name: "node read", snapshot: &WatchChannelAssignmentsCallbackParam{PChannelView: &channel.PChannelView{}}, nodeErr: failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer mockey.Mock((*balancerImpl).GetLatestChannelAssignment).Return(tc.snapshot, tc.snapshotErr).Build().UnPatch()
			defer mockey.Mock((*balancerImpl).GetAllStreamingNodes).Return(nil, tc.nodeErr).Build().UnPatch()
			_, err := CheckWALPlacement(context.Background(), &balancerImpl{}, "B")
			require.Error(t, err)
		})
	}
}
