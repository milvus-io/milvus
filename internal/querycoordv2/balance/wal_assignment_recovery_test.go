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

package balance

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
)

func TestChannelBalanceSortDoesNotWaitForMissingWAL(t *testing.T) {
	t.Setenv(streamingutil.MilvusStreamingServiceEnabled, "1")
	lookup := mockey.Mock((*snmanager.StreamingNodeManager).GetLatestWALLocated).To(func(_ *snmanager.StreamingNodeManager, ctx context.Context, channel string) (int64, error) {
		if channel == "missing_1v0" {
			return -1, merr.WrapErrChannelNotAvailable(channel)
		}
		return 1, nil
	}).Build()
	defer lookup.UnPatch()
	local := &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "local_1v0"}, Node: 1}
	remote := &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "remote_1v0"}, Node: 2}
	missing := &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "missing_1v0"}, Node: 2}
	channels := []*meta.DmChannel{local, missing, remote}
	require.Equal(t, []*meta.DmChannel{remote, local, missing}, sortIfChannelAtWALLocated(context.Background(), channels))
	require.Len(t, channels, 3, "unavailable WAL must not subtract an existing channel from node load")
}
