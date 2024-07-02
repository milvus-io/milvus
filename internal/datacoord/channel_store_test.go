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

package datacoord

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/testutils"
)

type ChannelStoreReloadSuite struct {
	testutils.PromMetricsSuite

	mockTxn *mocks.TxnKV
}

func (suite *ChannelStoreReloadSuite) SetupTest() {
	suite.mockTxn = mocks.NewTxnKV(suite.T())
}

func (suite *ChannelStoreReloadSuite) generateWatchInfo(name string, state datapb.ChannelWatchState) *datapb.ChannelWatchInfo {
	return &datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{
			ChannelName: name,
		},
		State: state,
	}
}

func (suite *ChannelStoreReloadSuite) TestReload() {
	type item struct {
		nodeID      int64
		channelName string
	}
	type testCase struct {
		tag    string
		items  []item
		expect map[int64]int
	}

	cases := []testCase{
		{
			tag:    "empty",
			items:  []item{},
			expect: map[int64]int{},
		},
		{
			tag: "normal",
			items: []item{
				{nodeID: 1, channelName: "dml1_v0"},
				{nodeID: 1, channelName: "dml2_v1"},
				{nodeID: 2, channelName: "dml3_v0"},
			},
			expect: map[int64]int{1: 2, 2: 1},
		},
		{
			tag: "buffer",
			items: []item{
				{nodeID: bufferID, channelName: "dml1_v0"},
			},
			expect: map[int64]int{bufferID: 1},
		},
	}

	for _, tc := range cases {
		suite.Run(tc.tag, func() {
			suite.mockTxn.ExpectedCalls = nil

			var keys, values []string
			for _, item := range tc.items {
				keys = append(keys, fmt.Sprintf("channel_store/%d/%s", item.nodeID, item.channelName))
				info := suite.generateWatchInfo(item.channelName, datapb.ChannelWatchState_WatchSuccess)
				bs, err := proto.Marshal(info)
				suite.Require().NoError(err)
				values = append(values, string(bs))
			}
			suite.mockTxn.EXPECT().LoadWithPrefix(mock.AnythingOfType("string")).Return(keys, values, nil)

			store := NewChannelStore(suite.mockTxn)
			err := store.Reload()
			suite.Require().NoError(err)

			for nodeID, expect := range tc.expect {
				suite.MetricsEqual(metrics.DataCoordDmlChannelNum.WithLabelValues(strconv.FormatInt(nodeID, 10)), float64(expect))
			}
		})
	}
}

func TestChannelStore(t *testing.T) {
	suite.Run(t, new(ChannelStoreReloadSuite))
}
