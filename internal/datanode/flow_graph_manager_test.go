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

package datanode

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
)

func TestFlowGraphManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()

	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
	defer node.Stop()
	node.SetEtcdClient(etcdCli)
	err = node.Init()
	require.Nil(t, err)

	fm := newFlowgraphManager()
	defer func() {
		fm.dropAll()
	}()
	t.Run("Test addAndStart", func(t *testing.T) {
		vchanName := "by-dev-rootcoord-dml-test-flowgraphmanager-addAndStart"
		vchan := &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  vchanName,
		}
		require.False(t, fm.exist(vchanName))

		err := fm.addAndStart(node, vchan, nil, genTestTickler())
		assert.NoError(t, err)
		assert.True(t, fm.exist(vchanName))

		fm.dropAll()
	})

	t.Run("Test Release", func(t *testing.T) {
		vchanName := "by-dev-rootcoord-dml-test-flowgraphmanager-Release"
		vchan := &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  vchanName,
		}
		require.False(t, fm.exist(vchanName))

		err := fm.addAndStart(node, vchan, nil, genTestTickler())
		assert.NoError(t, err)
		assert.True(t, fm.exist(vchanName))

		fm.release(vchanName)

		assert.False(t, fm.exist(vchanName))
		fm.dropAll()
	})

	t.Run("Test getChannel", func(t *testing.T) {
		vchanName := "by-dev-rootcoord-dml-test-flowgraphmanager-getChannel"
		vchan := &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  vchanName,
		}
		require.False(t, fm.exist(vchanName))

		err := fm.addAndStart(node, vchan, nil, genTestTickler())
		assert.NoError(t, err)
		assert.True(t, fm.exist(vchanName))
		fg, ok := fm.getFlowgraphService(vchanName)
		require.True(t, ok)
		err = fg.channel.addSegment(addSegmentReq{
			segType:     datapb.SegmentType_New,
			segID:       100,
			collID:      1,
			partitionID: 10,
			startPos:    &msgpb.MsgPosition{},
			endPos:      &msgpb.MsgPosition{},
		})
		require.NoError(t, err)

		tests := []struct {
			isvalid bool
			inSegID UniqueID

			description string
		}{
			{true, 100, "valid input for existed segmentID 100"},
			{false, 101, "invalid input for not existed segmentID 101"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				rep, err := fm.getChannel(test.inSegID)

				if test.isvalid {
					assert.NoError(t, err)
					assert.NotNil(t, rep)
				} else {
					assert.Error(t, err)
					assert.Nil(t, rep)
				}
			})
		}
	})

	t.Run("Test getFlushCh", func(t *testing.T) {
		vchanName := "by-dev-rootcoord-dml-test-flowgraphmanager-getFlushCh"
		vchan := &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  vchanName,
		}
		require.False(t, fm.exist(vchanName))

		err := fm.addAndStart(node, vchan, nil, genTestTickler())
		assert.NoError(t, err)
		assert.True(t, fm.exist(vchanName))

		fg, ok := fm.getFlowgraphService(vchanName)
		require.True(t, ok)
		err = fg.channel.addSegment(addSegmentReq{
			segType:     datapb.SegmentType_New,
			segID:       100,
			collID:      1,
			partitionID: 10,
			startPos:    &msgpb.MsgPosition{},
			endPos:      &msgpb.MsgPosition{},
		})
		require.NoError(t, err)

		tests := []struct {
			isvalid bool
			inSegID UniqueID

			description string
		}{
			{true, 100, "valid input for existed segmentID 100"},
			{false, 101, "invalid input for not existed segmentID 101"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				ch, err := fm.getFlushCh(test.inSegID)

				if test.isvalid {
					assert.NoError(t, err)
					assert.NotNil(t, ch)
				} else {
					assert.Error(t, err)
					assert.Nil(t, ch)
				}
			})
		}
	})

	t.Run("Test getFlowgraphService", func(t *testing.T) {
		fg, ok := fm.getFlowgraphService("channel-not-exist")
		assert.False(t, ok)
		assert.Nil(t, fg)
	})

	t.Run("test execute", func(t *testing.T) {
		tests := []struct {
			testName         string
			totalMemory      uint64
			watermark        float64
			memorySizes      []int64
			expectNeedToSync []bool
		}{
			{"test over the watermark", 100, 0.5,
				[]int64{15, 16, 17, 18}, []bool{false, false, false, true}},
			{"test below the watermark", 100, 0.5,
				[]int64{1, 2, 3, 4}, []bool{false, false, false, false}},
		}

		fm.dropAll()
		const channelPrefix = "by-dev-rootcoord-dml-test-fg-mgr-execute-"
		var baseParams = &Params.BaseTable
		baseParams.Save(Params.DataNodeCfg.MemoryForceSyncEnable.Key, fmt.Sprintf("%t", true))
		for _, test := range tests {
			baseParams.Save(Params.DataNodeCfg.MemoryWatermark.Key, fmt.Sprintf("%f", test.watermark))
			for i, memorySize := range test.memorySizes {
				vchannel := fmt.Sprintf("%s%d", channelPrefix, i)
				vchan := &datapb.VchannelInfo{
					ChannelName: vchannel,
				}
				err = fm.addAndStart(node, vchan, nil, genTestTickler())
				assert.NoError(t, err)
				fg, ok := fm.flowgraphs.Load(vchannel)
				assert.True(t, ok)
				err = fg.(*dataSyncService).channel.addSegment(addSegmentReq{segID: 0})
				assert.NoError(t, err)
				fg.(*dataSyncService).channel.updateSegmentMemorySize(0, memorySize)
				fg.(*dataSyncService).channel.(*ChannelMeta).needToSync.Store(false)
			}
			fm.execute(test.totalMemory)
			for i, needToSync := range test.expectNeedToSync {
				vchannel := fmt.Sprintf("%s%d", channelPrefix, i)
				fg, ok := fm.flowgraphs.Load(vchannel)
				assert.True(t, ok)
				assert.Equal(t, needToSync, fg.(*dataSyncService).channel.(*ChannelMeta).needToSync.Load())
			}
		}
	})
}
