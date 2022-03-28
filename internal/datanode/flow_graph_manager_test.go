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
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/etcd"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlowGraphManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()

	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
	node.SetEtcdClient(etcdCli)
	err = node.Init()
	require.Nil(t, err)
	err = node.Start()
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

		err := fm.addAndStart(node, vchan)
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

		err := fm.addAndStart(node, vchan)
		assert.NoError(t, err)
		assert.True(t, fm.exist(vchanName))

		fm.release(vchanName)

		assert.False(t, fm.exist(vchanName))
		fm.dropAll()
	})

	t.Run("Test getFlushCh", func(t *testing.T) {
		vchanName := "by-dev-rootcoord-dml-test-flowgraphmanager-getFlushCh"
		vchan := &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  vchanName,
		}
		require.False(t, fm.exist(vchanName))

		err := fm.addAndStart(node, vchan)
		assert.NoError(t, err)
		assert.True(t, fm.exist(vchanName))

		fg, ok := fm.getFlowgraphService(vchanName)
		require.True(t, ok)
		err = fg.replica.addNewSegment(100, 1, 10, vchanName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})
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
}
