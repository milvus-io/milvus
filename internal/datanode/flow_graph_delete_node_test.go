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
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/retry"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/stretchr/testify/assert"
)

var deleteNodeTestDir = "/tmp/milvus_test/deleteNode"

type mockReplica struct {
	Replica

	newSegments       map[UniqueID]*Segment
	normalSegments    map[UniqueID]*Segment
	flushedSegments   map[UniqueID]*Segment
	compactedSegments map[UniqueID]*Segment
}

var _ Replica = (*mockReplica)(nil)

func newMockReplica() *mockReplica {
	return &mockReplica{
		newSegments:       make(map[int64]*Segment),
		normalSegments:    make(map[int64]*Segment),
		flushedSegments:   make(map[int64]*Segment),
		compactedSegments: make(map[int64]*Segment),
	}
}

func (replica *mockReplica) listCompactedSegmentIDs() map[UniqueID][]UniqueID {
	return make(map[UniqueID][]UniqueID)
}

func (replica *mockReplica) removeSegments(segIDs ...UniqueID) {}

func (replica *mockReplica) filterSegments(channelName string, partitionID UniqueID) []*Segment {
	results := make([]*Segment, 0)
	for _, value := range replica.newSegments {
		results = append(results, value)
	}
	for _, value := range replica.normalSegments {
		results = append(results, value)
	}
	for _, value := range replica.flushedSegments {
		results = append(results, value)
	}
	return results
}

func (replica *mockReplica) getCollectionID() UniqueID {
	return 0
}

func (replica *mockReplica) getCollectionSchema(collectionID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error) {
	if ts == 0 {
		return nil, errors.New("mocked error")
	}
	return &schemapb.CollectionSchema{}, nil
}

func (replica *mockReplica) getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error) {
	if segID == -1 {
		return -1, -1, errors.New("mocked error")
	}
	return 0, 1, nil
}

func (replica *mockReplica) hasSegment(segID UniqueID, countFlushed bool) bool {
	_, has := replica.newSegments[segID]
	if has {
		return true
	}
	_, has = replica.normalSegments[segID]
	if has {
		return true
	}
	if !countFlushed {
		return false
	}
	_, has = replica.flushedSegments[segID]
	return has
}

func (replica *mockReplica) getChannelConsumeStats() *channelConsumeStats {
	return newChannelConsumeStats()
}

func TestFlowGraphDeleteNode_newDeleteNode(te *testing.T) {
	tests := []struct {
		ctx    context.Context
		config *nodeConfig

		description string
	}{
		{context.Background(), &nodeConfig{}, "pointer of SegmentReplica"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			dn, err := newDeleteNode(test.ctx, nil, make(chan string, 1), test.config)
			assert.Nil(t, err)

			assert.NotNil(t, dn)
			assert.Equal(t, "deleteNode-"+dn.channelName, dn.Name())
			dn.Close()
		})
	}
}

func genMockReplica(segIDs []int64, pks []primaryKey, chanName string) *mockReplica {
	buf := make([]byte, 8)
	filter0 := bloom.NewWithEstimates(1000000, 0.01)
	for i := 0; i < 3; i++ {
		switch pks[i].Type() {
		case schemapb.DataType_Int64:
			common.Endian.PutUint64(buf, uint64(pks[i].(*int64PrimaryKey).Value))
			filter0.Add(buf)
		case schemapb.DataType_VarChar:
			filter0.AddString(pks[i].(*varCharPrimaryKey).Value)
		}
	}

	filter1 := bloom.NewWithEstimates(1000000, 0.01)
	for i := 3; i < 5; i++ {
		switch pks[i].Type() {
		case schemapb.DataType_Int64:
			common.Endian.PutUint64(buf, uint64(pks[i].(*int64PrimaryKey).Value))
			filter1.Add(buf)
		case schemapb.DataType_VarChar:
			filter1.AddString(pks[i].(*varCharPrimaryKey).Value)
		}
	}

	seg0 := &Segment{
		segmentID:   segIDs[0],
		channelName: chanName,
		pkFilter:    filter0,
	}
	seg1 := &Segment{
		segmentID:   segIDs[1],
		channelName: chanName,
		pkFilter:    filter0,
	}
	seg2 := &Segment{
		segmentID:   segIDs[2],
		channelName: chanName,
		pkFilter:    filter0,
	}
	seg3 := &Segment{
		segmentID:   segIDs[3],
		channelName: chanName,
		pkFilter:    filter1,
	}
	seg4 := &Segment{
		segmentID:   segIDs[4],
		channelName: chanName,
		pkFilter:    filter1,
	}
	seg5 := &Segment{
		segmentID:   segIDs[4],
		channelName: "test_error",
		pkFilter:    filter1,
	}

	replica := newMockReplica()
	replica.newSegments[segIDs[0]] = seg0
	replica.newSegments[segIDs[1]] = seg1
	replica.normalSegments[segIDs[2]] = seg2
	replica.normalSegments[segIDs[3]] = seg3
	replica.flushedSegments[segIDs[4]] = seg4
	replica.flushedSegments[segIDs[4]] = seg5

	return replica
}

func TestFlowGraphDeleteNode_Operate(t *testing.T) {
	t.Run("Test deleteNode Operate invalid Msg", func(te *testing.T) {
		invalidInTests := []struct {
			in   []Msg
			desc string
		}{
			{[]Msg{},
				"Invalid input length == 0"},
			{[]Msg{&flowGraphMsg{}, &flowGraphMsg{}, &flowGraphMsg{}},
				"Invalid input length == 3"},
			{[]Msg{&flowGraphMsg{}},
				"Invalid input length == 1 but input message is not msgStreamMsg"},
			{[]Msg{&flowgraph.MsgStreamMsg{}},
				"Invalid input length == 1 but input message is not flowGraphMsg"},
		}

		for _, test := range invalidInTests {
			te.Run(test.desc, func(t *testing.T) {
				dn := deleteNode{replica: newMockReplica()}
				rt := dn.Operate(test.in)
				assert.Empty(t, rt)
			})
		}
	})

	const (
		chanName = "channel-test"
	)
	var (
		segIDs   = []int64{11, 22, 33, 44, 55}
		int64Pks = []primaryKey{
			newInt64PrimaryKey(3),
			newInt64PrimaryKey(17),
			newInt64PrimaryKey(44),
			newInt64PrimaryKey(190),
			newInt64PrimaryKey(425),
		}
		varCharPks = []primaryKey{
			newVarCharPrimaryKey("ab"),
			newVarCharPrimaryKey("ac"),
			newVarCharPrimaryKey("bcd"),
			newVarCharPrimaryKey("gggg"),
			newVarCharPrimaryKey("milvus"),
		}
		tss = []uint64{1, 1, 1, 1, 1}
	)
	cm := storage.NewLocalChunkManager(storage.RootPath(deleteNodeTestDir))
	defer cm.RemoveWithPrefix("")

	t.Run("Test get segment by varChar primary keys", func(te *testing.T) {
		replica := genMockReplica(segIDs, varCharPks, chanName)
		fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, replica, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)
		c := &nodeConfig{
			replica:      replica,
			allocator:    &allocator{},
			vChannelName: chanName,
		}

		dn, err := newDeleteNode(context.Background(), fm, make(chan string, 1), c)
		assert.Nil(t, err)

		segID2Pks, _ := dn.filterSegmentByPK(0, varCharPks, tss)
		expected := map[int64][]primaryKey{
			segIDs[0]: varCharPks[0:3],
			segIDs[1]: varCharPks[0:3],
			segIDs[2]: varCharPks[0:3],
			segIDs[3]: varCharPks[3:5],
			segIDs[4]: varCharPks[3:5],
		}
		for segmentID, expectedPks := range expected {
			filterPks := segID2Pks[segmentID]
			assert.Equal(t, len(expectedPks), len(filterPks))
			for index, pk := range expectedPks {
				assert.Equal(t, true, pk.EQ(filterPks[index]))
			}
		}
	})

	replica := genMockReplica(segIDs, int64Pks, chanName)
	fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, replica, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)
	t.Run("Test get segment by int64 primary keys", func(te *testing.T) {
		c := &nodeConfig{
			replica:      replica,
			allocator:    &allocator{},
			vChannelName: chanName,
		}

		dn, err := newDeleteNode(context.Background(), fm, make(chan string, 1), c)
		assert.Nil(t, err)

		segID2Pks, _ := dn.filterSegmentByPK(0, int64Pks, tss)
		expected := map[int64][]primaryKey{
			segIDs[0]: int64Pks[0:3],
			segIDs[1]: int64Pks[0:3],
			segIDs[2]: int64Pks[0:3],
			segIDs[3]: int64Pks[3:5],
			segIDs[4]: int64Pks[3:5],
		}
		for segmentID, expectedPks := range expected {
			filterPks := segID2Pks[segmentID]
			assert.Equal(t, len(expectedPks), len(filterPks))
			for index, pk := range expectedPks {
				assert.Equal(t, true, pk.EQ(filterPks[index]))
			}
		}
	})

	t.Run("Test deleteNode Operate valid Msg with failure", func(te *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-operate"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.EtcdCfg.MetaRootPath = testPath
		Params.DataNodeCfg.DeleteBinlogRootPath = testPath

		c := &nodeConfig{
			replica:      replica,
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		delNode, err := newDeleteNode(ctx, fm, make(chan string, 1), c)
		assert.Nil(te, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToFlush = segIDs
		// this will fail since ts = 0 will trigger mocked error
		var fgMsg flowgraph.Msg = &msg
		delNode.Operate([]flowgraph.Msg{fgMsg})
	})
	t.Run("Test deleteNode Operate valid Msg with failure", func(te *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-operate"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.EtcdCfg.MetaRootPath = testPath
		Params.DataNodeCfg.DeleteBinlogRootPath = testPath

		c := &nodeConfig{
			replica:      replica,
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		delNode, err := newDeleteNode(ctx, fm, make(chan string, 1), c)
		assert.Nil(te, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToFlush = segIDs

		msg.endPositions[0].Timestamp = 100 // set to normal timestamp
		var fgMsg flowgraph.Msg = &msg
		delNode.Operate([]flowgraph.Msg{fgMsg})

		msg.deleteMessages = []*msgstream.DeleteMsg{}
		// send again shall trigger empty buffer flush
		delNode.Operate([]flowgraph.Msg{fgMsg})
	})
	t.Run("Test deleteNode Operate valid with dropCollection", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-operate"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.EtcdCfg.MetaRootPath = testPath
		Params.DataNodeCfg.DeleteBinlogRootPath = testPath

		c := &nodeConfig{
			replica:      replica,
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		sig := make(chan string, 1)
		delNode, err := newDeleteNode(ctx, fm, sig, c)
		assert.Nil(t, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToFlush = segIDs

		msg.endPositions[0].Timestamp = 100 // set to normal timestamp
		msg.dropCollection = true
		assert.NotPanics(t, func() {
			fm.startDropping()
			delNode.Operate([]flowgraph.Msg{&msg})
		})
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-timer.C:
			t.FailNow()
		case <-sig:
		}
	})

	t.Run("Test deleteNode Operate flushDelData failed", func(te *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-operate"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.EtcdCfg.MetaRootPath = testPath
		Params.DataNodeCfg.DeleteBinlogRootPath = testPath

		c := &nodeConfig{
			replica:      &mockReplica{},
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		delNode, err := newDeleteNode(ctx, fm, make(chan string, 1), c)
		assert.Nil(te, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToFlush = []UniqueID{-1}
		delNode.delBuf.Store(UniqueID(-1), &DelDataBuf{})
		delNode.flushManager = &mockFlushManager{
			returnError: true,
		}

		var fgMsg flowgraph.Msg = &msg

		flowGraphRetryOpt = retry.Attempts(1)
		assert.Panics(te, func() {
			delNode.Operate([]flowgraph.Msg{fgMsg})
		})
	})
}
