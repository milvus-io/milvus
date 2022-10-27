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
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/retry"
)

var deleteNodeTestDir = "/tmp/milvus_test/deleteNode"

func TestFlowGraphDeleteNode_newDeleteNode(te *testing.T) {
	tests := []struct {
		ctx    context.Context
		config *nodeConfig

		description string
	}{
		{context.Background(), &nodeConfig{}, "pointer of channel"},
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

func genMockChannel(segIDs []int64, pks []primaryKey, chanName string) *ChannelMeta {
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

	segTypes := []datapb.SegmentType{
		datapb.SegmentType_New,
		datapb.SegmentType_New,
		datapb.SegmentType_Normal,
		datapb.SegmentType_Normal,
		datapb.SegmentType_Flushed,
		datapb.SegmentType_Flushed,
	}

	channel := &ChannelMeta{
		channelName: chanName,
		segments:    make(map[UniqueID]*Segment),
	}
	for i := range segIDs {
		seg := Segment{
			segmentID: segIDs[i],
		}
		seg.setType(segTypes[i])
		if i < 3 {
			seg.pkStat.pkFilter = filter0
		} else {
			seg.pkStat.pkFilter = filter1
		}
		channel.segments[segIDs[i]] = &seg
	}

	return channel
}

func TestFlowGraphDeleteNode_Operate(t *testing.T) {
	ctx := context.Background()
	t.Run("Test deleteNode Operate invalid Msg", func(te *testing.T) {
		invalidInTests := []struct {
			in   []Msg
			desc string
		}{
			{[]Msg{},
				"Invalid input length == 0"},
			{[]Msg{&flowGraphMsg{}, &flowGraphMsg{}, &flowGraphMsg{}},
				"Invalid input length == 3"},
			{[]Msg{&flowgraph.MsgStreamMsg{}},
				"Invalid input length == 1 but input message is not flowGraphMsg"},
		}

		for _, test := range invalidInTests {
			te.Run(test.desc, func(t *testing.T) {
				dn := deleteNode{}
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
	defer cm.RemoveWithPrefix(ctx, "")

	t.Run("Test get segment by varChar primary keys", func(te *testing.T) {
		channel := genMockChannel(segIDs, varCharPks, chanName)
		fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)
		c := &nodeConfig{
			channel:      channel,
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

	channel := genMockChannel(segIDs, int64Pks, chanName)
	fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)
	t.Run("Test get segment by int64 primary keys", func(te *testing.T) {
		c := &nodeConfig{
			channel:      channel,
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

		c := &nodeConfig{
			channel:      channel,
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		delNode, err := newDeleteNode(ctx, fm, make(chan string, 1), c)
		assert.Nil(te, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToSync = segIDs
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

		c := &nodeConfig{
			channel:      channel,
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		delNode, err := newDeleteNode(ctx, fm, make(chan string, 1), c)
		assert.Nil(te, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToSync = segIDs

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

		c := &nodeConfig{
			channel:      channel,
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		sig := make(chan string, 1)
		delNode, err := newDeleteNode(ctx, fm, sig, c)
		assert.Nil(t, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToSync = segIDs

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

		c := &nodeConfig{
			channel:      nil,
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		delNode, err := newDeleteNode(ctx, fm, make(chan string, 1), c)
		assert.Nil(te, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToSync = []UniqueID{-1}
		delNode.delBuf.Store(UniqueID(-1), &DelDataBuf{})
		delNode.flushManager = &mockFlushManager{
			returnError: true,
		}

		var fgMsg flowgraph.Msg = &msg

		setFlowGraphRetryOpt(retry.Attempts(1))
		assert.Panics(te, func() {
			delNode.Operate([]flowgraph.Msg{fgMsg})
		})
	})

	t.Run("Test issue#18565", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-issue18565"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.EtcdCfg.MetaRootPath = testPath

		channel := &ChannelMeta{
			segments: make(map[UniqueID]*Segment),
		}

		c := &nodeConfig{
			channel:      channel,
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		delNode, err := newDeleteNode(ctx, fm, make(chan string, 1), c)
		assert.Nil(t, err)

		compactedSegment := UniqueID(10020987)
		seg := Segment{
			segmentID:   compactedSegment,
			compactedTo: 100,
		}
		seg.setType(datapb.SegmentType_Compacted)
		channel.segments[compactedSegment] = &seg

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.deleteMessages = []*msgstream.DeleteMsg{}
		msg.segmentsToSync = []UniqueID{compactedSegment}

		delNode.delBuf.Store(compactedSegment, &DelDataBuf{delData: &DeleteData{}})
		delNode.flushManager = NewRendezvousFlushManager(&allocator{}, cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

		var fgMsg flowgraph.Msg = &msg
		setFlowGraphRetryOpt(retry.Attempts(1))
		assert.NotPanics(t, func() {
			delNode.Operate([]flowgraph.Msg{fgMsg})
		})

		_, ok := delNode.delBuf.Load(100)
		assert.False(t, ok)
		_, ok = delNode.delBuf.Load(compactedSegment)
		assert.False(t, ok)
	})
}

func TestFlowGraphDeleteNode_showDelBuf(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cm := storage.NewLocalChunkManager(storage.RootPath(deleteNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, "")

	fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, nil, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	chanName := "datanode-test-FlowGraphDeletenode-showDelBuf"
	testPath := "/test/datanode/root/meta"
	assert.NoError(t, clearEtcd(testPath))
	Params.EtcdCfg.MetaRootPath = testPath

	c := &nodeConfig{
		channel:      nil,
		allocator:    NewAllocatorFactory(),
		vChannelName: chanName,
	}
	delNode, err := newDeleteNode(ctx, fm, make(chan string, 1), c)
	require.NoError(t, err)

	tests := []struct {
		seg     UniqueID
		numRows int64
	}{
		{111, 10},
		{112, 10},
		{113, 1},
	}

	for _, test := range tests {
		delBuf := newDelDataBuf()
		delBuf.updateSize(test.numRows)
		delNode.delBuf.Store(test.seg, delBuf)
	}

	delNode.showDelBuf([]UniqueID{111, 112, 113}, 100)
}

func TestFlowGraphDeleteNode_updateCompactedSegments(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(deleteNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, "")

	fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, nil, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	chanName := "datanode-test-FlowGraphDeletenode-showDelBuf"
	testPath := "/test/datanode/root/meta"
	assert.NoError(t, clearEtcd(testPath))
	Params.EtcdCfg.MetaRootPath = testPath

	channel := ChannelMeta{
		segments: make(map[UniqueID]*Segment),
	}

	c := &nodeConfig{
		channel:      &channel,
		allocator:    NewAllocatorFactory(),
		vChannelName: chanName,
	}
	delNode, err := newDeleteNode(ctx, fm, make(chan string, 1), c)
	require.NoError(t, err)

	tests := []struct {
		description    string
		compactToExist bool
		segIDsInBuffer []UniqueID

		compactedToIDs   []UniqueID
		compactedFromIDs []UniqueID

		expectedSegsRemain []UniqueID
	}{
		{"zero segments", false,
			[]UniqueID{}, []UniqueID{}, []UniqueID{}, []UniqueID{}},
		{"segment no compaction", false,
			[]UniqueID{100, 101}, []UniqueID{}, []UniqueID{}, []UniqueID{100, 101}},
		{"segment compacted not in buffer", true,
			[]UniqueID{100, 101}, []UniqueID{200}, []UniqueID{103}, []UniqueID{100, 101}},
		{"segment compacted in buffer 100>201", true,
			[]UniqueID{100, 101}, []UniqueID{201}, []UniqueID{100}, []UniqueID{101, 201}},
		{"segment compacted in buffer 100+101>201", true,
			[]UniqueID{100, 101}, []UniqueID{201, 201}, []UniqueID{100, 101}, []UniqueID{201}},
		{"segment compacted in buffer 100>201, 101>202", true,
			[]UniqueID{100, 101}, []UniqueID{201, 202}, []UniqueID{100, 101}, []UniqueID{201, 202}},
		// false
		{"segment compacted in buffer 100>201", false,
			[]UniqueID{100, 101}, []UniqueID{201}, []UniqueID{100}, []UniqueID{101}},
		{"segment compacted in buffer 100+101>201", false,
			[]UniqueID{100, 101}, []UniqueID{201, 201}, []UniqueID{100, 101}, []UniqueID{}},
		{"segment compacted in buffer 100>201, 101>202", false,
			[]UniqueID{100, 101}, []UniqueID{201, 202}, []UniqueID{100, 101}, []UniqueID{}},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			for _, seg := range test.segIDsInBuffer {
				delBuf := newDelDataBuf()
				delBuf.updateSize(100)
				delNode.delBuf.Store(seg, delBuf)
			}

			if test.compactToExist {
				for _, segID := range test.compactedToIDs {
					seg := Segment{
						segmentID: segID,
						numRows:   10,
					}
					seg.setType(datapb.SegmentType_Flushed)
					channel.segments[segID] = &seg
				}
			} else { // clear all segments in channel
				channel.segments = make(map[UniqueID]*Segment)
			}

			for i, segID := range test.compactedFromIDs {
				seg := Segment{
					segmentID:   segID,
					compactedTo: test.compactedToIDs[i],
				}
				seg.setType(datapb.SegmentType_Compacted)
				channel.segments[segID] = &seg
			}

			delNode.updateCompactedSegments()

			for _, remain := range test.expectedSegsRemain {
				_, ok := delNode.delBuf.Load(remain)
				assert.True(t, ok)
			}
		})
	}
}
