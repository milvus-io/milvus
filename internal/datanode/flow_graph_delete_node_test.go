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
	"encoding/binary"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/stretchr/testify/assert"
)

type mockReplica struct {
	Replica

	newSegments     map[UniqueID]*Segment
	normalSegments  map[UniqueID]*Segment
	flushedSegments map[UniqueID]*Segment
}

func newMockReplica() *mockReplica {
	return &mockReplica{
		newSegments:     make(map[int64]*Segment),
		normalSegments:  make(map[int64]*Segment),
		flushedSegments: make(map[int64]*Segment),
	}
}

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

func (replica *mockReplica) getCollectionSchema(collectionID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error) {
	return &schemapb.CollectionSchema{}, nil
}

func (replica *mockReplica) getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error) {
	return 0, 1, nil
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
			dn, err := newDeleteNode(test.ctx, make(chan *flushMsg), test.config)
			assert.Nil(t, err)

			assert.NotNil(t, dn)
			assert.Equal(t, "deleteNode", dn.Name())
			dn.Close()
		})
	}
}

func genMockReplica(segIDs []int64, pks []int64, chanName string) *mockReplica {
	buf := make([]byte, 8)
	filter0 := bloom.NewWithEstimates(1000000, 0.01)
	for i := 0; i < 3; i++ {
		binary.BigEndian.PutUint64(buf, uint64(pks[i]))
		filter0.Add(buf)
	}

	filter1 := bloom.NewWithEstimates(1000000, 0.01)
	for i := 3; i < 5; i++ {
		binary.BigEndian.PutUint64(buf, uint64(pks[i]))
		filter1.Add(buf)
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
		}

		for _, test := range invalidInTests {
			te.Run(test.desc, func(t *testing.T) {
				flushCh := make(chan *flushMsg, 10)
				dn := deleteNode{flushCh: flushCh}
				rt := dn.Operate(test.in)
				assert.Empty(t, rt)
			})
		}
	})

	const (
		chanName = "channel-test"
	)
	var (
		segIDs = []int64{11, 22, 33, 44, 55}
		pks    = []int64{3, 17, 44, 190, 425}
	)
	replica := genMockReplica(segIDs, pks, chanName)
	t.Run("Test get segment by primary keys", func(te *testing.T) {
		c := &nodeConfig{
			replica:      replica,
			allocator:    &allocator{},
			vChannelName: chanName,
		}

		dn, err := newDeleteNode(context.Background(), make(chan *flushMsg), c)
		assert.Nil(t, err)

		results := dn.filterSegmentByPK(0, pks)
		expected := map[int64][]int64{
			pks[0]: segIDs[0:3],
			pks[1]: segIDs[0:3],
			pks[2]: segIDs[0:3],
			pks[3]: segIDs[3:5],
			pks[4]: segIDs[3:5],
		}
		for key, value := range expected {
			assert.ElementsMatch(t, value, results[key])
		}
	})

	t.Run("Test deleteNode Operate valid Msg", func(te *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-operate"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.MetaRootPath = testPath
		Params.DeleteBinlogRootPath = testPath

		flushChan := make(chan *flushMsg, 100)
		c := &nodeConfig{
			replica:      replica,
			allocator:    NewAllocatorFactory(),
			vChannelName: chanName,
		}
		delNode, err := newDeleteNode(ctx, flushChan, c)
		assert.Nil(te, err)

		flushChan <- &flushMsg{
			msgID:        1,
			timestamp:    2000,
			collectionID: UniqueID(1),
		}

		msg := GenFlowGraphDeleteMsg(pks, chanName)
		var fgMsg flowgraph.Msg = &msg
		delNode.Operate([]flowgraph.Msg{fgMsg})
	})
}
