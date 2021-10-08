// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"encoding/binary"
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"
)

type mockReplica struct {
	Replica

	newSegments     map[UniqueID]*Segment
	normalSegments  map[UniqueID]*Segment
	flushedSegments map[UniqueID]*Segment
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

func TestFlowGraphDeleteNode_newDeleteNode(te *testing.T) {
	tests := []struct {
		replica Replica

		description string
	}{
		{&SegmentReplica{}, "pointer of SegmentReplica"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			dn := newDeleteNode(test.replica, "", make(chan *flushMsg))

			assert.NotNil(t, dn)
			assert.Equal(t, "deleteNode", dn.Name())
			dn.Close()
		})
	}

}

func TestFlowGraphDeleteNode_Operate(te *testing.T) {
	tests := []struct {
		invalidIn []Msg
		validIn   []Msg

		description string
	}{
		{[]Msg{}, nil,
			"Invalid input length == 0"},
		{[]Msg{&flowGraphMsg{}, &flowGraphMsg{}, &flowGraphMsg{}}, nil,
			"Invalid input length == 3"},
		{[]Msg{&flowGraphMsg{}}, nil,
			"Invalid input length == 1 but input message is not msgStreamMsg"},
		{nil, []Msg{&MsgStreamMsg{}},
			"valid input"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			flushCh := make(chan *flushMsg, 10)
			dn := deleteNode{flushCh: flushCh}
			if test.invalidIn != nil {
				rt := dn.Operate(test.invalidIn)
				assert.Empty(t, rt)
			} else {
				flushCh <- &flushMsg{0, 100, 10, 1}
				rt := dn.Operate(test.validIn)
				assert.Empty(t, rt)
			}
		})
	}
}

func Test_GetSegmentsByPKs(t *testing.T) {
	buf := make([]byte, 8)
	filter1 := bloom.NewWithEstimates(1000000, 0.01)
	for i := 0; i < 3; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		filter1.Add(buf)
	}
	filter2 := bloom.NewWithEstimates(1000000, 0.01)
	for i := 3; i < 5; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		filter2.Add(buf)
	}
	segment1 := &Segment{
		segmentID:   1,
		channelName: "test",
		pkFilter:    filter1,
	}
	segment2 := &Segment{
		segmentID:   2,
		channelName: "test",
		pkFilter:    filter1,
	}
	segment3 := &Segment{
		segmentID:   3,
		channelName: "test",
		pkFilter:    filter1,
	}
	segment4 := &Segment{
		segmentID:   4,
		channelName: "test",
		pkFilter:    filter2,
	}
	segment5 := &Segment{
		segmentID:   5,
		channelName: "test",
		pkFilter:    filter2,
	}
	segment6 := &Segment{
		segmentID:   5,
		channelName: "test_error",
		pkFilter:    filter2,
	}
	mockReplica := &mockReplica{}
	mockReplica.newSegments = make(map[int64]*Segment)
	mockReplica.normalSegments = make(map[int64]*Segment)
	mockReplica.flushedSegments = make(map[int64]*Segment)
	mockReplica.newSegments[segment1.segmentID] = segment1
	mockReplica.newSegments[segment2.segmentID] = segment2
	mockReplica.normalSegments[segment3.segmentID] = segment3
	mockReplica.normalSegments[segment4.segmentID] = segment4
	mockReplica.flushedSegments[segment5.segmentID] = segment5
	mockReplica.flushedSegments[segment6.segmentID] = segment6
	dn := newDeleteNode(mockReplica, "test", make(chan *flushMsg))
	results := dn.filterSegmentByPK(0, []int64{0, 1, 2, 3, 4})
	expected := map[int64][]int64{
		0: {1, 2, 3},
		1: {1, 2, 3},
		2: {1, 2, 3},
		3: {4, 5},
		4: {4, 5},
	}
	for key, value := range expected {
		assert.ElementsMatch(t, value, results[key])
	}
}
