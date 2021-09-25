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

func TestFlowGraphDeleteNode_newDeleteNode(te *testing.T) {
	tests := []struct {
		replica Replica

		description string
	}{
		{&SegmentReplica{}, "pointer of SegmentReplica"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			dn := newDeleteDNode(test.replica)

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
		{[]Msg{&insertMsg{}, &insertMsg{}, &insertMsg{}}, nil,
			"Invalid input length == 3"},
		{[]Msg{&insertMsg{}}, nil,
			"Invalid input length == 1 but input message is not msgStreamMsg"},
		{nil, []Msg{&MsgStreamMsg{}},
			"valid input"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			dn := deleteNode{}
			if test.invalidIn != nil {
				rt := dn.Operate(test.invalidIn)
				assert.Empty(t, rt)
			} else {
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
		segmentID: 1,
		pkFilter:  filter1,
	}
	segment2 := &Segment{
		segmentID: 2,
		pkFilter:  filter1,
	}
	segment3 := &Segment{
		segmentID: 3,
		pkFilter:  filter1,
	}
	segment4 := &Segment{
		segmentID: 4,
		pkFilter:  filter2,
	}
	segment5 := &Segment{
		segmentID: 5,
		pkFilter:  filter2,
	}
	segments := []*Segment{segment1, segment2, segment3, segment4, segment5}
	results, err := getSegmentsByPKs([]int64{0, 1, 2, 3, 4}, segments)
	assert.Nil(t, err)
	expected := map[int64][]int64{
		1: {0, 1, 2},
		2: {0, 1, 2},
		3: {0, 1, 2},
		4: {3, 4},
		5: {3, 4},
	}
	assert.Equal(t, expected, results)

	_, err = getSegmentsByPKs(nil, segments)
	assert.NotNil(t, err)
	_, err = getSegmentsByPKs([]int64{0, 1, 2, 3, 4}, nil)
	assert.NotNil(t, err)
}
