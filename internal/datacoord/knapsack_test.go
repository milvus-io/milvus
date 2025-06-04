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
	"testing"

	"github.com/stretchr/testify/assert"
)

type element struct {
	size         int64
	id           int64
	residualSize int64
}

var _ Sizable = (*element)(nil)

func (e *element) getSegmentSize() int64 {
	return e.size
}

func (e *element) GetResidualSegmentSize() int64 {
	return e.size
}

func (e *element) GetEarliestTs() uint64 {
	return 0
}

func (e *element) GetID() int64 {
	return e.id
}

func mockElements(size ...int64) []*element {
	var candidates []*element
	for _, s := range size {
		candidates = append(candidates, &element{size: s, id: s})
	}
	return candidates
}

func Test_pack(t *testing.T) {
	type args struct {
		size     int64
		leftSize int64
		minSegs  int64
		maxSegs  int64
	}
	tests := []struct {
		name       string
		candidates []*element
		args       args
		want       []*element
		wantLeft   int64
	}{
		{
			name:       "all",
			candidates: mockElements(1, 2, 3, 4, 5),
			args: args{
				size:     15,
				leftSize: 0,
				minSegs:  2,
				maxSegs:  5,
			},
			want:     mockElements(5, 4, 3, 2, 1),
			wantLeft: 0,
		},
		{
			name:       "failed by left size",
			candidates: mockElements(1, 2, 3, 4, 5),
			args: args{
				size:     20,
				leftSize: 4,
				minSegs:  2,
				maxSegs:  5,
			},
			want:     mockElements(),
			wantLeft: 20,
		},
		{
			name:       "failed by min segs",
			candidates: mockElements(10, 10),
			args: args{
				size:     20,
				leftSize: 5,
				minSegs:  3,
				maxSegs:  5,
			},
			want:     mockElements(),
			wantLeft: 20,
		},
		{
			name:       "failed by max segs",
			candidates: mockElements(5, 5, 5, 5),
			args: args{
				size:     20,
				leftSize: 4,
				minSegs:  2,
				maxSegs:  3,
			},
			want:     mockElements(),
			wantLeft: 20,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newKnapsack[*element](tt.name, tt.candidates, func(a, b *element) bool {
				if a.GetResidualSegmentSize() != b.GetResidualSegmentSize() {
					return a.GetResidualSegmentSize() > b.GetResidualSegmentSize()
				} else if a.getSegmentSize() != b.getSegmentSize() {
					return a.getSegmentSize() > b.getSegmentSize()
				} else if a.GetEarliestTs() != b.GetEarliestTs() {
					return a.GetEarliestTs() < b.GetEarliestTs()
				}
				return a.GetID() < b.GetID()
			})
			got, left := p.pack(tt.args.size, tt.args.leftSize, tt.args.minSegs, tt.args.maxSegs)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantLeft, left)
		})
	}
}

type timeElement struct {
	element
	earliestTs   uint64
	residualSize int64
}

func (e *timeElement) GetEarliestTs() uint64         { return e.earliestTs }
func (e *timeElement) GetResidualSegmentSize() int64 { return e.residualSize }

var _ Sizable = (*timeElement)(nil)

func Test_newKnapsackTimeBased(t *testing.T) {
	type args struct {
		size     int64
		leftSize int64
		minSegs  int64
		maxSegs  int64
	}
	mockTimeElements := func(params ...struct {
		size         int64
		earliestTs   uint64
		residualSize int64
	},
	) []*timeElement {
		var out []*timeElement
		for i, p := range params {
			out = append(out, &timeElement{
				element:      element{size: p.size, id: int64(i)},
				earliestTs:   p.earliestTs,
				residualSize: p.residualSize,
			})
		}
		return out
	}

	tests := []struct {
		name       string
		candidates []*timeElement
		args       args
		init_order []int64
		pack_order []int64
	}{
		{
			name: "sort by earliestTs",
			candidates: mockTimeElements(
				struct {
					size         int64
					earliestTs   uint64
					residualSize int64
				}{size: 10, earliestTs: 10, residualSize: 5},
				struct {
					size         int64
					earliestTs   uint64
					residualSize int64
				}{size: 20, earliestTs: 5, residualSize: 8},
				struct {
					size         int64
					earliestTs   uint64
					residualSize int64
				}{size: 30, earliestTs: 5, residualSize: 8},
			),
			args:       args{size: 60, leftSize: 60, minSegs: 1, maxSegs: 3},
			init_order: []int64{2, 1, 0},
			pack_order: []int64{2, 1, 0},
		},
		{
			name: "sort by residualSize",
			candidates: mockTimeElements(
				struct {
					size         int64
					earliestTs   uint64
					residualSize int64
				}{size: 100, earliestTs: 5, residualSize: 80},
				struct {
					size         int64
					earliestTs   uint64
					residualSize int64
				}{size: 101, earliestTs: 5, residualSize: 60},
				struct {
					size         int64
					earliestTs   uint64
					residualSize int64
				}{size: 102, earliestTs: 5, residualSize: 60},
			),
			args:       args{size: 150, leftSize: 100, minSegs: 1, maxSegs: 3},
			init_order: []int64{0, 2, 1},
			pack_order: []int64{0, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newKnapsack[*timeElement](tt.name, tt.candidates, func(a, b *timeElement) bool {
				if a.GetEarliestTs() != b.GetEarliestTs() {
					return a.GetEarliestTs() < b.GetEarliestTs()
				} else if a.GetResidualSegmentSize() != b.GetResidualSegmentSize() {
					return a.GetResidualSegmentSize() > b.GetResidualSegmentSize()
				} else if a.getSegmentSize() != b.getSegmentSize() {
					return a.getSegmentSize() > b.getSegmentSize()
				}
				return a.GetID() < b.GetID()
			})
			for i, candidate := range p.candidates {
				assert.Equal(t, tt.init_order[i], candidate.id)
			}
			got, _ := p.pack(tt.args.size, tt.args.leftSize, tt.args.minSegs, tt.args.maxSegs)
			assert.Equal(t, len(tt.pack_order), len(got))
			for i, candidate := range got {
				assert.Equal(t, tt.pack_order[i], candidate.id)
			}
		})
	}
}
