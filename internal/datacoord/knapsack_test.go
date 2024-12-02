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
	size int64
	id   int64
}

var _ Sizable = (*element)(nil)

func (e *element) getSegmentSize() int64 {
	return e.size
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
			p := newKnapsack[*element](tt.name, tt.candidates)
			got, left := p.pack(tt.args.size, tt.args.leftSize, tt.args.minSegs, tt.args.maxSegs)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantLeft, left)
		})
	}
}
