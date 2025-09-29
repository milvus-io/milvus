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

package index

var _ Index = rtreeIndex{}

// rtreeIndex represents an RTree index for geometry fields
type rtreeIndex struct {
	baseIndex
}

func (idx rtreeIndex) Params() map[string]string {
	params := map[string]string{
		IndexTypeKey: string(RTREE),
	}
	return params
}

// NewRTreeIndex creates a new RTree index with default parameters
func NewRTreeIndex() Index {
	return rtreeIndex{
		baseIndex: baseIndex{
			indexType: RTREE,
		},
	}
}

// NewRTreeIndexWithParams creates a new RTree index with custom parameters
func NewRTreeIndexWithParams() Index {
	return rtreeIndex{
		baseIndex: baseIndex{
			indexType: RTREE,
		},
	}
}

// RTreeIndexBuilder provides a fluent API for building RTree indexes
type RTreeIndexBuilder struct {
	index rtreeIndex
}

// NewRTreeIndexBuilder creates a new RTree index builder
func NewRTreeIndexBuilder() *RTreeIndexBuilder {
	return &RTreeIndexBuilder{
		index: rtreeIndex{
			baseIndex: baseIndex{
				indexType: RTREE,
			},
		},
	}
}

// Build returns the constructed RTree index
func (b *RTreeIndexBuilder) Build() Index {
	return b.index
}
