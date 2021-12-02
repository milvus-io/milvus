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

package indexcoord

import "github.com/milvus-io/milvus/internal/proto/commonpb"

type PeekClientPolicy func(memorySize uint64, indexParams []*commonpb.KeyValuePair,
	typeParams []*commonpb.KeyValuePair, pq *PriorityQueue) UniqueID

func PeekClientV0(memorySize uint64, indexParams []*commonpb.KeyValuePair,
	typeParams []*commonpb.KeyValuePair, pq *PriorityQueue) UniqueID {
	return pq.items[0].key
}

func PeekClientV1(memorySize uint64, indexParams []*commonpb.KeyValuePair,
	typeParams []*commonpb.KeyValuePair, pq *PriorityQueue) UniqueID {
	for i := range pq.items {
		if pq.items[i].totalMem > memorySize {
			return pq.items[i].key
		}
	}
	return UniqueID(-1)
}
