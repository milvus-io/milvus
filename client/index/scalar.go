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

type scalarIndex struct {
	name      string
	indexType IndexType
}

func (idx scalarIndex) Name() string {
	return idx.name
}

func (idx scalarIndex) IndexType() IndexType {
	return idx.indexType
}

func (idx scalarIndex) Params() map[string]string {
	return map[string]string{
		IndexTypeKey: string(idx.indexType),
	}
}

var _ Index = scalarIndex{}

func NewTrieIndex() Index {
	return scalarIndex{
		indexType: Trie,
	}
}

func NewInvertedIndex() Index {
	return scalarIndex{
		indexType: Inverted,
	}
}

func NewSortedIndex() Index {
	return scalarIndex{
		indexType: Sorted,
	}
}

func NewBitmapIndex() Index {
	return scalarIndex{
		indexType: BITMAP,
	}
}
