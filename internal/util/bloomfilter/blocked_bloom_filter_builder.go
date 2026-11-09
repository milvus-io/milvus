// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bloomfilter

import (
	"github.com/zeebo/xxh3"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// BlockedBloomFilterBuilder caches hashes and builds a filter sized by their count.
type BlockedBloomFilterBuilder struct {
	hashes *varSizeBlockBuffer[uint64]
	fp     float64
}

// builderHashBlockSize is the number of hashes per cache block, 512 KiB per block.
const builderHashBlockSize = 64 * 1024

func NewBlockedBloomFilterBuilder(capacityHint uint, fp float64) (*BlockedBloomFilterBuilder, error) {
	if fp <= 0 || fp >= 1 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid bloom false positive rate %v", fp)
	}
	hashes, err := newVarSizeBlockBuffer[uint64](builderHashBlockSize, int64(capacityHint))
	if err != nil {
		return nil, err
	}
	return &BlockedBloomFilterBuilder{hashes: hashes, fp: fp}, nil
}

func (b *BlockedBloomFilterBuilder) Reserve(additional int64) error {
	return b.hashes.Reserve(additional)
}

func (b *BlockedBloomFilterBuilder) Add(data []byte) {
	b.hashes.Append(xxh3.Hash(data))
}

func (b *BlockedBloomFilterBuilder) AddString(data string) {
	b.hashes.Append(xxh3.HashString(data))
}

// Build leaves the cached hashes intact so serialization can be retried.
func (b *BlockedBloomFilterBuilder) Build() (BloomFilterInterface, error) {
	if b.hashes.Len() == 0 {
		return nil, merr.WrapErrServiceInternalMsg("blocked bloom filter builder has no data to build")
	}
	bf := newBlockedBloomFilter(uint(b.hashes.Len()), b.fp)
	for block := range b.hashes.Blocks() {
		for _, h := range block {
			bf.inner.Add(h)
		}
	}
	return bf, nil
}
