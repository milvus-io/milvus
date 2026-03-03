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
	"encoding/binary"

	"github.com/zeebo/xxh3"
)

const (
	bfWordSize   = 32
	bfBlockWords = 16 // BlockBits(512) / wordSize(32)
	bfBlockBytes = bfBlockWords * 4
)

// MmapBloomFilter is a read-only bloom filter backed by mmap'd memory.
// It implements BloomFilterInterface for testing operations only.
// Add operations will panic since this is a read-only view.
type MmapBloomFilter struct {
	data      []byte // mmap'd memory region containing BF block data
	offset    int    // start offset of block data within data
	numBlocks uint32 // number of 512-bit blocks
	k         int    // number of hash functions
}

// NewMmapBloomFilter creates a new MmapBloomFilter from mmap'd data.
// data is the mmap'd memory, offset is where the block data starts,
// numBlocks is the number of 512-bit blocks, and k is the hash count.
func NewMmapBloomFilter(data []byte, offset int, numBlocks uint32, k int) *MmapBloomFilter {
	return &MmapBloomFilter{
		data:      data,
		offset:    offset,
		numBlocks: numBlocks,
		k:         k,
	}
}

func (m *MmapBloomFilter) Type() BFType {
	return BlockedBF
}

func (m *MmapBloomFilter) Cap() uint {
	return uint(m.numBlocks) * 512 // BlockBits per block
}

func (m *MmapBloomFilter) K() uint {
	return uint(m.k)
}

func (m *MmapBloomFilter) Add(_ []byte) {
	panic("MmapBloomFilter is read-only, Add not supported")
}

func (m *MmapBloomFilter) AddString(_ string) {
	panic("MmapBloomFilter is read-only, AddString not supported")
}

func (m *MmapBloomFilter) Test(data []byte) bool {
	h := xxh3.Hash(data)
	return m.testHash(h)
}

func (m *MmapBloomFilter) TestString(data string) bool {
	h := xxh3.HashString(data)
	return m.testHash(h)
}

func (m *MmapBloomFilter) TestLocations(locs []uint64) bool {
	if len(locs) != 1 {
		return true
	}
	return m.testHash(locs[0])
}

func (m *MmapBloomFilter) BatchTestLocations(locs [][]uint64, hits []bool) []bool {
	ret := make([]bool, len(locs))
	n := len(hits)
	if len(locs) < n {
		n = len(locs)
	}
	for i := 0; i < n; i++ {
		if !hits[i] {
			if len(locs[i]) != 1 {
				ret[i] = true
				continue
			}
			ret[i] = m.testHash(locs[i][0])
		}
	}
	return ret
}

func (m *MmapBloomFilter) MarshalJSON() ([]byte, error) {
	panic("MmapBloomFilter is read-only, MarshalJSON not supported")
}

func (m *MmapBloomFilter) UnmarshalJSON(_ []byte) error {
	panic("MmapBloomFilter is read-only, UnmarshalJSON not supported")
}

// testHash implements the blocked bloom filter Has logic, directly reading
// from the mmap'd byte slice. This mirrors blobloom's Filter.Has().
func (m *MmapBloomFilter) testHash(h uint64) bool {
	h1, h2 := uint32(h>>32), uint32(h)
	blockIdx := reducerange(h2, m.numBlocks)
	blockOff := m.offset + int(blockIdx)*bfBlockBytes

	for i := 1; i < m.k; i++ {
		h1, h2 = doublehash(h1, h2, i)
		wordIdx := (h1 / bfWordSize) % bfBlockWords
		bitPos := h1 % bfWordSize
		word := binary.LittleEndian.Uint32(m.data[blockOff+int(wordIdx)*4:])
		if word&(1<<bitPos) == 0 {
			return false
		}
	}
	return true
}

// reducerange maps i to an integer in the range [0,n).
// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func reducerange(i, n uint32) uint32 {
	return uint32((uint64(i) * uint64(n)) >> 32)
}

// doublehash generates the hash values for enhanced double hashing.
// See https://www.ccs.neu.edu/home/pete/pub/bloom-filters-verification.pdf
func doublehash(h1, h2 uint32, i int) (uint32, uint32) {
	h1 = h1 + h2
	h2 = h2 + uint32(i)
	return h1, h2
}
