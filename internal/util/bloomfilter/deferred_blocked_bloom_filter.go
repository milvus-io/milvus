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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// deferredBlockedBloomFilter is a write-only blocked bloom filter whose size is decided at serialization time.
// Add and AddString only cache the xxh3 hashes of the data.
// MarshalJSON builds a blocked bloom filter sized by the number of cached hashes and serializes it,
// so the output is identical to a blockedBloomFilter built with that capacity and readers decode it as BlockedBF.
// Test, TestString, TestLocations, BatchTestLocations, Cap, K and UnmarshalJSON are not supported and panic.
type deferredBlockedBloomFilter struct {
	hashes *typeutil.VarSizeBlockBuffer[uint64]
	fp     float64
}

// deferredHashBlockSize is the number of hashes per cache block, 512 KiB per block.
const deferredHashBlockSize = 64 * 1024

// NewDeferredBlockedBloomFilter creates a write-only blocked bloom filter.
// capacityHint is the expected maximum number of rows; it only sizes the first cache block when smaller than one block,
// and does not affect the size of the built bloom filter.
// It returns an error if fp is not in (0, 1).
func NewDeferredBlockedBloomFilter(capacityHint uint, fp float64) (BloomFilterInterface, error) {
	if fp <= 0 || fp >= 1 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid bloom false positive rate %v", fp)
	}
	hashes, err := typeutil.NewVarSizeBlockBuffer[uint64](deferredHashBlockSize, int64(capacityHint))
	if err != nil {
		return nil, err
	}
	return &deferredBlockedBloomFilter{
		hashes: hashes,
		fp:     fp,
	}, nil
}

func (b *deferredBlockedBloomFilter) Type() BFType {
	return BlockedBF
}

// ReserveSpace reserves hash cache space for a deferred BF; other BF implementations are unchanged.
func ReserveSpace(bf BloomFilterInterface, additional int64) error {
	if deferred, ok := bf.(*deferredBlockedBloomFilter); ok {
		return deferred.hashes.Reserve(additional)
	}
	return nil
}

func (b *deferredBlockedBloomFilter) Add(data []byte) {
	b.hashes.Append(xxh3.Hash(data))
}

func (b *deferredBlockedBloomFilter) AddString(data string) {
	b.hashes.Append(xxh3.HashString(data))
}

// MarshalJSON builds a blocked bloom filter from the cached hashes and serializes it.
// It returns an error if no data has been added.
func (b *deferredBlockedBloomFilter) MarshalJSON() ([]byte, error) {
	if b.hashes.Len() == 0 {
		return nil, merr.WrapErrServiceInternalMsg("deferred blocked bloom filter has no data to build")
	}
	bf := newBlockedBloomFilter(uint(b.hashes.Len()), b.fp)
	for block := range b.hashes.Blocks() {
		for _, h := range block {
			bf.inner.Add(h)
		}
	}
	return bf.MarshalJSON()
}

func (b *deferredBlockedBloomFilter) Cap() uint {
	panic(errDeferredWriteOnly("Cap"))
}

func (b *deferredBlockedBloomFilter) K() uint {
	panic(errDeferredWriteOnly("K"))
}

func (b *deferredBlockedBloomFilter) Test(data []byte) bool {
	panic(errDeferredWriteOnly("Test"))
}

func (b *deferredBlockedBloomFilter) TestString(data string) bool {
	panic(errDeferredWriteOnly("TestString"))
}

func (b *deferredBlockedBloomFilter) TestLocations(locs []uint64) bool {
	panic(errDeferredWriteOnly("TestLocations"))
}

func (b *deferredBlockedBloomFilter) BatchTestLocations(locs [][]uint64, hits []bool) []bool {
	panic(errDeferredWriteOnly("BatchTestLocations"))
}

func (b *deferredBlockedBloomFilter) UnmarshalJSON(data []byte) error {
	panic(errDeferredWriteOnly("UnmarshalJSON"))
}

func errDeferredWriteOnly(method string) error {
	return merr.WrapErrServiceInternalMsg("deferredBlockedBloomFilter is write-only: %s is not supported", method)
}
