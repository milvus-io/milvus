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
	"encoding/json"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cockroachdb/errors"
	"github.com/greatroar/blobloom"
	"github.com/pingcap/log"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"
)

type BFType int

const (
	BlockBFName = "BlockedBloomFilter"
	BasicBFName = "BasicBloomFilter"
)

const (
	UnsupportedBF BFType = iota + 1
	BasicBF
	BlockedBF
)

var bfNames = map[BFType]string{
	BasicBF:   BlockBFName,
	BlockedBF: BasicBFName,
}

func (t BFType) String() string {
	return bfNames[t]
}

func BFTypeFromString(name string) BFType {
	switch name {
	case BasicBFName:
		return BasicBF
	case BlockBFName:
		return BlockedBF
	default:
		return UnsupportedBF
	}
}

type BloomFilterInterface interface {
	Type() BFType
	Cap() uint
	K() uint
	Add(data []byte)
	AddString(data string)
	Test(data []byte) bool
	TestString(data string) bool
	TestLocations(locs []uint64) bool
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
}

type basicBloomFilter struct {
	inner *bloom.BloomFilter
}

func newBasicBloomFilter(capacity uint, fp float64) *basicBloomFilter {
	return &basicBloomFilter{
		inner: bloom.NewWithEstimates(capacity, fp),
	}
}

func (b *basicBloomFilter) Type() BFType {
	return BasicBF
}

func (b *basicBloomFilter) Cap() uint {
	return b.inner.Cap()
}

func (b *basicBloomFilter) K() uint {
	return b.inner.K()
}

func (b *basicBloomFilter) Add(data []byte) {
	b.inner.Add(data)
}

func (b *basicBloomFilter) AddString(data string) {
	b.inner.AddString(data)
}

func (b *basicBloomFilter) Test(data []byte) bool {
	return b.inner.Test(data)
}

func (b *basicBloomFilter) TestString(data string) bool {
	return b.inner.TestString(data)
}

func (b *basicBloomFilter) TestLocations(locs []uint64) bool {
	if len(locs) == 0 {
		// make empty locations false positive
		return true
	}
	return b.inner.TestLocations(locs)
}

func (b basicBloomFilter) MarshalJSON() ([]byte, error) {
	return b.inner.MarshalJSON()
}

func (b *basicBloomFilter) UnmarshalJSON(data []byte) error {
	inner := &bloom.BloomFilter{}
	inner.UnmarshalJSON(data)
	b.inner = inner
	return nil
}

// impl Blocked Bloom filter with blobloom and xx3h hash
type blockedBloomFilter struct {
	inner *blobloom.Filter
}

func newBlockedBloomFilter(capacity uint, fp float64) *blockedBloomFilter {
	return &blockedBloomFilter{
		inner: blobloom.NewOptimized(blobloom.Config{
			Capacity: uint64(capacity),
			FPRate:   fp,
		}),
	}
}

func (b *blockedBloomFilter) Type() BFType {
	return BlockedBF
}

func (b *blockedBloomFilter) Cap() uint {
	return uint(b.inner.NumBits())
}

func (b *blockedBloomFilter) K() uint {
	return b.inner.K()
}

func (b *blockedBloomFilter) Add(data []byte) {
	loc := xxh3.Hash(data)
	b.inner.Add(loc)
}

func (b *blockedBloomFilter) AddString(data string) {
	h := xxh3.HashString(data)
	b.inner.Add(h)
}

func (b *blockedBloomFilter) Test(data []byte) bool {
	loc := xxh3.Hash(data)
	return b.inner.Has(loc)
}

func (b *blockedBloomFilter) TestString(data string) bool {
	h := xxh3.HashString(data)
	return b.inner.Has(h)
}

func (b *blockedBloomFilter) TestLocations(locs []uint64) bool {
	if len(locs) == 0 {
		// make empty locations false positive
		return true
	}
	return b.inner.TestLocations(locs)
}

func (b blockedBloomFilter) MarshalJSON() ([]byte, error) {
	return b.inner.MarshalJSON()
}

func (b *blockedBloomFilter) UnmarshalJSON(data []byte) error {
	inner := &blobloom.Filter{}
	inner.UnmarshalJSON(data)
	b.inner = inner

	return nil
}

func NewBloomFilterWithType(capacity uint, fp float64, typeName string) BloomFilterInterface {
	bfType := BFTypeFromString(typeName)
	switch bfType {
	case BlockedBF:
		return newBlockedBloomFilter(capacity, fp)
	case BasicBF:
		return newBasicBloomFilter(capacity, fp)
	default:
		log.Info("unsupported bloom filter type, using block bloom filter", zap.String("type", typeName))
		return newBlockedBloomFilter(capacity, fp)
	}
}

func UnmarshalJSON(data []byte, bfType BFType) (BloomFilterInterface, error) {
	switch bfType {
	case BlockedBF:
		bf := &blockedBloomFilter{}
		err := json.Unmarshal(data, bf)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal blocked bloom filter")
		}
		return bf, nil
	case BasicBF:
		bf := &basicBloomFilter{}
		err := json.Unmarshal(data, bf)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal blocked bloom filter")
		}
		return bf, nil
	default:
		return nil, errors.Errorf("unsupported bloom filter type: %d", bfType)
	}
}

func Locations(data []byte, k uint, bfType BFType) []uint64 {
	switch bfType {
	case BasicBF:
		return bloom.Locations(data, k)
	case BlockedBF:
		return blobloom.Locations(xxh3.Hash(data), k)
	default:
		log.Info("unsupported bloom filter type, using block bloom filter", zap.String("type", bfType.String()))
		return nil
	}
}
