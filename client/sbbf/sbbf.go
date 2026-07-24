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

// Package sbbf implements the Parquet Split-Block Bloom Filter (SBBF) wrapped
// in the Milvus MBF1 envelope, as specified by the bloom-filter-expression
// design doc (docs/design-docs/design_docs/20260707-bloom-filter-expression.md).
//
// The bit layout is bit-identical to Arrow C++'s parquet::BlockSplitBloomFilter
// (cpp/src/parquet/bloom_filter.{h,cc}) and therefore to the parquet-format
// BloomFilter.md spec:
//
//   - a filter is a power-of-two number of 32-byte blocks; each block is
//     eight little-endian uint32 words;
//   - values are hashed with XXH64 (seed 0); int64 values hash their 8-byte
//     little-endian encoding, strings hash their raw UTF-8 bytes (this matches
//     Parquet plain encoding for INT64 / BYTE_ARRAY);
//   - block index is the multiply-shift reduction
//     ((hash >> 32) * numBlocks) >> 32;
//   - within the block, one bit is set/checked per word i in 0..7 at position
//     (uint32(hash) * salt[i]) >> 27.
//
// MBF1 envelope layout (all integers little-endian):
//
//	offset  size  field
//	0       4     magic "MBF1"
//	4       2     version       (= 1)
//	6       2     algo          (1 = parquet_sbbf_xxh64)
//	8       8     n_declared    (informational)
//	16      8     fpr_declared  (float64, informational)
//	24      4     num_blocks    (body length must equal num_blocks * 32)
//	28      4     reserved      (must be 0)
//	32      ...   body: SBBF blocks
package sbbf

import (
	"encoding/binary"
	"math"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
)

const (
	// Magic is the 4-byte MBF1 envelope magic.
	Magic = "MBF1"
	// Version is the MBF1 envelope version implemented by this package.
	Version uint16 = 1
	// AlgoParquetSBBFXxh64 identifies the parquet SBBF + XXH64 algorithm.
	AlgoParquetSBBFXxh64 uint16 = 1
	// HeaderSize is the size in bytes of the MBF1 envelope header.
	HeaderSize = 32

	// BytesPerBlock is the size of one SBBF block (parquet-format spec).
	BytesPerBlock = 32
	wordsPerBlock = 8

	// MinFilterBytes / MaxFilterBytes mirror Arrow's
	// BlockSplitBloomFilter::kMinimumBloomFilterBytes / kMaximumBloomFilterBytes.
	MinFilterBytes = 32
	MaxFilterBytes = 128 * 1024 * 1024

	// MinFPR / MaxFPR bound the accepted false-positive rate.
	MinFPR = 0.0001
	MaxFPR = 0.05

	// DefaultFPR is the recommended false-positive rate when a caller has no
	// specific target: ~1.38 bytes/member, so a 32 MiB blob (the largest that
	// fits the default 64 MiB gRPC recv limit) holds ~24 M members.
	DefaultFPR = 0.005
)

// salt holds the eight odd constants used to derive one bit position per word
// inside a block. They are fixed by the parquet-format spec and mirrored from
// Arrow C++'s BlockSplitBloomFilter::SALT.
var salt = [wordsPerBlock]uint32{
	0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
	0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
}

// optimalNumOfBytes mirrors Arrow's BlockSplitBloomFilter::OptimalNumOfBytes:
// the classic blocked-bloom sizing formula m = -8n / ln(1 - fpp^(1/8)),
// rounded up to the next power of two and clamped to
// [MinFilterBytes, MaxFilterBytes]. The result is always a power of two and a
// multiple of BytesPerBlock.
func optimalNumOfBytes(ndv uint64, fpp float64) uint32 {
	const (
		minBits = uint32(MinFilterBytes) << 3
		maxBits = uint32(MaxFilterBytes) << 3
	)
	m := -8.0 * float64(ndv) / math.Log(1.0-math.Pow(fpp, 1.0/8.0))

	var numBits uint32
	if m < 0 || m > float64(maxBits) {
		numBits = maxBits
	} else {
		numBits = uint32(m)
	}
	if numBits < minBits {
		numBits = minBits
	}
	// Round up to the next power of two.
	if numBits&(numBits-1) != 0 {
		numBits = nextPower2(numBits)
	}
	if numBits > maxBits {
		numBits = maxBits
	}
	return numBits >> 3
}

// nextPower2 returns the smallest power of two >= v (v > 1, v <= 2^31).
func nextPower2(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

// hashInt64 encodes v as 8 little-endian bytes and returns XXH64(seed=0).
func hashInt64(v int64) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	return xxhash.Sum64(buf[:])
}

// hashString returns XXH64(seed=0) over the raw UTF-8 bytes of s.
func hashString(s string) uint64 {
	return xxhash.Sum64String(s)
}

// blockIndex reduces a hash to a block index via the multiply-shift scheme
// used by Arrow: ((hash >> 32) * numBlocks) >> 32. numBlocks <= 2^22, so the
// product cannot overflow uint64.
func blockIndex(hash uint64, numBlocks uint32) uint32 {
	return uint32(((hash >> 32) * uint64(numBlocks)) >> 32)
}

// Builder incrementally constructs an SBBF and serializes it into an MBF1
// envelope. It is not safe for concurrent use.
type Builder struct {
	words     []uint32
	numBlocks uint32
	nDeclared uint64
	fpr       float64
}

// NewBuilder returns a Builder sized for n distinct values at false-positive
// rate fpr. fpr must lie in [MinFPR, MaxFPR]. The filter size follows Arrow's
// OptimalNumOfBytes (power-of-two bytes, clamped to
// [MinFilterBytes, MaxFilterBytes]).
func NewBuilder(n uint64, fpr float64) (*Builder, error) {
	if math.IsNaN(fpr) || fpr < MinFPR || fpr > MaxFPR {
		return nil, errors.Errorf("bloom filter fpr %v out of range [%v, %v]", fpr, MinFPR, MaxFPR)
	}
	numBytes := optimalNumOfBytes(n, fpr)
	numBlocks := numBytes / BytesPerBlock
	return &Builder{
		words:     make([]uint32, uint64(numBlocks)*wordsPerBlock),
		numBlocks: numBlocks,
		nDeclared: n,
		fpr:       fpr,
	}, nil
}

// NumBlocks returns the number of 32-byte blocks in the filter body.
func (b *Builder) NumBlocks() uint32 {
	return b.numBlocks
}

// EstimateMarshalSize returns the exact number of bytes Marshal() would produce
// for a filter sized for n distinct values at false-positive rate fpr, without
// allocating the filter or hashing any value. Callers can use it to reject an
// over-large filter before spending time and memory building it. Returns an
// error if fpr is out of [MinFPR, MaxFPR].
func EstimateMarshalSize(n uint64, fpr float64) (int, error) {
	if math.IsNaN(fpr) || fpr < MinFPR || fpr > MaxFPR {
		return 0, errors.Errorf("bloom filter fpr %v out of range [%v, %v]", fpr, MinFPR, MaxFPR)
	}
	return HeaderSize + int(optimalNumOfBytes(n, fpr)), nil
}

func (b *Builder) addHash(h uint64) {
	base := blockIndex(h, b.numBlocks) * wordsPerBlock
	key := uint32(h)
	for i := 0; i < wordsPerBlock; i++ {
		mask := uint32(1) << ((key * salt[i]) >> 27)
		b.words[base+uint32(i)] |= mask
	}
}

// AddInt64 inserts an int64 value (8-byte little-endian encoding).
func (b *Builder) AddInt64(v int64) {
	b.addHash(hashInt64(v))
}

// AddString inserts a string value (raw UTF-8 bytes).
func (b *Builder) AddString(s string) {
	b.addHash(hashString(s))
}

// Marshal serializes the filter as an MBF1 envelope (header + blocks).
func (b *Builder) Marshal() []byte {
	out := make([]byte, HeaderSize+len(b.words)*4)
	copy(out[0:4], Magic)
	binary.LittleEndian.PutUint16(out[4:6], Version)
	binary.LittleEndian.PutUint16(out[6:8], AlgoParquetSBBFXxh64)
	binary.LittleEndian.PutUint64(out[8:16], b.nDeclared)
	binary.LittleEndian.PutUint64(out[16:24], math.Float64bits(b.fpr))
	binary.LittleEndian.PutUint32(out[24:28], b.numBlocks)
	binary.LittleEndian.PutUint32(out[28:32], 0) // reserved
	for i, w := range b.words {
		binary.LittleEndian.PutUint32(out[HeaderSize+i*4:], w)
	}
	return out
}

// Filter is a read-only, zero-copy view over an MBF1 blob. The blob must not
// be mutated while the Filter is in use. It is safe for concurrent probing.
type Filter struct {
	body      []byte // num_blocks * 32 bytes, aliasing the parsed blob
	numBlocks uint32
	nDeclared uint64
	fpr       float64
}

// Parse validates an MBF1 blob and returns a zero-copy Filter over it. All
// header fields are validated against the actual blob length before any use,
// so malformed or hostile inputs are rejected without allocation.
func Parse(blob []byte) (*Filter, error) {
	if len(blob) < HeaderSize {
		return nil, errors.Errorf("bloom filter blob too short: %d bytes, need at least %d", len(blob), HeaderSize)
	}
	if string(blob[0:4]) != Magic {
		return nil, errors.Errorf("bloom filter blob has invalid magic %q, expected %q", blob[0:4], Magic)
	}
	if v := binary.LittleEndian.Uint16(blob[4:6]); v != Version {
		return nil, errors.Errorf("unsupported bloom filter version %d, expected %d", v, Version)
	}
	if a := binary.LittleEndian.Uint16(blob[6:8]); a != AlgoParquetSBBFXxh64 {
		return nil, errors.Errorf("unsupported bloom filter algo %d, expected %d", a, AlgoParquetSBBFXxh64)
	}
	if r := binary.LittleEndian.Uint32(blob[28:32]); r != 0 {
		return nil, errors.Errorf("bloom filter reserved field must be 0, got %d", r)
	}
	numBlocks := binary.LittleEndian.Uint32(blob[24:28])
	// SBBF invariant (Arrow OptimalNumOfBytes): filter size is a power of two
	// in [MinFilterBytes, MaxFilterBytes], hence num_blocks is a power of two
	// in [1, MaxFilterBytes/BytesPerBlock].
	if numBlocks == 0 || numBlocks&(numBlocks-1) != 0 || numBlocks > MaxFilterBytes/BytesPerBlock {
		return nil, errors.Errorf("bloom filter num_blocks %d is not a power of two in [1, %d]", numBlocks, MaxFilterBytes/BytesPerBlock)
	}
	if bodyLen := uint64(len(blob) - HeaderSize); bodyLen != uint64(numBlocks)*BytesPerBlock {
		return nil, errors.Errorf("bloom filter body length %d does not match num_blocks %d (want %d bytes)", bodyLen, numBlocks, uint64(numBlocks)*BytesPerBlock)
	}
	return &Filter{
		body:      blob[HeaderSize:],
		numBlocks: numBlocks,
		nDeclared: binary.LittleEndian.Uint64(blob[8:16]),
		fpr:       math.Float64frombits(binary.LittleEndian.Uint64(blob[16:24])),
	}, nil
}

// NDeclared returns the declared (informational) number of inserted values.
func (f *Filter) NDeclared() uint64 {
	return f.nDeclared
}

// FPRDeclared returns the declared (informational) false-positive rate.
func (f *Filter) FPRDeclared() float64 {
	return f.fpr
}

// NumBlocks returns the number of 32-byte blocks in the filter body.
func (f *Filter) NumBlocks() uint32 {
	return f.numBlocks
}

func (f *Filter) testHash(h uint64) bool {
	blockOff := int(blockIndex(h, f.numBlocks)) * BytesPerBlock
	key := uint32(h)
	for i := 0; i < wordsPerBlock; i++ {
		mask := uint32(1) << ((key * salt[i]) >> 27)
		word := binary.LittleEndian.Uint32(f.body[blockOff+i*4:])
		if word&mask == 0 {
			return false
		}
	}
	return true
}

// TestInt64 reports whether v may be in the set. False means definitely absent.
func (f *Filter) TestInt64(v int64) bool {
	return f.testHash(hashInt64(v))
}

// TestString reports whether s may be in the set. False means definitely absent.
func (f *Filter) TestString(s string) bool {
	return f.testHash(hashString(s))
}
