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

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

#include "index/scalar/bsi/BitSlicedIndex.h"
#include "storage/artifact/Artifact.h"

// Persistence of BitSlicedIndex<T> through the index-file envelopes the other
// scalar families use: named byte entries, written to a storage::FileSink
// (V1/V2 layout) or a storage::IndexEntryWriter (packed V3 layout) and read
// back from the matching storage::FileSource or storage::IndexEntryReader.
// The entries are the same in both layouts; only the envelope differs.
//
// Layout, version 1. 2 + slice_count entries:
//   bsi_header     bsi_format::Header, 40 bytes.
//   bsi_existence  The existence bitmap: ceil(num_rows / 64) 64-bit words.
//   bsi_slice_NN   Slice NN (two decimal digits, 00 <= NN < slice_count), the
//                  same size as the existence bitmap. A bsi_slice_ entry with
//                  any other NN is rejected; entries with other names belong
//                  to the envelope and are ignored.
// One entry per slice keeps both directions copy-free per slice: Serialize
// hands each bitmap's words to the sink as they are, and the loader holds at
// most one slice's bytes in transit. Bitmap words are the kernel's in-memory
// words (row r is bit r % 64 of word r / 64; bits past num_rows in the last
// word are 0). Integers and words are in host byte order, like the numeric
// keys of the bitmap family; Milvus only runs on little-endian hosts.
//
// Loading is bounded: the header is read first, and the payload entries are
// read only after their sizes have been checked against the row and slice
// counts it declares, so a truncated or inflated entry is rejected before any
// bitmap is allocated. The decoded state then goes through
// BitSlicedIndex<T>::FromState, which rejects states that break the encoding
// invariants. An unknown version, or a value kind or width this
// instantiation does not hold, fails with Unsupported; every other defect
// fails with DataFormatBroken.

namespace milvus::storage {
class FileSource;
class IndexEntryReader;
}  // namespace milvus::storage

namespace milvus::index {

namespace bsi_format {

constexpr uint32_t kVersion = 1;

// Header::value_kind. The kernel holds signed integers only; the tag keeps a
// future kind from being decoded as one.
constexpr uint32_t kSignedInteger = 1;

constexpr std::string_view kHeaderEntry = "bsi_header";
constexpr std::string_view kExistenceEntry = "bsi_existence";

// "bsi_slice_NN" for slice `index` (< BitSlicedIndex<T>::kMaxSlices <= 32).
std::string
SliceEntryName(size_t index);

// The version comes first so that a reader can recognize a header of another
// version, whatever its size.
struct Header {
    // kVersion.
    uint32_t version;
    // kSignedInteger.
    uint32_t value_kind;
    // Width in bits of the value type: 8, 16 or 32.
    uint32_t value_bits;
    // bit_width(max - min); 0 when there are no valid rows.
    uint32_t slice_count;
    // Rows, including NULL rows.
    uint64_t num_rows;
    // Bounds over valid rows; both 0 when there are none.
    int64_t min;
    int64_t max;
};
static_assert(sizeof(Header) == 40, "bsi header layout changed");
static_assert(offsetof(Header, version) == 0, "version must come first");
static_assert(std::is_trivially_copyable_v<Header>);

}  // namespace bsi_format

// Build result of a bit-sliced index: owns the index and writes the layout
// above. The index can be queried through Index() until the artifact is
// destroyed.
template <BitSlicedIndexValue T>
class BitSlicedIndexArtifact final : public storage::Artifact {
 public:
    explicit BitSlicedIndexArtifact(BitSlicedIndex<T> index);
    ~BitSlicedIndexArtifact() override = default;

    void
    Serialize(storage::FileSink& sink) const override;

    void
    Serialize(storage::IndexEntryWriter& writer) const override;

    const BitSlicedIndex<T>&
    Index() const {
        return index_;
    }

 private:
    // Both outputs take (name, data, size); the layout is written once here.
    template <typename Output>
    void
    SerializeImpl(Output& out) const;

    BitSlicedIndex<T> index_;
};

// Reloads an index that BitSlicedIndexArtifact<T>::Serialize wrote. Peak
// memory is the index plus either one bitmap entry in transit (while reading)
// or a few bitmap-sized temporaries (while FromState validates); the two
// phases do not overlap.
template <BitSlicedIndexValue T>
BitSlicedIndex<T>
LoadBitSlicedIndex(storage::FileSource& source);

template <BitSlicedIndexValue T>
BitSlicedIndex<T>
LoadBitSlicedIndex(storage::IndexEntryReader& reader);

// Instantiated in BitSlicedIndexArtifact.cpp.
extern template class BitSlicedIndexArtifact<int32_t>;
extern template BitSlicedIndex<int32_t>
LoadBitSlicedIndex<int32_t>(storage::FileSource& source);
extern template BitSlicedIndex<int32_t>
LoadBitSlicedIndex<int32_t>(storage::IndexEntryReader& reader);

using BitSlicedIndexArtifactInt32 = BitSlicedIndexArtifact<int32_t>;

}  // namespace milvus::index
