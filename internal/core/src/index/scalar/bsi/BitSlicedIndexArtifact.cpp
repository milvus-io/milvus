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

#include "index/scalar/bsi/BitSlicedIndexArtifact.h"

#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/FileSource.h"

namespace milvus::index {

namespace bsi_format {

std::string
SliceEntryName(size_t index) {
    AssertInfo(
        index < 100, "bit-sliced index slice {} has no entry name", index);
    std::string name = "bsi_slice_00";
    name[name.size() - 2] = static_cast<char>('0' + index / 10);
    name[name.size() - 1] = static_cast<char>('0' + index % 10);
    return name;
}

}  // namespace bsi_format

namespace {

using Word = uint64_t;
constexpr size_t kWordBits = 64;
constexpr size_t kWordBytes = sizeof(Word);
static_assert(sizeof(size_t) >= sizeof(uint64_t),
              "persisted row counts must fit in size_t");

// Words needed for `rows` bits, without overflowing near the top of uint64_t.
constexpr uint64_t
WordsFor(uint64_t rows) {
    return rows / kWordBits + (rows % kWordBits != 0 ? 1 : 0);
}

// ---------------------------------------------------------------------------
// Load. The two readable envelopes differ only in how an entry is addressed,
// so each gets a thin adapter and the decoding is written once.
// ---------------------------------------------------------------------------

class FileSourceEntries {
 public:
    explicit FileSourceEntries(storage::FileSource& source) : source_(source) {
    }

    bool
    Has(std::string_view name) const {
        return source_.HasEntry(name);
    }

    uint64_t
    Size(std::string_view name) const {
        const auto size = source_.EntrySize(name);
        // Every source reports failures by throwing; a negative size is a
        // source bug, not a property of the artifact.
        AssertInfo(
            size >= 0, "bit-sliced index entry {} reports size {}", name, size);
        return static_cast<uint64_t>(size);
    }

    std::vector<uint8_t>
    Read(std::string_view name) {
        return source_.ReadEntry(name);
    }

    std::vector<std::string>
    Names() const {
        return source_.EntryNames();
    }

 private:
    storage::FileSource& source_;
};

class PackedEntries {
 public:
    explicit PackedEntries(storage::IndexEntryReader& reader)
        : reader_(reader) {
    }

    bool
    Has(std::string_view name) const {
        return reader_.Directory().HasEntry(name);
    }

    uint64_t
    Size(std::string_view name) const {
        return reader_.GetEntrySize(std::string(name));
    }

    std::vector<uint8_t>
    Read(std::string_view name) {
        return std::move(reader_.ReadEntry(std::string(name)).data);
    }

    std::vector<std::string>
    Names() const {
        std::vector<std::string> names;
        for (const auto& entry : reader_.Directory().Entries()) {
            names.push_back(entry.name);
        }
        return names;
    }

 private:
    storage::IndexEntryReader& reader_;
};

template <typename Entries>
std::vector<uint8_t>
ReadEntry(Entries& entries, std::string_view name) {
    if (!entries.Has(name)) {
        ThrowInfo(
            DataFormatBroken, "bit-sliced index entry {} is missing", name);
    }
    return entries.Read(name);
}

// Reads an entry whose size is already known from the header. The size is
// checked before the read, so a source never materializes more than the
// header accounts for.
template <typename Entries>
std::vector<uint8_t>
ReadEntryOfSize(Entries& entries, std::string_view name, uint64_t expected) {
    if (!entries.Has(name)) {
        ThrowInfo(
            DataFormatBroken, "bit-sliced index entry {} is missing", name);
    }
    const auto size = entries.Size(name);
    if (size != expected) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index entry {} has {} bytes, expected {}",
                  name,
                  size,
                  expected);
    }
    auto bytes = entries.Read(name);
    if (bytes.size() != expected) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index entry {} read {} bytes, expected {}",
                  name,
                  bytes.size(),
                  expected);
    }
    return bytes;
}

// The header is the only entry whose size is not known up front: another
// version may have another size, and it must still be reported as another
// version. So the version is decoded from the first bytes before the size
// of this version's header is required.
template <BitSlicedIndexValue T, typename Entries>
bsi_format::Header
ReadHeader(Entries& entries) {
    const auto bytes = ReadEntry(entries, bsi_format::kHeaderEntry);
    uint32_t version = 0;
    if (bytes.size() < sizeof(version)) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index header has {} bytes, expected at least {}",
                  bytes.size(),
                  sizeof(version));
    }
    std::memcpy(&version, bytes.data(), sizeof(version));
    if (version != bsi_format::kVersion) {
        ThrowInfo(Unsupported,
                  "unsupported bit-sliced index version {}, expected {}",
                  version,
                  bsi_format::kVersion);
    }
    bsi_format::Header header;
    if (bytes.size() != sizeof(header)) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index header has {} bytes, expected {}",
                  bytes.size(),
                  sizeof(header));
    }
    std::memcpy(&header, bytes.data(), sizeof(header));

    constexpr uint32_t kValueBits = sizeof(T) * 8;
    if (header.value_kind != bsi_format::kSignedInteger ||
        header.value_bits != kValueBits) {
        ThrowInfo(Unsupported,
                  "bit-sliced index holds values of kind {} with {} bits, "
                  "expected kind {} with {} bits",
                  header.value_kind,
                  header.value_bits,
                  bsi_format::kSignedInteger,
                  kValueBits);
    }
    if (header.slice_count > BitSlicedIndex<T>::kMaxSlices) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index header declares {} slices, at most {}",
                  header.slice_count,
                  BitSlicedIndex<T>::kMaxSlices);
    }
    if (header.min < std::numeric_limits<T>::min() ||
        header.max > std::numeric_limits<T>::max()) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index bounds [{}, {}] exceed the {}-bit range",
                  header.min,
                  header.max,
                  kValueBits);
    }
    return header;
}

// Reads one bitmap entry of `rows` bits and `words` words.
template <typename Entries>
TargetBitmap
ReadBitmap(Entries& entries, std::string_view name, size_t rows, size_t words) {
    const auto bytes = ReadEntryOfSize(entries, name, words * kWordBytes);
    TargetBitmap bitmap(rows, false);
    AssertInfo(bitmap.size_in_elements() == words,
               "bit-sliced index {} needs {} words, computed {}",
               name,
               bitmap.size_in_elements(),
               words);
    if (words != 0) {
        std::memcpy(bitmap.data(), bytes.data(), words * kWordBytes);
    }
    // The kernel keeps the bits past the last row clear and count() relies
    // on it, so stray bits are a format defect rather than something to mask.
    const size_t tail = rows % kWordBits;
    if (tail != 0 && (bitmap.data()[words - 1] >> tail) != 0) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index {} has bits past row {}",
                  name,
                  rows);
    }
    return bitmap;
}

// A slice entry the header does not account for means the header and the
// payload disagree. Other names belong to the envelope (V3 keeps its own
// metadata entry) and are left alone.
template <typename Entries>
void
RejectUnexpectedSliceEntries(const Entries& entries, uint32_t slice_count) {
    constexpr std::string_view kSlicePrefix = "bsi_slice_";
    for (const auto& name : entries.Names()) {
        if (!name.starts_with(kSlicePrefix)) {
            continue;
        }
        bool expected = false;
        for (uint32_t s = 0; s < slice_count && !expected; ++s) {
            expected = name == bsi_format::SliceEntryName(s);
        }
        if (!expected) {
            ThrowInfo(DataFormatBroken,
                      "bit-sliced index has entry {} but the header declares "
                      "{} slices",
                      name,
                      slice_count);
        }
    }
}

template <BitSlicedIndexValue T, typename Entries>
BitSlicedIndex<T>
LoadFromEntries(Entries& entries) {
    const auto header = ReadHeader<T>(entries);
    RejectUnexpectedSliceEntries(entries, header.slice_count);
    const auto rows = static_cast<size_t>(header.num_rows);
    const auto words = static_cast<size_t>(WordsFor(header.num_rows));

    typename BitSlicedIndex<T>::State state;
    state.num_rows = rows;
    state.min = static_cast<T>(header.min);
    state.max = static_cast<T>(header.max);
    state.existence =
        ReadBitmap(entries, bsi_format::kExistenceEntry, rows, words);
    state.slices.reserve(header.slice_count);
    for (uint32_t s = 0; s < header.slice_count; ++s) {
        state.slices.push_back(
            ReadBitmap(entries, bsi_format::SliceEntryName(s), rows, words));
    }
    return BitSlicedIndex<T>::FromState(std::move(state));
}

}  // namespace

// ---------------------------------------------------------------------------
// Serialize
// ---------------------------------------------------------------------------

template <BitSlicedIndexValue T>
BitSlicedIndexArtifact<T>::BitSlicedIndexArtifact(BitSlicedIndex<T> index)
    : index_(std::move(index)) {
}

template <BitSlicedIndexValue T>
void
BitSlicedIndexArtifact<T>::Serialize(storage::FileSink& sink) const {
    SerializeImpl(sink);
}

template <BitSlicedIndexValue T>
void
BitSlicedIndexArtifact<T>::Serialize(storage::IndexEntryWriter& writer) const {
    SerializeImpl(writer);
}

template <BitSlicedIndexValue T>
template <typename Output>
void
BitSlicedIndexArtifact<T>::SerializeImpl(Output& out) const {
    const bsi_format::Header header{
        .version = bsi_format::kVersion,
        .value_kind = bsi_format::kSignedInteger,
        .value_bits = static_cast<uint32_t>(sizeof(T) * 8),
        .slice_count = static_cast<uint32_t>(index_.SliceCount()),
        .num_rows = static_cast<uint64_t>(index_.Count()),
        .min = index_.Min().value_or(0),
        .max = index_.Max().value_or(0),
    };
    out.WriteEntry(
        std::string(bsi_format::kHeaderEntry), &header, sizeof(header));

    // Bitmaps are handed over as they are: no copy per entry.
    const auto& existence = index_.Existence();
    out.WriteEntry(std::string(bsi_format::kExistenceEntry),
                   existence.data(),
                   existence.size_in_bytes());
    for (size_t s = 0; s < index_.SliceCount(); ++s) {
        const auto& slice = index_.Slice(s);
        out.WriteEntry(
            bsi_format::SliceEntryName(s), slice.data(), slice.size_in_bytes());
    }
}

// ---------------------------------------------------------------------------
// Load entry points
// ---------------------------------------------------------------------------

template <BitSlicedIndexValue T>
BitSlicedIndex<T>
LoadBitSlicedIndex(storage::FileSource& source) {
    FileSourceEntries entries(source);
    return LoadFromEntries<T>(entries);
}

template <BitSlicedIndexValue T>
BitSlicedIndex<T>
LoadBitSlicedIndex(storage::IndexEntryReader& reader) {
    PackedEntries entries(reader);
    return LoadFromEntries<T>(entries);
}

template class BitSlicedIndexArtifact<int32_t>;
template BitSlicedIndex<int32_t>
LoadBitSlicedIndex<int32_t>(storage::FileSource& source);
template BitSlicedIndex<int32_t>
LoadBitSlicedIndex<int32_t>(storage::IndexEntryReader& reader);

}  // namespace milvus::index
