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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/scalar/sort/IndexStructure.h"

// Numeric and string indexes share one public reader. Their distinct persisted
// layouts and search algorithms remain private typed storage views.

namespace milvus::index {

// Owns one loader-created local file and its read-only mapping. Readers retain
// this object through type-erased/shared layout ownership; destruction unmaps
// before unlinking the file.
class SortedMmapOwner final {
 public:
    SortedMmapOwner(char* data,
                    size_t mapped_size,
                    size_t logical_size,
                    std::string path);
    ~SortedMmapOwner();

    SortedMmapOwner(const SortedMmapOwner&) = delete;
    SortedMmapOwner&
    operator=(const SortedMmapOwner&) = delete;

    const uint8_t*
    Data() const;

    size_t
    MappedSize() const;

    size_t
    LogicalSize() const;

 private:
    char* data_{nullptr};
    size_t mapped_size_{0};
    size_t logical_size_{0};
    std::string path_;
};

struct SortedStringPostingView {
    const uint8_t* data{nullptr};
    size_t size{0};

    uint32_t
    At(size_t index) const;
};

// Parsed string dictionary/posting representation shared by the reader and
// artifact serializer. Implementations may own heap vectors or packed mmap
// bytes; query signatures never expose that load-time choice.
class SortedStringLayout {
 public:
    virtual ~SortedStringLayout() = default;

    virtual size_t
    UniqueCount() const = 0;

    virtual std::string_view
    Value(size_t index) const = 0;

    virtual SortedStringPostingView
    Posting(size_t index) const = 0;

    virtual int64_t
    MemoryUsage() const = 0;

    virtual int64_t
    FileUsage() const = 0;

    std::vector<int32_t>
    BuildOffsets(size_t total_num_rows) const;

    static std::shared_ptr<const SortedStringLayout>
    FromHeap(std::vector<std::string> unique_values,
             std::vector<std::vector<uint32_t>> posting_lists,
             size_t total_num_rows);

    static std::shared_ptr<const SortedStringLayout>
    FromPackedHeap(std::vector<uint8_t> packed, size_t total_num_rows);

    static std::shared_ptr<const SortedStringLayout>
    FromPackedMmap(std::shared_ptr<SortedMmapOwner> owner,
                   size_t total_num_rows);
};

namespace sorted_reader_detail {

struct SortedReaderState {
    std::shared_ptr<const TargetBitmap> valid_bitset;
    const int32_t* idx_to_offsets{nullptr};
    size_t idx_to_offsets_size{0};
    std::shared_ptr<const void> idx_to_offsets_owner;
    size_t idx_to_offsets_heap_bytes{0};
    size_t idx_to_offsets_file_bytes{0};
    size_t total_num_rows{0};
    DataType value_type{DataType::NONE};
    bool nested{false};
    bool value_lookup{true};
};

template <typename T>
class SortedStorageView {
 public:
    static_assert(std::is_arithmetic_v<T>);

    struct OpenArgs {
        const IndexStructure<T>* data{nullptr};
        size_t size{0};
        std::shared_ptr<const void> data_owner;
        size_t data_heap_bytes{0};
        size_t data_file_bytes{0};
    };

    explicit SortedStorageView(OpenArgs args);

    void
    SetEqual(TargetBitmap& result, const T& value) const;

    void
    ClearEqual(TargetBitmap& result, const T& value) const;

    TargetBitmap
    Range(size_t count,
          const TargetBitmap& validity,
          bool value_lookup,
          const T& value,
          CompareOp op) const;

    TargetBitmap
    Range(size_t count,
          const TargetBitmap& validity,
          bool value_lookup,
          const T& lo,
          bool lo_inc,
          const T& hi,
          bool hi_inc) const;

    T
    ValueAt(size_t index) const;

    bool
    ValidReverseIndex(int32_t index) const;

    bool
    Empty() const;

    int64_t
    MemoryUsage() const;

    int64_t
    FileUsage() const;

 private:
    bool
    ShouldSkip(const T& lower, const T& upper, CompareOp op) const;

    OpenArgs data_;
};

template <>
class SortedStorageView<std::string_view> {
 public:
    struct OpenArgs {
        std::shared_ptr<const SortedStringLayout> layout;
    };

    explicit SortedStorageView(OpenArgs args);

    void
    SetEqual(TargetBitmap& result, const std::string_view& value) const;

    void
    ClearEqual(TargetBitmap& result, const std::string_view& value) const;

    TargetBitmap
    Range(size_t count,
          const TargetBitmap& validity,
          bool value_lookup,
          const std::string_view& value,
          CompareOp op) const;

    TargetBitmap
    Range(size_t count,
          const TargetBitmap& validity,
          bool value_lookup,
          const std::string_view& lo,
          bool lo_inc,
          const std::string_view& hi,
          bool hi_inc) const;

    std::string_view
    ValueAt(size_t index) const;

    bool
    ValidReverseIndex(int32_t index) const;

    int64_t
    MemoryUsage() const;

    int64_t
    FileUsage() const;

    TargetBitmap
    PatternMatch(size_t count,
                 std::string_view pattern,
                 PatternOp op) const;

 private:
    std::shared_ptr<const SortedStringLayout> layout_;
};

}  // namespace sorted_reader_detail

template <typename T>
class SortedIndexReader final
    : public IIndexReaderBase,
      public IScalarPredicateReader<T>,
      public IScalarValueReader<T>,
      public INullReader,
      public PatternMatchReaderAdapter<SortedIndexReader<T>, T> {
 public:
    static_assert(std::is_arithmetic_v<T> ||
                  std::is_same_v<T, std::string_view>);

    struct OpenArgs {
        sorted_reader_detail::SortedReaderState state;
        typename sorted_reader_detail::SortedStorageView<T>::OpenArgs storage;
    };

    explicit SortedIndexReader(OpenArgs args);

    ~SortedIndexReader() override;

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    cachinglayer::ResourceUsage
    CellByteSize() const override;

    TargetBitmap
    In(size_t n, const T* values) const override;

    TargetBitmap
    NotIn(size_t n, const T* values) const override;

    TargetBitmap
    Range(const T& value, CompareOp op) const override;

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override;

    std::optional<owned_t<T>>
    Lookup(int64_t offset) const override;

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t i, const T*, bool valid)>& out)
        const override;

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    template <typename Derived, typename U, bool DelegateShouldUseForOp>
    friend class PatternMatchReaderAdapter;

    template <typename U = T,
              std::enable_if_t<std::is_same_v<U, std::string_view>, int> = 0>
    TargetBitmap
    PatternMatchImpl(std::string_view pattern, PatternOp op) const {
        return storage_.PatternMatch(state_.total_num_rows, pattern, op);
    }

    sorted_reader_detail::SortedReaderState state_;
    sorted_reader_detail::SortedStorageView<T> storage_;
};

}  // namespace milvus::index
