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
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>

#include <roaring/roaring.hh>

#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"

// Public bitmap capability composition. Posting representation and algorithms
// are private implementation details selected by the bitmap artifact or loader.

namespace milvus::index {

template <typename T>
class BitmapIndexReader
    : public IIndexReaderBase,
      public IScalarPredicateReader<T>,
      public IScalarValueReader<T>,
      public INullReader {
 public:
    ~BitmapIndexReader() override = default;
};

namespace bitmap_params {

inline bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR;
}

}  // namespace bitmap_params

// Posting representation is a construction detail and does not appear in the
// public reader interfaces.
enum class BitmapLayout {
    Roaring,
    Bitset,
};

template <typename T, typename Posting>
using BitmapPostingMap = std::map<T, Posting, std::less<>>;

template <typename T>
using BitmapRoaringPostingMap = BitmapPostingMap<T, roaring::Roaring>;

template <typename T>
using BitmapBitsetPostingMap = BitmapPostingMap<T, TargetBitmap>;

template <typename StoredT>
using bitmap_query_t = std::conditional_t<std::is_same_v<StoredT, std::string>,
                                          std::string_view,
                                          StoredT>;

// Owns the local frozen-Roaring file and its mapping. Reader implementations
// declare this owner before their posting map so frozen views are destroyed
// before the mapping.
class BitmapMmapOwner final {
 public:
    BitmapMmapOwner(char* data, size_t size, std::string path);
    ~BitmapMmapOwner();

    BitmapMmapOwner(const BitmapMmapOwner&) = delete;
    BitmapMmapOwner&
    operator=(const BitmapMmapOwner&) = delete;

    const char*
    Data() const;

    size_t
    Size() const;

 private:
    char* data_{nullptr};
    size_t size_{0};
    std::string path_;
};

// The owner is declared before the map so destruction drops frozen Roaring
// views before unmapping their backing file. Each specialization contains one
// posting representation only.
template <typename T, typename Posting>
struct BitmapPostingStorage {
    std::shared_ptr<BitmapMmapOwner> mmap_owner;
    BitmapPostingMap<T, Posting> postings;
};

struct BitmapReaderOptions {
    TargetBitmap valid_bitset;
    size_t total_num_rows{0};
    bool nested{false};
    bool value_lookup{true};
    DataType value_type{DataType::NONE};
    bool offset_cache{false};
};

template <typename T>
std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>
CreateBitmapIndexReader(
    BitmapRoaringPostingMap<T> postings,
    BitmapReaderOptions options,
    std::shared_ptr<BitmapMmapOwner> mmap_owner = nullptr);

template <typename T>
std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>
CreateBitmapIndexReader(
    std::shared_ptr<const BitmapPostingStorage<T, roaring::Roaring>> storage,
    BitmapReaderOptions options);

template <typename T>
std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>
CreateBitmapIndexReader(BitmapBitsetPostingMap<T> postings,
                        BitmapReaderOptions options);

}  // namespace milvus::index
