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

#include "index/scalar/bitmap/BitmapIndexArtifact.h"

#include <cstring>
#include <limits>
#include <sstream>
#include <type_traits>
#include <utility>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "index/Meta.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kLegacyNestedKey = "is_nested_index";
constexpr std::string_view kV3NestedKey = "is_nested";

void
CheckedAdd(size_t& total, size_t value, std::string_view label) {
    if (value > std::numeric_limits<size_t>::max() - total) {
        ThrowInfo(
            DataTypeInvalid, "bitmap serialized {} byte size overflows", label);
    }
    total += value;
}

size_t
PackedValidityBytes(size_t count) {
    return count / 8 + static_cast<size_t>(count % 8 != 0);
}

template <typename T>
size_t
SerializedDataSize(const BitmapArtifactPostingMap<T>& postings) {
    size_t size = 0;
    for (const auto& [key, posting] : postings) {
        if constexpr (std::is_same_v<T, std::string>) {
            CheckedAdd(size, sizeof(size_t), "string key");
            CheckedAdd(size, key.size(), "string key");
        } else {
            CheckedAdd(size, sizeof(T), "numeric key");
        }
        CheckedAdd(size, posting.getSizeInBytes(true), "posting");
    }
    return size;
}

template <typename T>
std::vector<uint8_t>
SerializeData(const BitmapArtifactPostingMap<T>& postings) {
    std::vector<uint8_t> output(SerializedDataSize(postings));
    if (output.empty()) {
        return output;
    }
    auto* cursor = output.data();
    for (const auto& [key, posting] : postings) {
        if constexpr (std::is_same_v<T, std::string>) {
            const auto key_size = key.size();
            std::memcpy(cursor, &key_size, sizeof(key_size));
            cursor += sizeof(key_size);
            std::memcpy(cursor, key.data(), key_size);
            cursor += key_size;
        } else {
            std::memcpy(cursor, &key, sizeof(T));
            cursor += sizeof(T);
        }
        cursor += posting.write(reinterpret_cast<char*>(cursor), true);
    }
    AssertInfo(cursor == output.data() + output.size(),
               "bitmap serialization size mismatch");
    return output;
}

std::vector<uint8_t>
SerializeValidity(const TargetBitmap& validity, size_t count) {
    std::vector<uint8_t> output(PackedValidityBytes(count), 0);
    for (size_t i = 0; i < count; ++i) {
        if (validity[i]) {
            output[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
        }
    }
    return output;
}

}  // namespace

template <typename T>
BitmapIndexArtifact<T>::BitmapIndexArtifact(
    BitmapArtifactPostingMap<T> postings,
    TargetBitmap valid_bitset,
    size_t total_num_rows,
    bool nested,
    bool nullable)
    : state_(BuilderState{
          .postings = std::move(postings),
          .valid_bitset = std::move(valid_bitset),
          .total_num_rows = total_num_rows,
          .nested = nested,
          .nullable = nullable}) {
}

template <typename T>
BitmapIndexArtifact<T>::~BitmapIndexArtifact() = default;

template <typename T>
void
BitmapIndexArtifact<T>::Serialize(storage::FileSink& sink) const {
    const auto& postings = state_.postings;
    const auto data = SerializeData(postings);

    if (sink.Gen() == storage::Generation::V1V2) {
        YAML::Node meta;
        meta[BITMAP_INDEX_LENGTH] = postings.size();
        meta[BITMAP_INDEX_NUM_ROWS] = state_.total_num_rows;
        meta[std::string(kLegacyNestedKey)] = state_.nested;
        std::stringstream stream;
        stream << meta;
        const auto encoded = stream.str();
        sink.WriteEntry(BITMAP_INDEX_META, encoded.data(), encoded.size());
    } else {
        sink.PutMeta(BITMAP_INDEX_LENGTH,
                     nlohmann::json(postings.size()));
        sink.PutMeta(BITMAP_INDEX_NUM_ROWS,
                     nlohmann::json(state_.total_num_rows));
        sink.PutMeta(kV3NestedKey, nlohmann::json(state_.nested));
    }

    sink.WriteEntry(BITMAP_INDEX_DATA, data.data(), data.size());
    if (state_.nullable) {
        const auto validity =
            SerializeValidity(state_.valid_bitset, state_.total_num_rows);
        sink.WriteEntry(
            BITMAP_INDEX_VALID_BITSET, validity.data(), validity.size());
    }
}

#define INSTANTIATE_BITMAP_ARTIFACT(T) template class BitmapIndexArtifact<T>;
INSTANTIATE_BITMAP_ARTIFACT(bool)
INSTANTIATE_BITMAP_ARTIFACT(int8_t)
INSTANTIATE_BITMAP_ARTIFACT(int16_t)
INSTANTIATE_BITMAP_ARTIFACT(int32_t)
INSTANTIATE_BITMAP_ARTIFACT(int64_t)
INSTANTIATE_BITMAP_ARTIFACT(float)
INSTANTIATE_BITMAP_ARTIFACT(double)
INSTANTIATE_BITMAP_ARTIFACT(std::string)
#undef INSTANTIATE_BITMAP_ARTIFACT

}  // namespace milvus::index
