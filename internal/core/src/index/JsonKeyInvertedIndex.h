// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <string>
#include <boost/filesystem.hpp>

#include "index/InvertedIndexTantivy.h"
#include "common/jsmn.h"

namespace milvus::index {

using stdclock = std::chrono::high_resolution_clock;
class JsonKeyInvertedIndex : public InvertedIndexTantivy<std::string> {
 public:
    explicit JsonKeyInvertedIndex(const storage::FileManagerContext& ctx,
                                  bool is_load);

    ~JsonKeyInvertedIndex() override{};

 public:
    IndexStatsPtr
    Upload(const Config& config) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

    const TargetBitmap
    FilterByPath(const std::string& path,
                 int32_t row,
                 std::function<bool(uint32_t, uint16_t, uint16_t)> filter) {
        TargetBitmap bitset(row);
        auto array = wrapper_->term_query<std::string>(path);
        LOG_DEBUG("json key filter size:{}", array.array_.len);

        for (size_t j = 0; j < array.array_.len; j++) {
            auto the_offset = array.array_.array[j];
            auto tuple = DecodeOffset(the_offset);
            auto row_id = std::get<0>(tuple);
            bitset[row_id] = filter(
                std::get<0>(tuple), std::get<1>(tuple), std::get<2>(tuple));
        }

        return std::move(bitset);
    }

 private:
    void
    AddJson(const char* json, int64_t offset);

    void
    TravelJson(const char* json,
               jsmntok* tokens,
               int& index,
               std::vector<std::string>& path,
               int32_t offset);

    void
    AddInvertedRecord(const std::vector<std::string>& paths,
                      uint32_t row_id,
                      uint16_t offset,
                      uint16_t length);

    int64_t
    EncodeOffset(uint32_t row_id, uint16_t row_offset, uint16_t size) {
        return static_cast<int64_t>(row_id) << 32 |
               static_cast<int64_t>(row_offset) << 16 |
               static_cast<int64_t>(size);
    }

    std::tuple<uint32_t, uint16_t, uint16_t>
    DecodeOffset(int64_t encode_offset) {
        uint32_t row_id = (encode_offset >> 32) & 0xFFFFFFFF;
        uint16_t row_offset = (encode_offset >> 16) & 0xFFFF;
        uint16_t size = encode_offset & 0xFFFF;
        return std::make_tuple(row_id, row_offset, size);
    }

 private:
    int64_t field_id_;
};
}  // namespace milvus::index
