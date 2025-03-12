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
enum class JSONType {
    UNKNOWN,
    BOOL,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    STRING,
    STRING_ESCAPE
};
using stdclock = std::chrono::high_resolution_clock;
class JsonKeyInvertedIndex : public InvertedIndexTantivy<std::string> {
 public:
    explicit JsonKeyInvertedIndex(
        const storage::FileManagerContext& ctx,
        bool is_load,
        int64_t json_stats_build_type = 0,
        int64_t json_stats_tantivy_memory_budget = 16777216);

    explicit JsonKeyInvertedIndex(int64_t commit_interval_in_ms,
                                  const char* unique_id);

    explicit JsonKeyInvertedIndex(int64_t commit_interval_in_ms,
                                  const char* unique_id,
                                  const std::string& path);

    ~JsonKeyInvertedIndex() override{};

 public:
    IndexStatsPtr
    Upload(const Config& config) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas, bool nullable);

    const TargetBitmap
    FilterByPath(
        const std::string& path,
        int32_t row,
        bool is_growing,
        bool is_strong_consistency,
        std::function<bool(
            bool, uint8_t, uint32_t, uint16_t, uint16_t, uint32_t)> filter) {
        auto processArray = [this, &path, row, &filter]() {
            TargetBitmap bitset(row);
            auto array = wrapper_->term_query_i64(path);
            LOG_INFO("json key filter size:{}", array.array_.len);
            for (size_t j = 0; j < array.array_.len; j++) {
                auto the_offset = array.array_.array[j];
                if (build_type_ == 0) {
                    if (DecodeValid(the_offset)) {
                        auto tuple = DecodeValue(the_offset);
                        auto row_id = std::get<1>(tuple);
                        if (row_id >= row) {
                            continue;
                        }
                        bitset[row_id] = filter(true,
                                                std::get<0>(tuple),
                                                std::get<1>(tuple),
                                                0,
                                                0,
                                                std::get<2>(tuple));
                    } else {
                        auto tuple = DecodeOffset(the_offset);
                        auto row_id = std::get<1>(tuple);
                        if (row_id >= row) {
                            continue;
                        }
                        bitset[row_id] = filter(false,
                                                std::get<0>(tuple),
                                                std::get<1>(tuple),
                                                std::get<2>(tuple),
                                                std::get<3>(tuple),
                                                0);
                    }
                }
            }

            return bitset;
        };

        if (is_growing) {
            if (shouldTriggerCommit() || is_strong_consistency) {
                if (is_data_uncommitted_) {
                    Commit();
                }
                Reload();
                return processArray();
            } else {
                return processArray();
            }
        } else {
            return processArray();
        }
    }

    void
    AddJSONDatas(size_t n,
                 const std::string* jsonDatas,
                 const bool* valids,
                 int64_t offset_begin);

    void
    Finish();

    void
    Commit();

    void
    Reload();

    void
    CreateReader();

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
    AddJSONEncodeValue(const std::vector<std::string>& paths,
                       uint8_t flag,
                       uint8_t type,
                       uint32_t row_id,
                       uint16_t offset,
                       uint16_t length,
                       uint32_t value);

    int64_t
    EncodeOffset(uint8_t flag,
                 uint8_t type,
                 uint32_t row_id,
                 uint16_t row_offset,
                 uint16_t size) {
        row_id &= 0x0FFFFFFF;
        return static_cast<int64_t>(flag) << 63 |
               static_cast<int64_t>(type) << 60 |
               static_cast<int64_t>(row_id) << 32 |
               static_cast<int64_t>(row_offset) << 16 |
               static_cast<int64_t>(size);
    }

    int64_t
    EncodeValue(uint8_t flag, uint8_t type, uint32_t row_id, uint32_t value) {
        return static_cast<int64_t>(flag) << 63 |
               static_cast<int64_t>(type) << 60 |
               static_cast<int64_t>(row_id) << 32 | static_cast<int64_t>(value);
    }

    bool
    DecodeValid(int64_t encode_offset) {
        return (encode_offset >> 63) & 1;
    }

    std::tuple<uint8_t, uint32_t, uint32_t>
    DecodeValue(int64_t encode_offset) {
        uint8_t type = (encode_offset >> 60) & 0x7;
        uint32_t row_id = (encode_offset >> 32) & 0x0FFFFFFF;
        uint32_t value = encode_offset & 0xFFFFFFFF;
        return std::make_tuple(type, row_id, value);
    }

    std::tuple<uint8_t, uint32_t, uint16_t, uint16_t>
    DecodeOffset(int64_t encode_offset) {
        uint8_t type = (encode_offset >> 60) & 0x7;
        uint32_t row_id = (encode_offset >> 32) & 0x0FFFFFFF;
        uint16_t row_offset = (encode_offset >> 16) & 0xFFFF;
        uint16_t size = encode_offset & 0xFFFF;
        return std::make_tuple(type, row_id, row_offset, size);
    }

    bool
    shouldTriggerCommit();

    bool
    isBoolean(const std::string& str) {
        return str == "true" || str == "false";
    }

    bool
    isInt32(const std::string& str) {
        std::istringstream iss(str);
        int64_t num;
        iss >> num;

        return !iss.fail() && iss.eof() &&
               num >= std::numeric_limits<int32_t>::min() &&
               num <= std::numeric_limits<int32_t>::max();
    }

    bool
    isInt64(const std::string& str) {
        std::istringstream iss(str);
        int64_t num;
        iss >> num;

        return !iss.fail() && iss.eof();
    }

    bool
    isFloat(const std::string& str) {
        try {
            float d = std::stof(str);
            return true;
        } catch (...) {
            return false;
        }
    }

    bool
    isDouble(const std::string& str) {
        try {
            double d = std::stod(str);
            return true;
        } catch (...) {
            return false;
        }
    }

    bool
    has_escape_sequence(const std::string& str) {
        for (size_t i = 0; i < str.size(); ++i) {
            if (str[i] == '\\' && i + 1 < str.size()) {
                char next = str[i + 1];
                if (next == 'n' || next == 't' || next == 'r' || next == 'b' ||
                    next == 'f' || next == 'v' || next == '\\' ||
                    next == '\"' || next == '\'') {
                    return true;
                }
            }
        }
        return false;
    }

    JSONType
    getType(const std::string& str) {
        if (isBoolean(str)) {
            return JSONType::BOOL;
        } else if (isInt32(str)) {
            return JSONType::INT32;
        } else if (isInt64(str)) {
            return JSONType::INT64;
        } else if (isFloat(str)) {
            return JSONType::FLOAT;
        } else if (isDouble(str)) {
            return JSONType::DOUBLE;
        }
        return JSONType::UNKNOWN;
    }

    void
    AddInvertedRecord();

 private:
    int64_t field_id_;
    mutable std::mutex mtx_;
    std::atomic<stdclock::time_point> last_commit_time_;
    int64_t commit_interval_in_ms_;
    int64_t build_type_ = 0;
    std::map<std::string, std::vector<int64_t>> mp;
    std::atomic<bool> is_data_uncommitted_ = false;
};
}  // namespace milvus::index
