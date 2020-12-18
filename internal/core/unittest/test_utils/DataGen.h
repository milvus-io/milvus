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
#include "common/Schema.h"
#include <random>
#include <memory>
#include <cstring>
#include "segcore/SegmentBase.h"
namespace milvus::segcore {

struct GeneratedData {
    std::vector<char> rows_;
    std::vector<std::vector<char>> cols_;
    std::vector<idx_t> row_ids_;
    std::vector<Timestamp> timestamps_;
    RowBasedRawData raw_;
    template <typename T>
    auto
    get_col(int index) {
        auto& target = cols_.at(index);
        std::vector<T> ret(target.size() / sizeof(T));
        memcpy(ret.data(), target.data(), target.size());
        return ret;
    }
    template <typename T>
    auto
    get_mutable_col(int index) {
        auto& target = cols_.at(index);
        assert(target.size() == row_ids_.size() * sizeof(T));
        auto ptr = reinterpret_cast<T*>(target.data());
        return ptr;
    }

 private:
    GeneratedData() = default;
    friend GeneratedData
    DataGen(SchemaPtr schema, int64_t N, uint64_t seed);
    void
    generate_rows(int N, SchemaPtr schema);
};
inline void
GeneratedData::generate_rows(int N, SchemaPtr schema) {
    std::vector<int> offset_infos(schema->size() + 1, 0);
    auto sizeof_infos = schema->get_sizeof_infos();
    std::partial_sum(sizeof_infos.begin(), sizeof_infos.end(), offset_infos.begin() + 1);
    auto len_per_row = offset_infos.back();
    assert(len_per_row == schema->get_total_sizeof());

    std::vector<char> result(len_per_row * N);
    for (int index = 0; index < N; ++index) {
        for (int fid = 0; fid < schema->size(); ++fid) {
            auto len = sizeof_infos[fid];
            auto offset = offset_infos[fid];
            auto src = cols_[fid].data() + index * len;
            auto dst = result.data() + offset + index * len_per_row;
            memcpy(dst, src, len);
        }
    }
    rows_ = std::move(result);
    raw_.raw_data = rows_.data();
    raw_.sizeof_per_row = schema->get_total_sizeof();
    raw_.count = N;
}

inline GeneratedData
DataGen(SchemaPtr schema, int64_t N, uint64_t seed = 42) {
    using std::vector;
    std::vector<vector<char>> cols;
    std::default_random_engine er(seed);
    std::normal_distribution<> distr(0, 1);
    int offset = 0;

    auto insert_cols = [&cols](auto& data) {
        using T = std::remove_reference_t<decltype(data)>;
        auto len = sizeof(typename T::value_type) * data.size();
        auto ptr = vector<char>(len);
        memcpy(ptr.data(), data.data(), len);
        cols.emplace_back(std::move(ptr));
    };

    for (auto& field : schema->get_fields()) {
        switch (field.get_data_type()) {
            case engine::DataType::VECTOR_FLOAT: {
                auto dim = field.get_dim();
                vector<float> data(dim * N);
                for (auto& x : data) {
                    x = distr(er) + offset;
                }
                insert_cols(data);
                break;
            }
            case engine::DataType::VECTOR_BINARY: {
                auto dim = field.get_dim();
                Assert(dim % 8 == 0);
                vector<uint8_t> data(dim / 8 * N);
                for (auto& x : data) {
                    x = er();
                }
                insert_cols(data);
                break;
            }
            case engine::DataType::INT64: {
                vector<int64_t> data(N);
                for (auto& x : data) {
                    x = er();
                }
                insert_cols(data);
                break;
            }
            case engine::DataType::INT32: {
                vector<int> data(N);
                for (auto& x : data) {
                    x = er() % (2 * N);
                }
                insert_cols(data);
                break;
            }
            case engine::DataType::FLOAT: {
                vector<float> data(N);
                for (auto& x : data) {
                    x = distr(er);
                }
                insert_cols(data);
                break;
            }
            default: {
                throw std::runtime_error("unimplemented");
            }
        }
        ++offset;
    }
    GeneratedData res;
    res.cols_ = std::move(cols);
    for (int i = 0; i < N; ++i) {
        res.row_ids_.push_back(i);
        res.timestamps_.push_back(i);
    }

    res.generate_rows(N, schema);
    return std::move(res);
}

inline auto
CreatePlaceholderGroup(int64_t num_queries, int dim, int64_t seed = 42) {
    namespace ser = milvus::proto::service;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::VECTOR_FLOAT);
    std::normal_distribution<double> dis(0, 1);
    std::default_random_engine e(seed);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<float> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(dis(e));
        }
        // std::string line((char*)vec.data(), (char*)vec.data() + vec.size() * sizeof(float));
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    return raw_group;
}

inline auto
CreateBinaryPlaceholderGroup(int64_t num_queries, int64_t dim, int64_t seed = 42) {
    assert(dim % 8 == 0);
    namespace ser = milvus::proto::service;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::VECTOR_BINARY);
    std::default_random_engine e(seed);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<uint8_t> vec;
        for (int d = 0; d < dim / 8; ++d) {
            vec.push_back(e());
        }
        // std::string line((char*)vec.data(), (char*)vec.data() + vec.size() * sizeof(float));
        value->add_values(vec.data(), vec.size());
    }
    return raw_group;
}

inline auto
CreateBinaryPlaceholderGroupFromBlob(int64_t num_queries, int64_t dim, const uint8_t* ptr) {
    assert(dim % 8 == 0);
    namespace ser = milvus::proto::service;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::VECTOR_BINARY);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<uint8_t> vec;
        for (int d = 0; d < dim / 8; ++d) {
            vec.push_back(*ptr);
            ++ptr;
        }
        // std::string line((char*)vec.data(), (char*)vec.data() + vec.size() * sizeof(float));
        value->add_values(vec.data(), vec.size());
    }
    return raw_group;
}

inline json
QueryResultToJson(const QueryResult& qr) {
    int64_t num_queries = qr.num_queries_;
    int64_t topk = qr.topK_;
    std::vector<std::vector<std::string>> results;
    for (int q = 0; q < num_queries; ++q) {
        std::vector<std::string> result;
        for (int k = 0; k < topk; ++k) {
            int index = q * topk + k;
            result.emplace_back(std::to_string(qr.internal_seg_offsets_[index]) + "->" +
                                std::to_string(qr.result_distances_[index]));
        }
        results.emplace_back(std::move(result));
    }
    return json{results};
};

}  // namespace milvus::segcore
