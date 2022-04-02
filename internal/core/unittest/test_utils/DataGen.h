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

#include <boost/algorithm/string/predicate.hpp>
#include <cstring>
#include <memory>
#include <random>
#include <knowhere/index/vector_index/VecIndex.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <knowhere/index/vector_index/IndexIVF.h>
#include <index/ScalarIndexSort.h>

#include "Constants.h"
#include "common/Schema.h"
#include "query/SearchOnIndex.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"

using boost::algorithm::starts_with;

namespace milvus::segcore {

struct GeneratedData {
    std::vector<uint8_t> rows_;
    std::vector<aligned_vector<uint8_t>> cols_;
    std::vector<idx_t> row_ids_;
    std::vector<Timestamp> timestamps_;
    RowBasedRawData raw_;
    template <typename T>
    auto
    get_col(int index) const {
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
    DataGen(SchemaPtr schema, int64_t N, uint64_t seed, uint64_t ts_offset);
    void
    generate_rows(int64_t N, SchemaPtr schema);
};

inline void
GeneratedData::generate_rows(int64_t N, SchemaPtr schema) {
    std::vector<int> offset_infos(schema->size() + 1, 0);
    auto sizeof_infos = schema->get_sizeof_infos();
    std::partial_sum(sizeof_infos.begin(), sizeof_infos.end(), offset_infos.begin() + 1);
    int64_t len_per_row = offset_infos.back();
    assert(len_per_row == schema->get_total_sizeof());

    // change column-based data to row-based data
    std::vector<uint8_t> result(len_per_row * N);
    for (int index = 0; index < N; ++index) {
        for (int fid = 0; fid < schema->size(); ++fid) {
            auto len = sizeof_infos[fid];
            auto offset = offset_infos[fid];
            auto src = cols_[fid].data() + index * len;
            auto dst = result.data() + index * len_per_row + offset;
            memcpy(dst, src, len);
        }
    }
    rows_ = std::move(result);
    raw_.raw_data = rows_.data();
    raw_.sizeof_per_row = schema->get_total_sizeof();
    raw_.count = N;
}

inline GeneratedData
DataGen(SchemaPtr schema, int64_t N, uint64_t seed = 42, uint64_t ts_offset = 0) {
    using std::vector;
    std::vector<aligned_vector<uint8_t>> cols;
    std::default_random_engine er(seed);
    std::normal_distribution<> distr(0, 1);
    int offset = 0;

    auto insert_cols = [&cols](auto& data) {
        using T = std::remove_reference_t<decltype(data)>;
        auto len = sizeof(typename T::value_type) * data.size();
        auto ptr = aligned_vector<uint8_t>(len);
        memcpy(ptr.data(), data.data(), len);
        cols.emplace_back(std::move(ptr));
    };

    for (auto& field : schema->get_fields()) {
        switch (field.get_data_type()) {
            case engine::DataType::VECTOR_FLOAT: {
                auto dim = field.get_dim();
                vector<float> final(dim * N);
                bool is_ip = starts_with(field.get_name().get(), "normalized");
#pragma omp parallel for
                for (int n = 0; n < N; ++n) {
                    vector<float> data(dim);
                    float sum = 0;

                    std::default_random_engine er2(seed + n);
                    std::normal_distribution<> distr2(0, 1);
                    for (auto& x : data) {
                        x = distr2(er2) + offset;
                        sum += x * x;
                    }
                    if (is_ip) {
                        sum = sqrt(sum);
                        for (auto& x : data) {
                            x /= sum;
                        }
                    }

                    std::copy(data.begin(), data.end(), final.begin() + dim * n);
                }
                insert_cols(final);
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
                // begin with counter
                if (starts_with(field.get_name().get(), "counter")) {
                    int64_t index = 0;
                    for (auto& x : data) {
                        x = index++;
                    }
                } else {
                    int i = 0;
                    for (auto& x : data) {
                        x = er() % (2 * N);
                        x = i;
                        i++;
                    }
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
            case engine::DataType::DOUBLE: {
                vector<double> data(N);
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
        res.timestamps_.push_back(i + ts_offset);
    }
    //    std::shuffle(res.row_ids_.begin(), res.row_ids_.end(), er);
    res.generate_rows(N, schema);
    return std::move(res);
}

inline auto
CreatePlaceholderGroup(int64_t num_queries, int dim, int64_t seed = 42) {
    namespace ser = milvus::proto::milvus;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::FloatVector);
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
CreatePlaceholderGroupFromBlob(int64_t num_queries, int dim, const float* src) {
    namespace ser = milvus::proto::milvus;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::FloatVector);
    int64_t src_index = 0;

    for (int i = 0; i < num_queries; ++i) {
        std::vector<float> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(src[src_index++]);
        }
        // std::string line((char*)vec.data(), (char*)vec.data() + vec.size() * sizeof(float));
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    return raw_group;
}

inline auto
CreateBinaryPlaceholderGroup(int64_t num_queries, int64_t dim, int64_t seed = 42) {
    assert(dim % 8 == 0);
    namespace ser = milvus::proto::milvus;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::BinaryVector);
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
    namespace ser = milvus::proto::milvus;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::BinaryVector);
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
SearchResultToJson(const SearchResult& sr) {
    int64_t num_queries = sr.num_queries_;
    int64_t topk = sr.topk_;
    std::vector<std::vector<std::string>> results;
    for (int q = 0; q < num_queries; ++q) {
        std::vector<std::string> result;
        for (int k = 0; k < topk; ++k) {
            int index = q * topk + k;
            result.emplace_back(std::to_string(sr.ids_[index]) + "->" + std::to_string(sr.distances_[index]));
        }
        results.emplace_back(std::move(result));
    }
    return json{results};
};

inline void
SealedLoader(const GeneratedData& dataset, SegmentSealed& seg) {
    // TODO
    auto row_count = dataset.row_ids_.size();
    {
        LoadFieldDataInfo info;
        info.blob = dataset.row_ids_.data();
        info.row_count = dataset.row_ids_.size();
        info.field_id = 0;  // field id for RowId
        seg.LoadFieldData(info);
    }
    {
        LoadFieldDataInfo info;
        info.blob = dataset.timestamps_.data();
        info.row_count = dataset.timestamps_.size();
        info.field_id = 1;
        seg.LoadFieldData(info);
    }
    int field_offset = 0;
    for (auto& meta : seg.get_schema().get_fields()) {
        LoadFieldDataInfo info;
        info.field_id = meta.get_id().get();
        info.row_count = row_count;
        info.blob = dataset.cols_[field_offset].data();
        seg.LoadFieldData(info);
        ++field_offset;
    }
}

inline std::unique_ptr<SegmentSealed>
SealedCreator(SchemaPtr schema, const GeneratedData& dataset, const LoadIndexInfo& index_info) {
    auto segment = CreateSealedSegment(schema);
    SealedLoader(dataset, *segment);
    segment->LoadIndex(index_info);
    return segment;
}

inline knowhere::VecIndexPtr
GenIndexing(int64_t N, int64_t dim, const float* vec) {
    // {knowhere::IndexParams::nprobe, 10},
    auto conf = knowhere::Config{{knowhere::meta::DIM, dim},
                                 {knowhere::IndexParams::nlist, 1024},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};
    auto database = knowhere::GenDataset(N, dim, vec);
    auto indexing = std::make_shared<knowhere::IVF>();
    indexing->Train(database, conf);
    indexing->AddWithoutIds(database, conf);
    return indexing;
}

template<typename T>
inline scalar::IndexBasePtr
GenScalarIndexing(int64_t N, const T* data) {
    auto indexing = scalar::CreateScalarIndexSort<T>();
    indexing->Build(N, data);
    return indexing;
}

}  // namespace milvus::segcore
