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
#include <string>
#include <google/protobuf/text_format.h>

#include "Constants.h"
#include "common/Schema.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
#include "index/VectorMemNMIndex.h"
#include "query/SearchOnIndex.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/Utils.h"
#include "knowhere/comp/index_param.h"

#include "PbHelper.h"

using boost::algorithm::starts_with;

namespace milvus::segcore {

struct GeneratedData {
    std::vector<idx_t> row_ids_;
    std::vector<Timestamp> timestamps_;
    InsertData* raw_;
    std::vector<FieldId> field_ids;
    SchemaPtr schema_;

    void
    DeepCopy(const GeneratedData& data) {
        row_ids_ = data.row_ids_;
        timestamps_ = data.timestamps_;
        raw_ = clone_msg(data.raw_).release();
        field_ids = data.field_ids;
        schema_ = data.schema_;
    }

    GeneratedData(const GeneratedData& data) {
        DeepCopy(data);
    }

    GeneratedData&
    operator=(const GeneratedData& data) {
        if (this != &data) {
            delete raw_;
            DeepCopy(data);
        }
        return *this;
    }

    GeneratedData(GeneratedData&& data)
        : row_ids_(std::move(data.row_ids_)),
          timestamps_(std::move(data.timestamps_)),
          raw_(data.raw_),
          field_ids(std::move(data.field_ids)),
          schema_(std::move(data.schema_)) {
        data.raw_ = nullptr;
    }

    ~GeneratedData() {
        delete raw_;
    }

    template <typename T>
    std::vector<T>
    get_col(FieldId field_id) const {
        std::vector<T> ret(raw_->num_rows());
        for (auto i = 0; i < raw_->fields_data_size(); i++) {
            auto target_field_data = raw_->fields_data(i);
            if (field_id.get() != target_field_data.field_id()) {
                continue;
            }

            auto& field_meta = schema_->operator[](field_id);
            if (field_meta.is_vector()) {
                if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                    int len = raw_->num_rows() * field_meta.get_dim();
                    ret.resize(len);
                    auto src_data =
                        reinterpret_cast<const T*>(target_field_data.vectors()
                                                       .float_vector()
                                                       .data()
                                                       .data());
                    std::copy_n(src_data, len, ret.data());
                } else if (field_meta.get_data_type() ==
                           DataType::VECTOR_BINARY) {
                    int len = raw_->num_rows() * (field_meta.get_dim() / 8);
                    ret.resize(len);
                    auto src_data = reinterpret_cast<const T*>(
                        target_field_data.vectors().binary_vector().data());
                    std::copy_n(src_data, len, ret.data());
                } else {
                    PanicInfo("unsupported");
                }

                return std::move(ret);
            }
            switch (field_meta.get_data_type()) {
                case DataType::BOOL: {
                    auto src_data = reinterpret_cast<const T*>(
                        target_field_data.scalars().bool_data().data().data());
                    std::copy_n(src_data, raw_->num_rows(), ret.data());
                    break;
                }
                case DataType::INT8:
                case DataType::INT16:
                case DataType::INT32: {
                    auto src_data = reinterpret_cast<const int32_t*>(
                        target_field_data.scalars().int_data().data().data());
                    std::copy_n(src_data, raw_->num_rows(), ret.data());
                    break;
                }
                case DataType::INT64: {
                    auto src_data = reinterpret_cast<const T*>(
                        target_field_data.scalars().long_data().data().data());
                    std::copy_n(src_data, raw_->num_rows(), ret.data());
                    break;
                }
                case DataType::FLOAT: {
                    auto src_data = reinterpret_cast<const T*>(
                        target_field_data.scalars().float_data().data().data());
                    std::copy_n(src_data, raw_->num_rows(), ret.data());
                    break;
                }
                case DataType::DOUBLE: {
                    auto src_data =
                        reinterpret_cast<const T*>(target_field_data.scalars()
                                                       .double_data()
                                                       .data()
                                                       .data());
                    std::copy_n(src_data, raw_->num_rows(), ret.data());
                    break;
                }
                case DataType::VARCHAR: {
                    auto ret_data = reinterpret_cast<std::string*>(ret.data());
                    auto src_data =
                        target_field_data.scalars().string_data().data();
                    std::copy(src_data.begin(), src_data.end(), ret_data);

                    break;
                }
                case DataType::JSON: {
                    auto ret_data = reinterpret_cast<std::string*>(ret.data());
                    auto src_data =
                        target_field_data.scalars().json_data().data();
                    std::copy(src_data.begin(), src_data.end(), ret_data);
                    break;
                }
                default: {
                    PanicInfo("unsupported");
                }
            }
        }
        return std::move(ret);
    }

    std::unique_ptr<DataArray>
    get_col(FieldId field_id) const {
        for (auto target_field_data : raw_->fields_data()) {
            if (field_id.get() == target_field_data.field_id()) {
                return std::make_unique<DataArray>(target_field_data);
            }
        }

        PanicInfo("field id not find");
    }

 private:
    GeneratedData() = default;
    friend GeneratedData
    DataGen(SchemaPtr schema,
            int64_t N,
            uint64_t seed,
            uint64_t ts_offset,
            int repeat_count);
    friend GeneratedData
    DataGenForJsonArray(SchemaPtr schema,
                        int64_t N,
                        uint64_t seed,
                        uint64_t ts_offset,
                        int repeat_count,
                        int array_len);
};

inline GeneratedData
DataGen(SchemaPtr schema,
        int64_t N,
        uint64_t seed = 42,
        uint64_t ts_offset = 0,
        int repeat_count = 1) {
    using std::vector;
    std::default_random_engine er(seed);
    std::normal_distribution<> distr(0, 1);
    int offset = 0;

    auto insert_data = std::make_unique<InsertData>();
    auto insert_cols = [&insert_data](
                           auto& data, int64_t count, auto& field_meta) {
        auto array = milvus::segcore::CreateDataArrayFrom(
            data.data(), count, field_meta);
        insert_data->mutable_fields_data()->AddAllocated(array.release());
    };

    for (auto field_id : schema->get_field_ids()) {
        auto field_meta = schema->operator[](field_id);
        switch (field_meta.get_data_type()) {
            case DataType::VECTOR_FLOAT: {
                auto dim = field_meta.get_dim();
                vector<float> final(dim * N);
                bool is_ip =
                    starts_with(field_meta.get_name().get(), "normalized");
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

                    std::copy(
                        data.begin(), data.end(), final.begin() + dim * n);
                }
                insert_cols(final, N, field_meta);
                break;
            }
            case DataType::VECTOR_BINARY: {
                auto dim = field_meta.get_dim();
                Assert(dim % 8 == 0);
                vector<uint8_t> data(dim / 8 * N);
                for (auto& x : data) {
                    x = er();
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::BOOL: {
                FixedVector<bool> data(N);
                for (int i = 0; i < N; ++i) {
                    data[i] = i % 2 == 0 ? true : false;
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::INT64: {
                vector<int64_t> data(N);
                for (int i = 0; i < N; i++) {
                    data[i] = i / repeat_count;
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::INT32: {
                vector<int> data(N);
                for (auto& x : data) {
                    x = er() % (2 * N);
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::INT16: {
                vector<int16_t> data(N);
                for (auto& x : data) {
                    x = er() % (2 * N);
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::INT8: {
                vector<int8_t> data(N);
                for (auto& x : data) {
                    x = er() % (2 * N);
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::FLOAT: {
                vector<float> data(N);
                for (auto& x : data) {
                    x = distr(er);
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::DOUBLE: {
                vector<double> data(N);
                for (auto& x : data) {
                    x = distr(er);
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::VARCHAR: {
                vector<std::string> data(N);
                for (int i = 0; i < N / repeat_count; i++) {
                    auto str = std::to_string(er());
                    for (int j = 0; j < repeat_count; j++) {
                        data[i * repeat_count + j] = str;
                    }
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::JSON: {
                vector<std::string> data(N);
                for (int i = 0; i < N / repeat_count; i++) {
                    auto str = R"({"int":)" + std::to_string(er()) +
                               R"(,"double":)" +
                               std::to_string(static_cast<double>(er())) +
                               R"(,"string":")" + std::to_string(er()) +
                               R"(","bool": true)" + "}";
                    data[i] = str;
                }
                insert_cols(data, N, field_meta);
                break;
            }
            default: {
                throw std::runtime_error("unimplemented");
            }
        }
        ++offset;
    }

    GeneratedData res;
    res.schema_ = schema;
    res.raw_ = insert_data.release();
    res.raw_->set_num_rows(N);
    for (int i = 0; i < N; ++i) {
        res.row_ids_.push_back(i);
        res.timestamps_.push_back(i + ts_offset);
    }

    return res;
}

template <typename T>
std::string
join(const std::vector<T>& items, const std::string& delimiter) {
    std::stringstream ss;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i > 0) {
            ss << delimiter;
        }
        ss << items[i];
    }
    return ss.str();
}

inline GeneratedData
DataGenForJsonArray(SchemaPtr schema,
                    int64_t N,
                    uint64_t seed = 42,
                    uint64_t ts_offset = 0,
                    int repeat_count = 1,
                    int array_len = 1) {
    using std::vector;
    std::default_random_engine er(seed);
    std::normal_distribution<> distr(0, 1);

    auto insert_data = std::make_unique<InsertData>();
    auto insert_cols = [&insert_data](
                           auto& data, int64_t count, auto& field_meta) {
        auto array = milvus::segcore::CreateDataArrayFrom(
            data.data(), count, field_meta);
        insert_data->mutable_fields_data()->AddAllocated(array.release());
    };
    for (auto field_id : schema->get_field_ids()) {
        auto field_meta = schema->operator[](field_id);
        switch (field_meta.get_data_type()) {
            case DataType::INT64: {
                vector<int64_t> data(N);
                for (int i = 0; i < N; i++) {
                    data[i] = i / repeat_count;
                }
                insert_cols(data, N, field_meta);
                break;
            }
            case DataType::JSON: {
                vector<std::string> data(N);
                for (int i = 0; i < N / repeat_count; i++) {
                    std::vector<std::string> intVec;
                    std::vector<std::string> doubleVec;
                    std::vector<std::string> stringVec;
                    std::vector<std::string> boolVec;
                    for (int i = 0; i < array_len; ++i) {
                        intVec.push_back(std::to_string(er()));
                        doubleVec.push_back(
                            std::to_string(static_cast<double>(er())));
                        stringVec.push_back("\"" + std::to_string(er()) + "\"");
                        boolVec.push_back(i % 2 == 0 ? "true" : "false");
                    }
                    auto str = R"({"int":[)" + join(intVec, ",") +
                               R"(],"double":[)" + join(doubleVec, ",") +
                               R"(],"string":[)" + join(stringVec, ",") +
                               R"(],"bool": [)" + join(boolVec, ",") + "]}";
                    //std::cout << str << std::endl;
                    data[i] = str;
                }
                insert_cols(data, N, field_meta);
                break;
            }
            default: {
                throw std::runtime_error("unimplemented");
            }
        }
    }

    milvus::segcore::GeneratedData res;
    res.schema_ = schema;
    res.raw_ = insert_data.release();
    res.raw_->set_num_rows(N);
    for (int i = 0; i < N; ++i) {
        res.row_ids_.push_back(i);
        res.timestamps_.push_back(i + ts_offset);
    }

    return res;
}

inline auto
CreatePlaceholderGroup(int64_t num_queries, int dim, int64_t seed = 42) {
    namespace ser = milvus::proto::common;
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
CreatePlaceholderGroup(int64_t num_queries,
                       int dim,
                       const std::vector<float>& vecs) {
    namespace ser = milvus::proto::common;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::FloatVector);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<float> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(vecs[i * dim + d]);
        }
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    return raw_group;
}

inline auto
CreatePlaceholderGroupFromBlob(int64_t num_queries, int dim, const float* src) {
    namespace ser = milvus::proto::common;
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
CreateBinaryPlaceholderGroup(int64_t num_queries,
                             int64_t dim,
                             int64_t seed = 42) {
    assert(dim % 8 == 0);
    namespace ser = milvus::proto::common;
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
CreateBinaryPlaceholderGroupFromBlob(int64_t num_queries,
                                     int64_t dim,
                                     const uint8_t* ptr) {
    assert(dim % 8 == 0);
    namespace ser = milvus::proto::common;
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

inline auto
SearchResultToVector(const SearchResult& sr) {
    int64_t num_queries = sr.total_nq_;
    int64_t topk = sr.unity_topK_;
    std::vector<std::pair<int, float>> result;
    for (int q = 0; q < num_queries; ++q) {
        for (int k = 0; k < topk; ++k) {
            int index = q * topk + k;
            result.emplace_back(
                std::make_pair(sr.seg_offsets_[index], sr.distances_[index]));
        }
    }
    return result;
}

inline json
SearchResultToJson(const SearchResult& sr) {
    int64_t num_queries = sr.total_nq_;
    int64_t topk = sr.unity_topK_;
    std::vector<std::vector<std::string>> results;
    for (int q = 0; q < num_queries; ++q) {
        std::vector<std::string> result;
        for (int k = 0; k < topk; ++k) {
            int index = q * topk + k;
            result.emplace_back(std::to_string(sr.seg_offsets_[index]) + "->" +
                                std::to_string(sr.distances_[index]));
        }
        results.emplace_back(std::move(result));
    }
    return json{results};
};

inline storage::FieldDataPtr
CreateFieldDataFromDataArray(ssize_t raw_count,
                             const DataArray* data,
                             const FieldMeta& field_meta) {
    int64_t dim = 1;
    storage::FieldDataPtr field_data = nullptr;

    auto createFieldData = [&field_data, &raw_count](const void* raw_data,
                                                     DataType data_type,
                                                     int64_t dim) {
        field_data = storage::CreateFieldData(data_type, dim);
        field_data->FillFieldData(raw_data, raw_count);
    };

    if (field_meta.is_vector()) {
        switch (field_meta.get_data_type()) {
            case DataType::VECTOR_FLOAT: {
                auto raw_data = data->vectors().float_vector().data().data();
                dim = field_meta.get_dim();
                createFieldData(raw_data, DataType::VECTOR_FLOAT, dim);
                break;
            }
            case DataType::VECTOR_BINARY: {
                auto raw_data = data->vectors().binary_vector().data();
                dim = field_meta.get_dim();
                AssertInfo(dim % 8 == 0, "wrong dim value for binary vector");
                createFieldData(raw_data, DataType::VECTOR_BINARY, dim);
                break;
            }
            default: {
                PanicInfo("unsupported");
            }
        }
    } else {
        switch (field_meta.get_data_type()) {
            case DataType::BOOL: {
                auto raw_data = data->scalars().bool_data().data().data();
                createFieldData(raw_data, DataType::BOOL, dim);
                break;
            }
            case DataType::INT8: {
                auto src_data = data->scalars().int_data().data();
                std::vector<int8_t> data_raw(src_data.size());
                std::copy_n(src_data.data(), src_data.size(), data_raw.data());
                createFieldData(data_raw.data(), DataType::INT8, dim);
                break;
            }
            case DataType::INT16: {
                auto src_data = data->scalars().int_data().data();
                std::vector<int16_t> data_raw(src_data.size());
                std::copy_n(src_data.data(), src_data.size(), data_raw.data());
                createFieldData(data_raw.data(), DataType::INT16, dim);
                break;
            }
            case DataType::INT32: {
                auto raw_data = data->scalars().int_data().data().data();
                createFieldData(raw_data, DataType::INT32, dim);
                break;
            }
            case DataType::INT64: {
                auto raw_data = data->scalars().long_data().data().data();
                createFieldData(raw_data, DataType::INT64, dim);
                break;
            }
            case DataType::FLOAT: {
                auto raw_data = data->scalars().float_data().data().data();
                createFieldData(raw_data, DataType::FLOAT, dim);
                break;
            }
            case DataType::DOUBLE: {
                auto raw_data = data->scalars().double_data().data().data();
                createFieldData(raw_data, DataType::DOUBLE, dim);
                break;
            }
            case DataType::VARCHAR: {
                auto begin = data->scalars().string_data().data().begin();
                auto end = data->scalars().string_data().data().end();
                std::vector<std::string> data_raw(begin, end);
                createFieldData(data_raw.data(), DataType::VARCHAR, dim);
                break;
            }
            case DataType::JSON: {
                auto src_data = data->scalars().json_data().data();
                std::vector<Json> data_raw(src_data.size());
                for (int i = 0; i < src_data.size(); i++) {
                    auto str = src_data.Get(i);
                    data_raw[i] = Json(simdjson::padded_string(str));
                }
                createFieldData(data_raw.data(), DataType::JSON, dim);
                break;
            }
            default: {
                PanicInfo("unsupported");
            }
        }
    }

    return field_data;
}

inline void
SealedLoadFieldData(const GeneratedData& dataset,
                    SegmentSealed& seg,
                    const std::set<int64_t>& exclude_fields = {},
                    bool with_mmap = false) {
    auto row_count = dataset.row_ids_.size();
    {
        auto field_data = std::make_shared<milvus::storage::FieldData<int64_t>>(
            DataType::INT64);
        field_data->FillFieldData(dataset.row_ids_.data(), row_count);
        auto field_data_info = FieldDataInfo(
            RowFieldID.get(),
            row_count,
            std::vector<milvus::storage::FieldDataPtr>{field_data});
        seg.LoadFieldData(RowFieldID, field_data_info);
    }
    {
        auto field_data = std::make_shared<milvus::storage::FieldData<int64_t>>(
            DataType::INT64);
        field_data->FillFieldData(dataset.timestamps_.data(), row_count);
        auto field_data_info = FieldDataInfo(
            TimestampFieldID.get(),
            row_count,
            std::vector<milvus::storage::FieldDataPtr>{field_data});
        seg.LoadFieldData(TimestampFieldID, field_data_info);
    }
    for (auto& iter : dataset.schema_->get_fields()) {
        int64_t field_id = iter.first.get();
        if (exclude_fields.find(field_id) != exclude_fields.end()) {
            continue;
        }
    }
    auto fields = dataset.schema_->get_fields();
    for (auto& field_data : dataset.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();
        if (exclude_fields.find(field_id) != exclude_fields.end()) {
            continue;
        }
        FieldDataInfo info;
        if (with_mmap) {
            info.mmap_dir_path = "./data/mmap-test";
        }
        info.field_id = field_data.field_id();
        info.row_count = row_count;
        auto field_meta = fields.at(FieldId(field_id));
        info.channel->push(
            CreateFieldDataFromDataArray(row_count, &field_data, field_meta));
        info.channel->close();

        if (with_mmap) {
            seg.MapFieldData(FieldId(field_id), info);
        } else {
            seg.LoadFieldData(FieldId(field_id), info);
        }
    }
}

inline std::unique_ptr<SegmentSealed>
SealedCreator(SchemaPtr schema, const GeneratedData& dataset) {
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    return segment;
}

inline std::unique_ptr<milvus::index::VectorIndex>
GenVecIndexing(int64_t N, int64_t dim, const float* vec) {
    // {knowhere::IndexParams::nprobe, 10},
    auto conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                       {knowhere::meta::DIM, std::to_string(dim)},
                       {knowhere::indexparam::NLIST, "1024"},
                       {knowhere::meta::DEVICE_ID, 0}};
    auto database = knowhere::GenDataSet(N, dim, vec);
    auto indexing = std::make_unique<index::VectorMemNMIndex>(
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, knowhere::metric::L2);
    indexing->BuildWithDataset(database, conf);
    return indexing;
}

template <typename T>
inline index::IndexBasePtr
GenScalarIndexing(int64_t N, const T* data) {
    if constexpr (std::is_same_v<T, std::string>) {
        auto indexing = index::CreateStringIndexSort();
        indexing->Build(N, data);
        return indexing;
    } else {
        auto indexing = index::CreateScalarIndexSort<T>();
        indexing->Build(N, data);
        return indexing;
    }
}

inline std::vector<char>
translate_text_plan_to_binary_plan(const char* text_plan) {
    proto::plan::PlanNode plan_node;
    auto ok =
        google::protobuf::TextFormat::ParseFromString(text_plan, &plan_node);
    AssertInfo(ok, "Failed to parse");

    std::string binary_plan;
    plan_node.SerializeToString(&binary_plan);

    std::vector<char> ret;
    ret.resize(binary_plan.size());
    std::memcpy(ret.data(), binary_plan.c_str(), binary_plan.size());

    return ret;
}

inline auto
GenTss(int64_t num, int64_t begin_ts) {
    std::vector<Timestamp> tss(num, 0);
    std::iota(tss.begin(), tss.end(), begin_ts);
    return tss;
}

inline auto
GenPKs(int64_t num, int64_t begin_pk) {
    auto arr = std::make_unique<milvus::proto::schema::LongArray>();
    for (int64_t i = 0; i < num; i++) {
        arr->add_data(begin_pk + i);
    }
    auto ids = std::make_shared<IdArray>();
    ids->set_allocated_int_id(arr.release());
    return ids;
}

template <typename Iter>
inline auto
GenPKs(const Iter begin, const Iter end) {
    auto arr = std::make_unique<milvus::proto::schema::LongArray>();
    for (auto it = begin; it != end; it++) {
        arr->add_data(*it);
    }
    auto ids = std::make_shared<IdArray>();
    ids->set_allocated_int_id(arr.release());
    return ids;
}

inline auto
GenPKs(const std::vector<int64_t>& pks) {
    return GenPKs(pks.begin(), pks.end());
}

inline std::shared_ptr<knowhere::DataSet>
GenRandomIds(int rows, int64_t seed = 42) {
    std::mt19937 g(seed);
    auto* ids = new int64_t[rows];
    for (int i = 0; i < rows; ++i) ids[i] = i;
    std::shuffle(ids, ids + rows, g);
    auto ids_ds = GenIdsDataset(rows, ids);
    ids_ds->SetIsOwner(true);
    return ids_ds;
}

}  // namespace milvus::segcore
