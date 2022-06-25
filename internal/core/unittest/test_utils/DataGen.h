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
#include <google/protobuf/text_format.h>

#include "Constants.h"
#include "common/ArrowConverter.h"
#include "common/Schema.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
#include "knowhere/index/VecIndex.h"
#include "knowhere/index/VecIndexFactory.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "query/SearchOnIndex.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/Utils.h"

using boost::algorithm::starts_with;

namespace milvus::segcore {

struct GeneratedData {
    std::shared_ptr<arrow::Array> row_ids_;
    std::shared_ptr<arrow::Array> timestamps_;
    std::shared_ptr<InsertData> raw_;
    std::vector<FieldId> field_ids;
    SchemaPtr schema_;

    std::vector<int64_t> raw_row_ids_;
    std::vector<Timestamp> raw_timestamps_;

    template <typename T>
    std::vector<T>
    get_col(FieldId field_id) const {
        std::vector<T> ret(raw_->num_rows());
        int col_index = -1;
        for (const auto& target_field_data : raw_->schema()->fields()) {
            col_index++;
            if (field_id.get() != std::stoi(target_field_data->metadata()->Get(METADATA_FIELD_ID_KEY).ValueOrDie())) {
                continue;
            }

            auto raw_array_data = raw_->column(col_index)->data();
            auto& field_meta = schema_->operator[](field_id);
            if (field_meta.is_vector()) {
                if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                    int len = raw_->num_rows() * field_meta.get_dim();
                    ret.resize(len);
                    std::copy_n(
                        reinterpret_cast<const float*>(arrow::FixedSizeBinaryArray(raw_array_data).raw_values()), len,
                        ret.data());
                } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                    int len = raw_->num_rows() * (field_meta.get_dim() / 8);
                    ret.resize(len);
                    std::copy_n(arrow::FixedSizeBinaryArray(raw_array_data).raw_values(), len, ret.data());
                } else {
                    PanicInfo("unsupported");
                }

                return std::move(ret);
            }
            switch (field_meta.get_data_type()) {
                case DataType::BOOL: {
                    if constexpr (std::is_same_v<T, bool>) {
                        auto src_data = arrow::BooleanArray(raw_array_data);
                        std::copy(src_data.begin(), src_data.end(), ret.begin());
                        break;
                    }
                }
                case DataType::INT8:
                    std::copy_n(arrow::Int8Array(raw_array_data).raw_values(), raw_->num_rows(), ret.data());
                    break;
                case DataType::INT16:
                    std::copy_n(arrow::Int16Array(raw_array_data).raw_values(), raw_->num_rows(), ret.data());
                    break;
                case DataType::INT32: {
                    std::copy_n(arrow::Int32Array(raw_array_data).raw_values(), raw_->num_rows(), ret.data());
                    break;
                }
                case DataType::INT64: {
                    std::copy_n(arrow::Int64Array(raw_array_data).raw_values(), raw_->num_rows(), ret.data());
                    break;
                }
                case DataType::FLOAT: {
                    std::copy_n(arrow::FloatArray(raw_array_data).raw_values(), raw_->num_rows(), ret.data());
                    break;
                }
                case DataType::DOUBLE: {
                    std::copy_n(arrow::DoubleArray(raw_array_data).raw_values(), raw_->num_rows(), ret.data());
                    break;
                }
                case DataType::VARCHAR:
                case DataType::STRING: {
                    if constexpr (std::is_same_v<T, std::string>) {
                        auto src_data = arrow::StringArray(raw_array_data);
                        std::transform(src_data.begin(), src_data.end(), ret.data(),
                                       [&src_data](auto item) { return std::string(*item); });
                    }
                    break;
                }
                default: {
                    PanicInfo("unsupported");
                }
            }
        }
        return ret;
    }

    DataArray
    get_data_array(FieldId field_id) const {
        int index = 0;
        for (const auto& field : raw_->schema()->fields()) {
            if (field_id == milvus::GetFieldId(field)) {
                return DataArray{field, raw_->column(index)};
            }
            index++;
        }

        PanicInfo("field id not find");
    }

    const int64_t*
    get_raw_row_ids() {
        return raw_row_ids_.data();
    }

    const Timestamp*
    get_raw_timestamps() {
        return raw_timestamps_.data();
    }

 private:
    GeneratedData() = default;
    friend GeneratedData
    DataGen(SchemaPtr schema, int64_t N, uint64_t seed, uint64_t ts_offset, int repeat_count);
};

inline GeneratedData
DataGen(SchemaPtr schema, int64_t N, uint64_t seed = 42, uint64_t ts_offset = 0, int repeat_count = 1) {
    using std::vector;
    std::default_random_engine er(seed);
    std::normal_distribution<> distr(0, 1);
    int offset = 0;

    auto arrow_schema = milvus::ToArrowSchema(schema);
    std::vector<std::shared_ptr<arrow::Array>> columns;

    for (auto field_id : schema->get_field_ids()) {
        auto field_meta = (*schema)[field_id];
        switch (field_meta.get_data_type()) {
            case DataType::VECTOR_FLOAT: {
                auto dim = field_meta.get_dim();

                auto col_builder = arrow::FixedSizeBinaryBuilder(arrow::fixed_size_binary(dim * 4));
                bool is_ip = starts_with(field_meta.get_name().get(), "normalized");
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
                    col_builder.Append((const uint8_t*)data.data());
                }
                columns.push_back(col_builder.Finish().ValueOrDie());
                break;
            }
            case DataType::VECTOR_BINARY: {
                auto dim = field_meta.get_dim();
                Assert(dim % 8 == 0);
                auto builder = arrow::FixedSizeBinaryBuilder(arrow::fixed_size_binary(dim / 8));
                for (int i = 0; i < N; ++i) {
                    vector<uint8_t> data(dim / 8);
                    for (auto& x : data) {
                        x = er();
                    }
                    builder.Append(data.data());
                }
                columns.push_back(builder.Finish().ValueOrDie());
                break;
            }
            case DataType::INT64: {
                auto builder = arrow::Int64Builder();
                for (int i = 0; i < N; i++) {
                    builder.Append(i / repeat_count);
                }
                columns.push_back(builder.Finish().ValueOrDie());
                break;
            }
            case DataType::INT32: {
                auto builder = arrow::Int32Builder();
                for (int i = 0; i < N; i++) {
                    builder.Append(er() % (2 * N));
                }
                columns.push_back(builder.Finish().ValueOrDie());
                break;
            }
            case DataType::INT16: {
                auto builder = arrow::Int16Builder();
                for (int i = 0; i < N; i++) {
                    builder.Append(er() % (2 * N));
                }
                columns.push_back(builder.Finish().ValueOrDie());
                break;
            }
            case DataType::INT8: {
                auto builder = arrow::Int8Builder();
                for (int i = 0; i < N; i++) {
                    builder.Append(er() % (2 * N));
                }
                columns.push_back(builder.Finish().ValueOrDie());
                break;
            }
            case DataType::FLOAT: {
                auto builder = arrow::FloatBuilder();
                for (int i = 0; i < N; i++) {
                    builder.Append(distr(er));
                }
                columns.push_back(builder.Finish().ValueOrDie());
                break;
            }
            case DataType::DOUBLE: {
                auto builder = arrow::DoubleBuilder();
                for (int i = 0; i < N; i++) {
                    builder.Append(distr(er));
                }
                columns.push_back(builder.Finish().ValueOrDie());
                break;
            }
            case DataType::VARCHAR: {
                auto builder = arrow::StringBuilder();
                for (int i = 0; i < N / repeat_count; i++) {
                    auto str = std::to_string(er());
                    for (int j = 0; j < repeat_count; j++) {
                        builder.Append(str);
                    }
                }
                columns.push_back(builder.Finish().ValueOrDie());
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
    res.raw_ = arrow::RecordBatch::Make(arrow_schema, N, columns);

    auto row_ids_builder = arrow::Int64Builder();
    auto ts_builder = arrow::UInt64Builder();
    for (int i = 0; i < N; ++i) {
        row_ids_builder.Append(i);
        ts_builder.Append(i + ts_offset);

        res.raw_row_ids_.push_back(i);
        res.raw_timestamps_.push_back(i + ts_offset);
    }
    res.row_ids_ = row_ids_builder.Finish().ValueOrDie();
    res.timestamps_ = ts_builder.Finish().ValueOrDie();

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
CreatePlaceholderGroup(int64_t num_queries, int dim, const std::vector<float>& vecs) {
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
CreateBinaryPlaceholderGroup(int64_t num_queries, int64_t dim, int64_t seed = 42) {
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
CreateBinaryPlaceholderGroupFromBlob(int64_t num_queries, int64_t dim, const uint8_t* ptr) {
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
            result.emplace_back(std::make_pair(sr.seg_offsets_[index], sr.distances_[index]));
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
            result.emplace_back(std::to_string(sr.seg_offsets_[index]) + "->" + std::to_string(sr.distances_[index]));
        }
        results.emplace_back(std::move(result));
    }
    return json{results};
};

inline void
SealedLoadFieldData(const GeneratedData& dataset, SegmentSealed& seg, const std::set<int64_t>& exclude_fields = {}) {
    auto row_count = dataset.row_ids_->length();
    {
        LoadFieldDataInfo info;
        FieldMeta field_meta(FieldName("RowID"), RowFieldID, DataType::INT64);
        info.field_data = new DataArray{ToArrowField(field_meta), dataset.row_ids_};
        info.row_count = dataset.row_ids_->length();
        info.field_id = RowFieldID.get();  // field id for RowId
        seg.LoadFieldData(info);
    }
    {
        LoadFieldDataInfo info;
        FieldMeta field_meta(FieldName("Timestamp"), TimestampFieldID, DataType::INT64);
        info.field_data = new DataArray{ToArrowField(field_meta), dataset.timestamps_};
        info.row_count = dataset.timestamps_->length();
        info.field_id = TimestampFieldID.get();
        seg.LoadFieldData(info);
    }
    int index = 0;
    for (const auto& field : dataset.raw_->schema()->fields()) {
        int64_t field_id = std::stoi(field->metadata()->Get(METADATA_FIELD_ID_KEY).ValueOrDie());
        if (exclude_fields.find(field_id) != exclude_fields.end()) {
            continue;
        }
        LoadFieldDataInfo info;
        // auto data_type = std::stoi(field->metadata()->Get(METADATA_FIELD_TYPE_KEY).ValueOrDie());
        info.field_id = field_id;
        info.row_count = row_count;
        info.field_data = new DataArray{field, dataset.raw_->column(index)};
        seg.LoadFieldData(info);
        index++;
    }
}

inline std::unique_ptr<SegmentSealed>
SealedCreator(SchemaPtr schema, const GeneratedData& dataset) {
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);
    return segment;
}

inline knowhere::VecIndexPtr
GenVecIndexing(int64_t N, int64_t dim, const float* vec) {
    // {knowhere::IndexParams::nprobe, 10},
    auto conf = knowhere::Config{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                                 {knowhere::meta::DIM, dim},
                                 {knowhere::indexparam::NLIST, 1024},
                                 {knowhere::meta::DEVICE_ID, 0}};
    auto database = knowhere::GenDataset(N, dim, vec);
    auto indexing = std::make_shared<knowhere::IVF>();
    indexing->Train(database, conf);
    indexing->AddWithoutIds(database, conf);
    return indexing;
}

template <typename T>
inline scalar::IndexBasePtr
GenScalarIndexing(int64_t N, const T* data) {
    if constexpr (std::is_same_v<T, std::string>) {
        auto indexing = scalar::CreateStringIndexSort();
        indexing->Build(N, data);
        return indexing;
    } else {
        auto indexing = scalar::CreateScalarIndexSort<T>();
        indexing->Build(N, data);
        return indexing;
    }
}

inline std::vector<char>
translate_text_plan_to_binary_plan(const char* text_plan) {
    proto::plan::PlanNode plan_node;
    auto ok = google::protobuf::TextFormat::ParseFromString(text_plan, &plan_node);
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
    auto ids_builder = arrow::Int64Builder();
    for (int64_t i = 0; i < num; i++) {
        ids_builder.Append(begin_pk + i);
    }
    return ids_builder.Finish().ValueOrDie();
}

template <typename Iter>
inline auto
GenPKs(const Iter begin, const Iter end) {
    auto arr = std::make_unique<milvus::proto::schema::LongArray>();
    auto ids_builder = arrow::Int64Builder();
    for (auto it = begin; it != end; it++) {
        ids_builder.Append(*it);
    }
    return ids_builder.Finish().ValueOrDie();
}

inline auto
GenPKs(const std::vector<int64_t>& pks) {
    return GenPKs(pks.begin(), pks.end());
}

}  // namespace milvus::segcore
