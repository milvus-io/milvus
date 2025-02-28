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
#include <cmath>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include <cstdlib>

#include "Constants.h"
#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
#include "index/VectorMemIndex.h"
#include "segcore/Collection.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/Utils.h"
#include "knowhere/comp/index_param.h"

#include "PbHelper.h"
#include "segcore/collection_c.h"

using boost::algorithm::starts_with;

namespace milvus::segcore {

struct GeneratedData {
    std::vector<idx_t> row_ids_;
    std::vector<Timestamp> timestamps_;
    InsertRecordProto* raw_;
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
    FixedVector<T>
    get_col(FieldId field_id) const {
        FixedVector<T> ret(raw_->num_rows());
        for (auto i = 0; i < raw_->fields_data_size(); i++) {
            auto target_field_data = raw_->fields_data(i);
            if (field_id.get() != target_field_data.field_id()) {
                continue;
            }

            auto& field_meta = schema_->operator[](field_id);
            if (field_meta.is_vector() &&
                field_meta.get_data_type() != DataType::VECTOR_SPARSE_FLOAT) {
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
                } else if (field_meta.get_data_type() ==
                           DataType::VECTOR_FLOAT16) {
                    int len = raw_->num_rows() * field_meta.get_dim();
                    ret.resize(len);
                    auto src_data = reinterpret_cast<const T*>(
                        target_field_data.vectors().float16_vector().data());
                    std::copy_n(src_data, len, ret.data());
                } else if (field_meta.get_data_type() ==
                           DataType::VECTOR_BFLOAT16) {
                    int len = raw_->num_rows() * field_meta.get_dim();
                    ret.resize(len);
                    auto src_data = reinterpret_cast<const T*>(
                        target_field_data.vectors().bfloat16_vector().data());
                    std::copy_n(src_data, len, ret.data());
                } else if (field_meta.get_data_type() ==
                           DataType::VECTOR_INT8) {
                    int len = raw_->num_rows() * field_meta.get_dim();
                    ret.resize(len);
                    auto src_data = reinterpret_cast<const T*>(
                        target_field_data.vectors().int8_vector().data());
                    std::copy_n(src_data, len, ret.data());
                } else {
                    PanicInfo(Unsupported, "unsupported");
                }

                return std::move(ret);
            }
            if constexpr (std::is_same_v<T,
                                         knowhere::sparse::SparseRow<float>>) {
                auto sparse_float_array =
                    target_field_data.vectors().sparse_float_vector();
                auto rows = SparseBytesToRows(sparse_float_array.contents());
                std::copy_n(rows.get(), raw_->num_rows(), ret.data());
            } else if constexpr (std::is_same_v<T, ScalarArray>) {
                auto ret_data = reinterpret_cast<ScalarArray*>(ret.data());
                auto src_data = target_field_data.scalars().array_data().data();
                std::copy(src_data.begin(), src_data.end(), ret_data);
            } else {
                switch (field_meta.get_data_type()) {
                    case DataType::BOOL: {
                        auto src_data = reinterpret_cast<const T*>(
                            target_field_data.scalars()
                                .bool_data()
                                .data()
                                .data());
                        std::copy_n(src_data, raw_->num_rows(), ret.data());
                        break;
                    }
                    case DataType::INT8:
                    case DataType::INT16:
                    case DataType::INT32: {
                        auto src_data = reinterpret_cast<const int32_t*>(
                            target_field_data.scalars()
                                .int_data()
                                .data()
                                .data());
                        std::copy_n(src_data, raw_->num_rows(), ret.data());
                        break;
                    }
                    case DataType::INT64: {
                        auto src_data = reinterpret_cast<const T*>(
                            target_field_data.scalars()
                                .long_data()
                                .data()
                                .data());
                        std::copy_n(src_data, raw_->num_rows(), ret.data());
                        break;
                    }
                    case DataType::FLOAT: {
                        auto src_data = reinterpret_cast<const T*>(
                            target_field_data.scalars()
                                .float_data()
                                .data()
                                .data());
                        std::copy_n(src_data, raw_->num_rows(), ret.data());
                        break;
                    }
                    case DataType::DOUBLE: {
                        auto src_data = reinterpret_cast<const T*>(
                            target_field_data.scalars()
                                .double_data()
                                .data()
                                .data());
                        std::copy_n(src_data, raw_->num_rows(), ret.data());
                        break;
                    }
                    case DataType::VARCHAR: {
                        auto ret_data =
                            reinterpret_cast<std::string*>(ret.data());
                        auto src_data =
                            target_field_data.scalars().string_data().data();
                        std::copy(src_data.begin(), src_data.end(), ret_data);
                        break;
                    }
                    case DataType::JSON: {
                        auto ret_data =
                            reinterpret_cast<std::string*>(ret.data());
                        auto src_data =
                            target_field_data.scalars().json_data().data();
                        std::copy(src_data.begin(), src_data.end(), ret_data);
                        break;
                    }
                    default: {
                        PanicInfo(Unsupported, "unsupported");
                    }
                }
            }
        }
        return std::move(ret);
    }

    FixedVector<bool>
    get_col_valid(FieldId field_id) const {
        for (const auto& target_field_data : raw_->fields_data()) {
            if (field_id.get() == target_field_data.field_id()) {
                auto& field_meta = schema_->operator[](field_id);
                Assert(field_meta.is_nullable());
                FixedVector<bool> ret(raw_->num_rows());
                auto src_data = target_field_data.valid_data().data();
                std::copy_n(src_data, raw_->num_rows(), ret.data());
                return ret;
            }
        }
        PanicInfo(FieldIDInvalid, "field id not find");
    }

    std::unique_ptr<DataArray>
    get_col(FieldId field_id) const {
        for (const auto& target_field_data : raw_->fields_data()) {
            if (field_id.get() == target_field_data.field_id()) {
                return std::make_unique<DataArray>(target_field_data);
            }
        }

        PanicInfo(FieldIDInvalid, "field id not find");
    }

    GeneratedData() = default;

 private:
    friend GeneratedData
    DataGen(SchemaPtr schema,
            int64_t N,
            uint64_t seed,
            uint64_t ts_offset,
            int repeat_count,
            int array_len,
            bool random_pk,
            bool random_val);
    friend GeneratedData
    DataGenForJsonArray(SchemaPtr schema,
                        int64_t N,
                        uint64_t seed,
                        uint64_t ts_offset,
                        int repeat_count,
                        int array_len);
};

inline std::unique_ptr<knowhere::sparse::SparseRow<float>[]>
GenerateRandomSparseFloatVector(size_t rows,
                                size_t cols = kTestSparseDim,
                                float density = kTestSparseVectorDensity,
                                int seed = 42) {
    int32_t num_elements = static_cast<int32_t>(rows * cols * density);

    std::mt19937 rng(seed);
    auto real_distrib = std::uniform_real_distribution<float>(0, 1);
    auto row_distrib = std::uniform_int_distribution<int32_t>(0, rows - 1);
    auto col_distrib = std::uniform_int_distribution<int32_t>(0, cols - 1);

    std::vector<std::map<int32_t, float>> data(rows);

    // ensure the actual dim of the entire generated dataset is cols.
    data[0][cols - 1] = real_distrib(rng);
    --num_elements;

    // Ensure each row has at least one non-zero value
    for (size_t i = 0; i < rows; ++i) {
        auto col = col_distrib(rng);
        float val = real_distrib(rng);
        data[i][col] = val;
    }
    num_elements -= rows;

    for (int32_t i = 0; i < num_elements; ++i) {
        auto row = row_distrib(rng);
        while (data[row].size() == (size_t)cols) {
            row = row_distrib(rng);
        }
        auto col = col_distrib(rng);
        while (data[row].find(col) != data[row].end()) {
            col = col_distrib(rng);
        }
        auto val = real_distrib(rng);
        data[row][col] = val;
    }

    auto tensor = std::make_unique<knowhere::sparse::SparseRow<float>[]>(rows);

    for (int32_t i = 0; i < rows; ++i) {
        if (data[i].size() == 0) {
            continue;
        }
        knowhere::sparse::SparseRow<float> row(data[i].size());
        size_t j = 0;
        for (auto& [idx, val] : data[i]) {
            row.set_at(j++, idx, val);
        }
        tensor[i] = std::move(row);
    }
    return tensor;
}

inline GeneratedData DataGen(SchemaPtr schema,
                             int64_t N,
                             uint64_t seed = 42,
                             uint64_t ts_offset = 0,
                             int repeat_count = 1,
                             int array_len = 10,
                             bool random_pk = false,
                             bool random_val = true,
                             bool random_valid = false) {
    using std::vector;
    std::default_random_engine random(seed);
    std::normal_distribution<> distr(0, 1);
    int offset = 0;

    auto insert_data = std::make_unique<InsertRecordProto>();
    auto insert_cols =
        [&insert_data](
            auto& data, int64_t count, auto& field_meta, bool random_valid) {
            FixedVector<bool> valid_data(count);
            if (field_meta.is_nullable()) {
                for (int i = 0; i < count; ++i) {
                    int x = i;
                    if (random_valid)
                        x = rand();
                    valid_data[i] = x % 2 == 0 ? true : false;
                }
            }
            auto array = milvus::segcore::CreateDataArrayFrom(
                data.data(), valid_data.data(), count, field_meta);
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
                insert_cols(final, N, field_meta, random_valid);
                break;
            }
            case DataType::VECTOR_BINARY: {
                auto dim = field_meta.get_dim();
                Assert(dim % 8 == 0);
                vector<uint8_t> data(dim / 8 * N);
                for (auto& x : data) {
                    x = random();
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::VECTOR_FLOAT16: {
                auto dim = field_meta.get_dim();
                vector<float16> final(dim * N);
                for (auto& x : final) {
                    x = float16(distr(random) + offset);
                }
                insert_cols(final, N, field_meta, random_valid);
                break;
            }
            case DataType::VECTOR_SPARSE_FLOAT: {
                auto res = GenerateRandomSparseFloatVector(
                    N, kTestSparseDim, kTestSparseVectorDensity, seed);
                auto array = milvus::segcore::CreateDataArrayFrom(
                    res.get(), nullptr, N, field_meta);
                insert_data->mutable_fields_data()->AddAllocated(
                    array.release());
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                auto dim = field_meta.get_dim();
                vector<bfloat16> final(dim * N);
                for (auto& x : final) {
                    x = bfloat16(distr(random) + offset);
                }
                insert_cols(final, N, field_meta, random_valid);
                break;
            }
            case DataType::VECTOR_INT8: {
                auto dim = field_meta.get_dim();
                vector<int8> final(dim * N);
                srand(seed);
                for (auto& x : final) {
                    x = int8_t(rand() % 256 - 128);
                }
                insert_cols(final, N, field_meta, random_valid);
                break;
            }
            case DataType::BOOL: {
                FixedVector<bool> data(N);
                for (int i = 0; i < N; ++i) {
                    data[i] = i % 2 == 0 ? true : false;
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::INT64: {
                vector<int64_t> data(N);
                for (int i = 0; i < N; i++) {
                    if (random_pk && schema->get_primary_field_id()->get() ==
                                         field_id.get()) {
                        data[i] = random();
                    } else {
                        data[i] = i / repeat_count;
                    }
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::INT32: {
                vector<int> data(N);
                for (int i = 0; i < N; i++) {
                    int x = 0;
                    if (random_val)
                        x = random() % (2 * N);
                    else
                        x = i / repeat_count;
                    data[i] = x;
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::INT16: {
                vector<int16_t> data(N);
                for (int i = 0; i < N; i++) {
                    int16_t x = 0;
                    if (random_val)
                        x = random() % (2 * N);
                    else
                        x = i / repeat_count;
                    data[i] = x;
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::INT8: {
                vector<int8_t> data(N);
                for (int i = 0; i < N; i++) {
                    int8_t x = 0;
                    if (random_val)
                        x = random() % (2 * N);
                    else
                        x = i / repeat_count;
                    data[i] = x;
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::FLOAT: {
                vector<float> data(N);
                for (auto& x : data) {
                    x = distr(random);
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::DOUBLE: {
                vector<double> data(N);
                for (auto& x : data) {
                    x = distr(random);
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::VARCHAR: {
                vector<std::string> data(N);
                for (int i = 0; i < N / repeat_count; i++) {
                    auto str = std::to_string(random());
                    for (int j = 0; j < repeat_count; j++) {
                        data[i * repeat_count + j] = str;
                    }
                }
                std::sort(data.begin(), data.end());
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::JSON: {
                vector<std::string> data(N);
                for (int i = 0; i < N / repeat_count; i++) {
                    auto str = R"({"int":)" + std::to_string(random()) +
                               R"(,"double":)" +
                               std::to_string(static_cast<double>(random())) +
                               R"(,"string":")" + std::to_string(random()) +
                               R"(","bool": true)" + R"(, "array": [1,2,3])" +
                               "}";
                    data[i] = str;
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            case DataType::ARRAY: {
                vector<ScalarArray> data(N);
                switch (field_meta.get_element_type()) {
                    case DataType::BOOL: {
                        for (int i = 0; i < N / repeat_count; i++) {
                            milvus::proto::schema::ScalarField field_data;

                            for (int j = 0; j < array_len; j++) {
                                field_data.mutable_bool_data()->add_data(
                                    static_cast<bool>(random()));
                            }
                            data[i] = field_data;
                        }
                        break;
                    }
                    case DataType::INT8: {
                        for (int i = 0; i < N / repeat_count; i++) {
                            milvus::proto::schema::ScalarField field_data;

                            for (int j = 0; j < array_len; j++) {
                                field_data.mutable_int_data()->add_data(
                                    static_cast<int8_t>(random()));
                            }
                            data[i] = field_data;
                        }
                        break;
                    }
                    case DataType::INT16: {
                        for (int i = 0; i < N / repeat_count; i++) {
                            milvus::proto::schema::ScalarField field_data;

                            for (int j = 0; j < array_len; j++) {
                                field_data.mutable_int_data()->add_data(
                                    static_cast<int16_t>(random()));
                            }
                            data[i] = field_data;
                        }
                        break;
                    }
                    case DataType::INT32: {
                        for (int i = 0; i < N / repeat_count; i++) {
                            milvus::proto::schema::ScalarField field_data;

                            for (int j = 0; j < array_len; j++) {
                                field_data.mutable_int_data()->add_data(
                                    static_cast<int>(random()));
                            }
                            data[i] = field_data;
                        }
                        break;
                    }
                    case DataType::INT64: {
                        for (int i = 0; i < N / repeat_count; i++) {
                            milvus::proto::schema::ScalarField field_data;
                            for (int j = 0; j < array_len; j++) {
                                field_data.mutable_long_data()->add_data(
                                    static_cast<int64_t>(random()));
                            }
                            data[i] = field_data;
                        }
                        break;
                    }
                    case DataType::STRING:
                    case DataType::VARCHAR: {
                        for (int i = 0; i < N / repeat_count; i++) {
                            milvus::proto::schema::ScalarField field_data;

                            for (int j = 0; j < array_len; j++) {
                                field_data.mutable_string_data()->add_data(
                                    std::to_string(random()));
                            }
                            data[i] = field_data;
                        }
                        break;
                    }
                    case DataType::FLOAT: {
                        for (int i = 0; i < N / repeat_count; i++) {
                            milvus::proto::schema::ScalarField field_data;

                            for (int j = 0; j < array_len; j++) {
                                field_data.mutable_float_data()->add_data(
                                    static_cast<float>(random()));
                            }
                            data[i] = field_data;
                        }
                        break;
                    }
                    case DataType::DOUBLE: {
                        for (int i = 0; i < N / repeat_count; i++) {
                            milvus::proto::schema::ScalarField field_data;

                            for (int j = 0; j < array_len; j++) {
                                field_data.mutable_double_data()->add_data(
                                    static_cast<double>(random()));
                            }
                            data[i] = field_data;
                        }
                        break;
                    }
                    default: {
                        throw std::runtime_error("unsupported data type");
                    }
                }
                insert_cols(data, N, field_meta, random_valid);
                break;
            }
            default: {
                throw SegcoreError(ErrorCode::NotImplemented, "unimplemented");
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

    auto insert_data = std::make_unique<InsertRecordProto>();
    auto insert_cols = [&insert_data](
                           auto& data, int64_t count, auto& field_meta) {
        FixedVector<bool> valid_data(count);
        if (field_meta.is_nullable()) {
            for (int i = 0; i < count; ++i) {
                valid_data[i] = i % 2 == 0 ? true : false;
            }
        }
        auto array = milvus::segcore::CreateDataArrayFrom(
            data.data(), valid_data.data(), count, field_meta);
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
                    std::vector<std::string> arrayVec;
                    for (int j = 0; j < array_len; ++j) {
                        intVec.push_back(std::to_string(er()));
                        doubleVec.push_back(
                            std::to_string(static_cast<double>(er())));
                        stringVec.push_back("\"" + std::to_string(er()) + "\"");
                        boolVec.push_back(i % 2 == 0 ? "true" : "false");
                        arrayVec.push_back(
                            fmt::format("[{}, {}, {}]", i, i + 1, i + 2));
                    }
                    auto str =
                        R"({"int":[)" + join(intVec, ",") + R"(],"double":[)" +
                        join(doubleVec, ",") + R"(],"string":[)" +
                        join(stringVec, ",") + R"(],"bool": [)" +
                        join(boolVec, ",") + R"(],"array": [)" +
                        join(arrayVec, ",") + R"(],"array2": [[1,2], [3,4]])" +
                        R"(,"array3": [[1,2.2,false,"abc"]])" +
                        R"(,"diff_type_array": [1,2.2,true,"abc"])" + "}";
                    data[i] = str;
                }
                insert_cols(data, N, field_meta);
                break;
            }
            default: {
                throw SegcoreError(ErrorCode::NotImplemented, "unimplemented");
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

template <class TraitType = milvus::FloatVector>
auto
CreatePlaceholderGroup(int64_t num_queries, int dim, int64_t seed = 42) {
    if (std::is_same_v<TraitType, milvus::BinaryVector>) {
        assert(dim % 8 == 0);
    }
    namespace ser = milvus::proto::common;
    GET_ELEM_TYPE_FOR_VECTOR_TRAIT

    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(TraitType::placeholder_type);
    // TODO caiyd: need update for Int8Vector
    std::normal_distribution<double> dis(0, 1);
    std::default_random_engine e(seed);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<elem_type> vec;
        for (int d = 0; d < dim / TraitType::dim_factor; ++d) {
            if (std::is_same_v<TraitType, milvus::BinaryVector>) {
                vec.push_back(e());
            } else {
                vec.push_back(elem_type(dis(e)));
            }
        }
        value->add_values(vec.data(), vec.size() * sizeof(elem_type));
    }
    return raw_group;
}

template <class TraitType = milvus::FloatVector>
inline auto
CreatePlaceholderGroupFromBlob(int64_t num_queries, int dim, const void* src) {
    if (std::is_same_v<TraitType, milvus::BinaryVector>) {
        assert(dim % 8 == 0);
    }
    namespace ser = milvus::proto::common;
    GET_ELEM_TYPE_FOR_VECTOR_TRAIT

    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(TraitType::placeholder_type);
    int64_t src_index = 0;

    for (int i = 0; i < num_queries; ++i) {
        std::vector<elem_type> vec;
        for (int d = 0; d < dim / TraitType::dim_factor; ++d) {
            vec.push_back(((elem_type*)src)[src_index++]);
        }
        value->add_values(vec.data(), vec.size() * sizeof(elem_type));
    }
    return raw_group;
}

inline auto
CreateSparseFloatPlaceholderGroup(int64_t num_queries, int64_t seed = 42) {
    namespace ser = milvus::proto::common;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();

    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::SparseFloatVector);
    auto sparse_vecs = GenerateRandomSparseFloatVector(
        num_queries, kTestSparseDim, kTestSparseVectorDensity, seed);
    for (int i = 0; i < num_queries; ++i) {
        value->add_values(sparse_vecs[i].data(),
                          sparse_vecs[i].data_byte_size());
    }
    return raw_group;
}

inline auto
CreateInt8PlaceholderGroup(int64_t num_queries,
                           int64_t dim,
                           int64_t seed = 42) {
    namespace ser = milvus::proto::common;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::Int8Vector);
    std::default_random_engine e(seed);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<int8> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(e());
        }
        value->add_values(vec.data(), vec.size() * sizeof(int8));
    }
    return raw_group;
}

inline auto
CreateInt8PlaceholderGroupFromBlob(int64_t num_queries,
                                   int64_t dim,
                                   const int8* ptr) {
    namespace ser = milvus::proto::common;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::Int8Vector);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<int8> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(*ptr);
            ++ptr;
        }
        value->add_values(vec.data(), vec.size() * sizeof(int8));
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

inline nlohmann::json
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
    return nlohmann::json{results};
};

inline FieldDataPtr
CreateFieldDataFromDataArray(ssize_t raw_count,
                             const DataArray* data,
                             const FieldMeta& field_meta) {
    int64_t dim = 1;
    FieldDataPtr field_data = nullptr;

    auto createFieldData = [&field_data, &raw_count](const void* raw_data,
                                                     DataType data_type,
                                                     int64_t dim) {
        field_data = storage::CreateFieldData(data_type, false, dim);
        field_data->FillFieldData(raw_data, raw_count);
    };
    auto createNullableFieldData = [&field_data, &raw_count](
                                       const void* raw_data,
                                       const bool* raw_valid_data,
                                       DataType data_type,
                                       int64_t dim) {
        field_data = storage::CreateFieldData(data_type, true, dim);
        int byteSize = (raw_count + 7) / 8;
        uint8_t* valid_data = new uint8_t[byteSize];
        for (int i = 0; i < raw_count; i++) {
            bool value = raw_valid_data[i];
            int byteIndex = i / 8;
            int bitIndex = i % 8;
            if (value) {
                valid_data[byteIndex] |= (1 << bitIndex);
            } else {
                valid_data[byteIndex] &= ~(1 << bitIndex);
            }
        }
        field_data->FillFieldData(raw_data, valid_data, raw_count);
        delete[] valid_data;
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
            case DataType::VECTOR_FLOAT16: {
                auto raw_data = data->vectors().float16_vector().data();
                dim = field_meta.get_dim();
                createFieldData(raw_data, DataType::VECTOR_FLOAT16, dim);
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                auto raw_data = data->vectors().bfloat16_vector().data();
                dim = field_meta.get_dim();
                createFieldData(raw_data, DataType::VECTOR_BFLOAT16, dim);
                break;
            }
            case DataType::VECTOR_SPARSE_FLOAT: {
                auto sparse_float_array = data->vectors().sparse_float_vector();
                auto rows = SparseBytesToRows(sparse_float_array.contents());
                createFieldData(rows.get(), DataType::VECTOR_SPARSE_FLOAT, 0);
                break;
            }
            case DataType::VECTOR_INT8: {
                auto raw_data = data->vectors().int8_vector().data();
                dim = field_meta.get_dim();
                createFieldData(raw_data, DataType::VECTOR_INT8, dim);
                break;
            }
            default: {
                PanicInfo(Unsupported, "unsupported");
            }
        }
    } else {
        switch (field_meta.get_data_type()) {
            case DataType::BOOL: {
                auto raw_data = data->scalars().bool_data().data().data();
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(
                        raw_data, raw_valid_data, DataType::BOOL, dim);
                } else {
                    createFieldData(raw_data, DataType::BOOL, dim);
                }
                break;
            }
            case DataType::INT8: {
                auto src_data = data->scalars().int_data().data();
                std::vector<int8_t> data_raw(src_data.size());
                std::copy_n(src_data.data(), src_data.size(), data_raw.data());
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(
                        data_raw.data(), raw_valid_data, DataType::INT8, dim);
                } else {
                    createFieldData(data_raw.data(), DataType::INT8, dim);
                }
                break;
            }
            case DataType::INT16: {
                auto src_data = data->scalars().int_data().data();
                std::vector<int16_t> data_raw(src_data.size());
                std::copy_n(src_data.data(), src_data.size(), data_raw.data());
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(
                        data_raw.data(), raw_valid_data, DataType::INT16, dim);
                } else {
                    createFieldData(data_raw.data(), DataType::INT16, dim);
                }
                break;
            }
            case DataType::INT32: {
                auto raw_data = data->scalars().int_data().data().data();
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(
                        raw_data, raw_valid_data, DataType::INT32, dim);
                } else {
                    createFieldData(raw_data, DataType::INT32, dim);
                }
                break;
            }
            case DataType::INT64: {
                auto raw_data = data->scalars().long_data().data().data();
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(
                        raw_data, raw_valid_data, DataType::INT64, dim);
                } else {
                    createFieldData(raw_data, DataType::INT64, dim);
                }
                break;
            }
            case DataType::FLOAT: {
                auto raw_data = data->scalars().float_data().data().data();
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(
                        raw_data, raw_valid_data, DataType::FLOAT, dim);
                } else {
                    createFieldData(raw_data, DataType::FLOAT, dim);
                }
                break;
            }
            case DataType::DOUBLE: {
                auto raw_data = data->scalars().double_data().data().data();
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(
                        raw_data, raw_valid_data, DataType::DOUBLE, dim);
                } else {
                    createFieldData(raw_data, DataType::DOUBLE, dim);
                }
                break;
            }
            case DataType::VARCHAR: {
                auto begin = data->scalars().string_data().data().begin();
                auto end = data->scalars().string_data().data().end();
                std::vector<std::string> data_raw(begin, end);
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(data_raw.data(),
                                            raw_valid_data,
                                            DataType::VARCHAR,
                                            dim);
                } else {
                    createFieldData(data_raw.data(), DataType::VARCHAR, dim);
                }
                break;
            }
            case DataType::JSON: {
                auto src_data = data->scalars().json_data().data();
                std::vector<Json> data_raw(src_data.size());
                for (int i = 0; i < src_data.size(); i++) {
                    auto str = src_data.Get(i);
                    data_raw[i] = Json(simdjson::padded_string(str));
                }
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(
                        data_raw.data(), raw_valid_data, DataType::JSON, dim);
                } else {
                    createFieldData(data_raw.data(), DataType::JSON, dim);
                }
                break;
            }
            case DataType::ARRAY: {
                auto src_data = data->scalars().array_data().data();
                std::vector<Array> data_raw(src_data.size());
                for (int i = 0; i < src_data.size(); i++) {
                    data_raw[i] = Array(src_data.at(i));
                }
                if (field_meta.is_nullable()) {
                    auto raw_valid_data = data->valid_data().data();
                    createNullableFieldData(
                        data_raw.data(), raw_valid_data, DataType::ARRAY, dim);
                } else {
                    createFieldData(data_raw.data(), DataType::ARRAY, dim);
                }
                break;
            }
            default: {
                PanicInfo(Unsupported, "unsupported");
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
        auto field_data = std::make_shared<milvus::FieldData<int64_t>>(
            DataType::INT64, false);
        field_data->FillFieldData(dataset.row_ids_.data(), row_count);
        auto field_data_info =
            FieldDataInfo(RowFieldID.get(),
                          row_count,
                          std::vector<milvus::FieldDataPtr>{field_data});
        seg.LoadFieldData(RowFieldID, field_data_info);
    }
    {
        auto field_data = std::make_shared<milvus::FieldData<int64_t>>(
            DataType::INT64, false);
        field_data->FillFieldData(dataset.timestamps_.data(), row_count);
        auto field_data_info =
            FieldDataInfo(TimestampFieldID.get(),
                          row_count,
                          std::vector<milvus::FieldDataPtr>{field_data});
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
GenVecIndexing(int64_t N,
               int64_t dim,
               const float* vec,
               const char* index_type) {
    auto conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                       {knowhere::meta::DIM, std::to_string(dim)},
                       {knowhere::indexparam::NLIST, "1024"},
                       {knowhere::meta::DEVICE_ID, 0}};
    auto database = knowhere::GenDataSet(N, dim, vec);
    milvus::storage::FieldDataMeta field_data_meta{1, 2, 3, 100};
    milvus::storage::IndexMeta index_meta{3, 100, 1000, 1};
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestRemotePath;
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    milvus::storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, chunk_manager);
    auto indexing = std::make_unique<index::VectorMemIndex<float>>(
        index_type,
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        file_manager_context);
    indexing->BuildWithDataset(database, conf);
    auto create_index_result = indexing->Upload();
    auto index_files = create_index_result->GetIndexFiles();
    conf["index_files"] = index_files;
    // we need a load stage to use index as the producation does
    // knowhere would do some data preparation in this stage
    indexing->Load(milvus::tracer::TraceContext{}, conf);
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

// we have lots of tests with literal string plan with hard coded metric type,
// so creating a helper function to replace metric type for different metrics.
inline std::vector<char>
replace_metric_and_translate_text_plan_to_binary_plan(
    std::string plan, knowhere::MetricType metric_type) {
    if (metric_type != knowhere::metric::L2) {
        std::string replace = R"(metric_type: "L2")";
        std::string target = "metric_type: \"" + metric_type + "\"";
        size_t pos = 0;
        while ((pos = plan.find(replace, pos)) != std::string::npos) {
            plan.replace(pos, replace.length(), target);
            pos += target.length();
        }
    }
    return translate_text_plan_to_binary_plan(plan.c_str());
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

inline CCollection
NewCollection(const char* schema_proto_blob,
              const MetricType metric_type = knowhere::metric::L2) {
    auto proto = std::string(schema_proto_blob);
    auto collection = std::make_unique<milvus::segcore::Collection>(proto);
    auto schema = collection->get_schema();
    milvus::proto::segcore::CollectionIndexMeta col_index_meta;
    for (auto field : schema->get_fields()) {
        auto field_index_meta = col_index_meta.add_index_metas();
        auto index_param = field_index_meta->add_index_params();
        index_param->set_key("metric_type");
        index_param->set_value(metric_type);
        field_index_meta->set_fieldid(field.first.get());
    }

    collection->set_index_meta(
        std::make_shared<CollectionIndexMeta>(col_index_meta));
    return (void*)collection.release();
}

inline CCollection
NewCollection(const milvus::proto::schema::CollectionSchema* schema,
              MetricType metric_type = knowhere::metric::L2) {
    auto collection = std::make_unique<milvus::segcore::Collection>(schema);
    milvus::proto::segcore::CollectionIndexMeta col_index_meta;
    for (auto field : collection->get_schema()->get_fields()) {
        auto field_index_meta = col_index_meta.add_index_metas();
        auto index_param = field_index_meta->add_index_params();
        index_param->set_key("metric_type");
        index_param->set_value(metric_type);
        field_index_meta->set_fieldid(field.first.get());
    }
    collection->set_index_meta(
        std::make_shared<CollectionIndexMeta>(col_index_meta));
    return (void*)collection.release();
}

}  // namespace milvus::segcore
