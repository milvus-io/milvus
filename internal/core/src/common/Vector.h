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

#include <memory>

#include <boost/variant.hpp>
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"

namespace milvus {

/**
 * @brief base class for different type vector  
 * @todo implement full null value support
 */
class BaseVector {
 public:
    BaseVector(DataType data_type,
               size_t length,
               std::optional<size_t> null_count = std::nullopt)
        : type_kind_(data_type), length_(length), null_count_(null_count) {
    }
    virtual ~BaseVector() = default;

    int64_t
    size() {
        return length_;
    }

    DataType
    type() {
        return type_kind_;
    }

 protected:
    DataType type_kind_;
    size_t length_;
    std::optional<size_t> null_count_;
};

using VectorPtr = std::shared_ptr<BaseVector>;

/**
 * @brief Single vector for scalar types
 * @todo using memory pool && buffer replace FieldData
 */
class ColumnVector final : public BaseVector {
 public:
    ColumnVector(DataType data_type,
                 size_t length,
                 std::optional<size_t> null_count = std::nullopt)
        : BaseVector(data_type, length, null_count) {
        //todo: support null expr
        values_ = InitScalarFieldData(data_type, false, length);
    }

    //    ColumnVector(FixedVector<bool>&& data)
    //        : BaseVector(DataType::BOOL, data.size()) {
    //        values_ =
    //            std::make_shared<FieldData<bool>>(DataType::BOOL, std::move(data));
    //    }

    // the size is the number of bits
    ColumnVector(TargetBitmap&& bitmap)
        : BaseVector(DataType::INT8, bitmap.size()) {
        values_ = std::make_shared<FieldBitsetImpl<uint8_t>>(DataType::INT8,
                                                             std::move(bitmap));
    }

    virtual ~ColumnVector() override {
        values_.reset();
    }

    void*
    GetRawData() const {
        return values_->Data();
    }

    template <typename As>
    const As*
    RawAsValues() const {
        return reinterpret_cast<const As*>(values_->Data());
    }

 private:
    FieldDataPtr values_;
};

using ColumnVectorPtr = std::shared_ptr<ColumnVector>;

/**
 * @brief Multi vectors for scalar types
 * mainly using it to pass internal result in segcore scalar engine system
 */
class RowVector : public BaseVector {
 public:
    RowVector(std::vector<DataType>& data_types,
              size_t length,
              std::optional<size_t> null_count = std::nullopt)
        : BaseVector(DataType::ROW, length, null_count) {
        for (auto& type : data_types) {
            children_values_.emplace_back(
                std::make_shared<ColumnVector>(type, length));
        }
    }

    RowVector(const std::vector<VectorPtr>& children)
        : BaseVector(DataType::ROW, 0) {
        for (auto& child : children) {
            children_values_.push_back(child);
            if (child->size() > length_) {
                length_ = child->size();
            }
        }
    }

    RowVector(std::vector<VectorPtr>&& children)
        : BaseVector(DataType::ROW, 0) {
        children_values_ = std::move(children);
        for (auto& child : children_values_) {
            if (child->size() > length_) {
                length_ = child->size();
            }
        }
    }

    const std::vector<VectorPtr>&
    childrens() {
        return children_values_;
    }

    VectorPtr
    child(int index) {
        assert(index < children_values_.size());
        return children_values_[index];
    }

 private:
    std::vector<VectorPtr> children_values_;
};

using RowVectorPtr = std::shared_ptr<RowVector>;

class ConstantVector final : public BaseVector {
 public:
    ConstantVector(DataType data_type,
                   size_t length,
                   const proto::plan::GenericValue&& val,
                   std::optional<size_t> null_count = std::nullopt)
        : BaseVector(data_type, length, null_count), val_(val) {
    }

    const proto::plan::GenericValue&
    GetGenericValue() const {
        return val_;
    }

    bool ToBool() const {
        switch (type_kind_) {
            case DataType::BOOL:
                return val_.bool_val();
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
                return val_.int64_val();
            case DataType::FLOAT:
            case DataType::DOUBLE:
                return val_.float_val() != 0;
            default:
                return false;
        }
    }

 private:
    const proto::plan::GenericValue val_;
};

using ConstantVectorPtr = std::shared_ptr<ConstantVector>;

// TODO: simd support
class LazySegmentVector final : public BaseVector {
 public:
    LazySegmentVector(DataType data_type,
                      size_t length,
                      int64_t size_per_chunk,
                      std::optional<size_t> null_count = std::nullopt)
        : BaseVector(data_type, length, null_count),
          size_per_chunk_(size_per_chunk) {
    }

    // NOTE: same as PhyCompareFilterExpr
    using number = boost::variant<bool,
                                  int8_t,
                                  int16_t,
                                  int32_t,
                                  int64_t,
                                  float,
                                  double,
                                  std::string>;
    using ChunkDataAccessor = std::function<const number(int)>;
    void
    AddChunkData(ChunkDataAccessor accessor) {
        chunk_data_accesors_.push_back(accessor);
    }

    template<typename T>
    T GetValue(size_t i) {
        auto chunk_id = i / size_per_chunk_;
        auto chunk_pos = i % size_per_chunk_;
        auto& accessor = chunk_data_accesors_[chunk_id];
        auto value = accessor(chunk_pos);
        return boost::get<T>(value);
    }

 private:
    std::vector<ChunkDataAccessor> chunk_data_accesors_;
    const int64_t size_per_chunk_;
};
}  // namespace milvus
