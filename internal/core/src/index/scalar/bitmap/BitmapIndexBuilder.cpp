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

#include "index/scalar/bitmap/BitmapIndexBuilder.h"

#include <limits>
#include <type_traits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/bitmap/BitmapIndexArtifact.h"

namespace milvus::index {
namespace {

using bitmap_params::IsStringType;

bool
CompatibleArrayType(DataType actual, DataType expected) {
    return actual == expected ||
           (IsStringType(actual) && IsStringType(expected));
}

BitmapBuildParams
ParseBuildParams(const Config& params, DataType default_type, bool array) {
    BitmapBuildParams parsed;
    parsed.nested = ReadNestedConfigParam(params, "bitmap").value_or(false);
    parsed.nullable =
        GetValueFromConfigOrFallback<bool>(params, "nullable", false);
    const auto field_type =
        ReadDataTypeParam(params, "field_type")
            .value_or(array ? DataType::ARRAY : default_type);
    const auto array_element_type =
        ReadDataTypeParam(params, "array_element_type")
            .value_or(DataType::NONE);
    const auto configured_value_type =
        ReadDataTypeParam(params, "value_type").value_or(DataType::NONE);
    auto value_type =
        field_type == DataType::ARRAY && array_element_type != DataType::NONE
            ? array_element_type
            : configured_value_type;
    if (value_type == DataType::NONE || value_type == DataType::ARRAY) {
        value_type = field_type == DataType::ARRAY ? default_type : field_type;
    }
    parsed.value_type = value_type;
    return parsed;
}

void
CheckAppend(size_t current, size_t count) {
    constexpr uint64_t kMaxCoordinateCount =
        static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
    if (count > std::numeric_limits<size_t>::max() - current ||
        current > kMaxCoordinateCount ||
        count > kMaxCoordinateCount - current) {
        ThrowInfo(DataTypeInvalid,
                  "bitmap coordinate count {} + {} exceeds Roaring's uint32 "
                  "domain",
                  current,
                  count);
    }
}

template <typename T>
owned_t<T>
OwnValue(const T& value) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        return std::string(value);
    } else {
        return value;
    }
}

}  // namespace

template <typename T>
BitmapIndexBuilder<T>::BitmapIndexBuilder(BitmapBuildParams params)
    : params_(std::move(params)) {
    if (params_.value_type == DataType::NONE ||
        params_.value_type == DataType::ARRAY) {
        params_.value_type = CppDataType<T>();
    }
}

template <typename T>
BitmapIndexBuilder<T>::~BitmapIndexBuilder() = default;

template <typename T>
storage::ArtifactPtr
BitmapIndexBuilder<T>::Build(const ScalarBuildInput<T>& input) && {
    for (const auto& batch : input.batches) {
        const auto n = batch.values.size();
        CheckAppend(total_num_rows_, n);
        validity_.resize(total_num_rows_ + n, true);
        for (size_t i = 0; i < n; ++i) {
            const auto coordinate = total_num_rows_ + i;

            // Nested callers already removed null/empty source rows and
            // flattened elements. Every supplied item owns one coordinate.
            const bool is_valid =
                params_.nested || !batch.validity || batch.validity[i];
            if (is_valid) {
                postings_[OwnValue(batch.values[i])].add(
                    static_cast<uint32_t>(coordinate));
            } else {
                validity_.reset(coordinate);
            }
        }
        total_num_rows_ += n;
    }
    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "bitmap index cannot build empty input");
    }
    return std::make_unique<BitmapIndexArtifact<StoredT>>(std::move(postings_),
                                                          std::move(validity_),
                                                          total_num_rows_,
                                                          params_.nested,
                                                          params_.nullable);
}

class BitmapArrayIndexBuilder::Impl {
 public:
    virtual ~Impl() = default;

    virtual void
    AddRow(const ArrayView& value, uint32_t coordinate) = 0;

    virtual storage::ArtifactPtr
    Finish(TargetBitmap validity,
           size_t count,
           const BitmapBuildParams& params) = 0;
};

namespace {

template <typename T>
class BitmapArrayBuilderImpl final : public BitmapArrayIndexBuilder::Impl {
 public:
    void
    AddRow(const ArrayView& value, uint32_t coordinate) override {
        for (int i = 0; i < value.length(); ++i) {
            postings_[OwnValue(value.get_data<T>(i))].add(coordinate);
        }
    }

    storage::ArtifactPtr
    Finish(TargetBitmap validity,
           size_t count,
           const BitmapBuildParams& params) override {
        return std::make_unique<BitmapIndexArtifact<owned_t<T>>>(
            std::move(postings_),
            std::move(validity),
            count,
            false,
            params.nullable);
    }

 private:
    BitmapRoaringPostingMap<owned_t<T>> postings_;
};

std::unique_ptr<BitmapArrayIndexBuilder::Impl>
MakeArrayImpl(DataType element_type) {
    switch (element_type) {
        case DataType::BOOL:
            return std::make_unique<BitmapArrayBuilderImpl<bool>>();
        case DataType::INT8:
            return std::make_unique<BitmapArrayBuilderImpl<int8_t>>();
        case DataType::INT16:
            return std::make_unique<BitmapArrayBuilderImpl<int16_t>>();
        case DataType::INT32:
            return std::make_unique<BitmapArrayBuilderImpl<int32_t>>();
        case DataType::INT64:
            return std::make_unique<BitmapArrayBuilderImpl<int64_t>>();
        case DataType::FLOAT:
            return std::make_unique<BitmapArrayBuilderImpl<float>>();
        case DataType::DOUBLE:
            return std::make_unique<BitmapArrayBuilderImpl<double>>();
        case DataType::STRING:
        case DataType::VARCHAR:
            return std::make_unique<BitmapArrayBuilderImpl<std::string_view>>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported ARRAY element type {} for bitmap index",
                      static_cast<int>(element_type));
    }
}

}  // namespace

BitmapArrayIndexBuilder::BitmapArrayIndexBuilder(BitmapBuildParams params)
    : params_(std::move(params)), impl_(MakeArrayImpl(params_.value_type)) {
    AssertInfo(!params_.nested,
               "nested ARRAY bitmap input must be flattened into typed "
               "elements");
}

BitmapArrayIndexBuilder::~BitmapArrayIndexBuilder() = default;

storage::ArtifactPtr
BitmapArrayIndexBuilder::Build(const ScalarBuildInput<ArrayView>& input) && {
    for (const auto& batch : input.batches) {
        const auto n = batch.values.size();
        CheckAppend(total_num_rows_, n);
        validity_.resize(total_num_rows_ + n, true);
        for (size_t i = 0; i < n; ++i) {
            const auto coordinate = total_num_rows_ + i;
            const bool is_valid = !batch.validity || batch.validity[i];
            if (!is_valid) {
                validity_.reset(coordinate);
                continue;
            }
            AssertInfo(CompatibleArrayType(batch.values[i].get_element_type(),
                                           params_.value_type),
                       "ARRAY element type {} does not match bitmap type {}",
                       static_cast<int>(batch.values[i].get_element_type()),
                       static_cast<int>(params_.value_type));
            // All elements use the same row coordinate. Roaring de-duplicates
            // repeated values within the row. Empty rows remain valid.
            impl_->AddRow(batch.values[i], static_cast<uint32_t>(coordinate));
        }
        total_num_rows_ += n;
    }
    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "bitmap ARRAY index cannot build empty input");
    }
    return impl_->Finish(std::move(validity_), total_num_rows_, params_);
}

namespace {

template <typename T>
bool
RegisterBitmapBuilder() {
    BuilderRegistry<ScalarBuildInput<T>>::Instance().Register(
        families::kBitmap, [](const BuildParams& params) {
            return std::make_unique<BitmapIndexBuilder<T>>(
                ParseBuildParams(params, CppDataType<T>(), false));
        });
    return true;
}

bool
RegisterBitmapArrayBuilder() {
    BuilderRegistry<ScalarBuildInput<ArrayView>>::Instance().Register(
        families::kBitmap, [](const BuildParams& params) {
            auto parsed = ParseBuildParams(params, DataType::NONE, true);
            if (parsed.value_type == DataType::NONE ||
                parsed.value_type == DataType::ARRAY) {
                ThrowInfo(DataTypeInvalid,
                          "bitmap ARRAY builder requires array_element_type");
            }
            return std::make_unique<BitmapArrayIndexBuilder>(std::move(parsed));
        });
    return true;
}

const bool kRegistered =
    RegisterBitmapBuilder<bool>() && RegisterBitmapBuilder<int8_t>() &&
    RegisterBitmapBuilder<int16_t>() && RegisterBitmapBuilder<int32_t>() &&
    RegisterBitmapBuilder<int64_t>() && RegisterBitmapBuilder<float>() &&
    RegisterBitmapBuilder<double>() &&
    RegisterBitmapBuilder<std::string_view>() && RegisterBitmapArrayBuilder();

}  // namespace

#define INSTANTIATE_BITMAP_BUILDER(T) template class BitmapIndexBuilder<T>;
INSTANTIATE_BITMAP_BUILDER(bool)
INSTANTIATE_BITMAP_BUILDER(int8_t)
INSTANTIATE_BITMAP_BUILDER(int16_t)
INSTANTIATE_BITMAP_BUILDER(int32_t)
INSTANTIATE_BITMAP_BUILDER(int64_t)
INSTANTIATE_BITMAP_BUILDER(float)
INSTANTIATE_BITMAP_BUILDER(double)
INSTANTIATE_BITMAP_BUILDER(std::string_view)
#undef INSTANTIATE_BITMAP_BUILDER

}  // namespace milvus::index
