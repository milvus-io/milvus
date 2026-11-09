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

#include <algorithm>
#include <any>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>

#include "nlohmann/json.hpp"

#include "common/Array.h"
#include "common/CustomBitset.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/scalar/ngram/JsonProjectedString.h"

namespace milvus::index::test {

enum class BackendInputShape {
    Scalar,
    ArrayRows,
    NestedElements,
    SpatialWkb,
    JsonDocument,
    JsonProjected,
};

struct OwnedArrayValue {
    template <typename T>
    static OwnedArrayValue
    FromFixed(DataType element_type, std::span<const T> values) {
        static_assert(std::is_trivially_copyable_v<T>);
        OwnedArrayValue result;
        result.element_type = element_type;
        result.length = static_cast<int>(values.size());
        result.byte_size = values.size_bytes();
        result.data.resize(std::max<size_t>(result.byte_size, 1));
        if (!values.empty()) {
            std::memcpy(result.data.data(), values.data(), result.byte_size);
        }
        return result;
    }

    static OwnedArrayValue
    FromStrings(DataType element_type, const std::vector<std::string>& values) {
        OwnedArrayValue result;
        result.element_type = element_type;
        result.length = static_cast<int>(values.size());
        result.offsets.reserve(std::max<size_t>(values.size(), 1));
        for (const auto& value : values) {
            result.offsets.push_back(static_cast<uint32_t>(result.byte_size));
            result.byte_size += value.size();
        }
        if (result.offsets.empty()) {
            result.offsets.push_back(0);
        }
        result.data.resize(std::max<size_t>(result.byte_size, 1));
        size_t offset = 0;
        for (const auto& value : values) {
            if (!value.empty()) {
                std::memcpy(
                    result.data.data() + offset, value.data(), value.size());
            }
            offset += value.size();
        }
        return result;
    }

    ArrayView
    View() const {
        return ArrayView(
            const_cast<char*>(data.data()),
            length,
            byte_size,
            element_type,
            offsets.empty() ? nullptr : const_cast<uint32_t*>(offsets.data()));
    }

    std::vector<char> data;
    std::vector<uint32_t> offsets;
    int length{0};
    size_t byte_size{0};
    DataType element_type{DataType::NONE};
};

struct OwnedJsonProjectedString {
    std::string value;
    JsonProjectedStringState state{JsonProjectedStringState::NoValue};
};

template <typename T>
struct ScalarTestValueSelector {
    using type = T;
};

template <>
struct ScalarTestValueSelector<std::string_view> {
    using type = std::string;
};

template <>
struct ScalarTestValueSelector<ArrayView> {
    using type = OwnedArrayValue;
};

template <>
struct ScalarTestValueSelector<JsonProjectedString> {
    using type = OwnedJsonProjectedString;
};

// T is the builder input type. Test data and query expectations own all bytes.
template <typename T>
using ScalarTestValue = typename ScalarTestValueSelector<T>::type;

// The caller keeps the strings alive and unchanged while these views are used.
inline std::vector<std::string_view>
MakeStringViews(const std::vector<std::string>& values) {
    std::vector<std::string_view> views;
    views.reserve(values.size());
    for (const auto& value : values) {
        views.emplace_back(value);
    }
    return views;
}

// Shared input only. Queries and their expected results belong to each contract.
template <typename T>
struct ScalarTestData {
    explicit ScalarTestData(std::vector<ScalarTestValue<T>> data_values)
        : values(std::move(data_values)), validity(values.size(), true) {
    }

    std::vector<ScalarTestValue<T>> values;
    // One bit per row, initially all valid. Row count is values.size().
    CustomBitset validity;
    // false passes an empty ValidityView, which is distinct from a present
    // all-valid bitmap at the build contract.
    bool validity_present{true};
    // Empty means one batch containing every row. Otherwise sizes must sum to
    // values.size(); validity subviews retain their original packed bit offset.
    std::vector<size_t> batch_sizes;
    Domain domain{Domain::Row};
    Config metadata = Config::object();
};

template <typename T>
bool
ScalarTestHasNulls(const ScalarTestData<T>& data) {
    if constexpr (std::is_same_v<T, JsonProjectedString>) {
        return std::any_of(
            data.values.begin(), data.values.end(), [](const auto& value) {
                return value.state == JsonProjectedStringState::FieldNull;
            });
    }
    return data.validity.count() != data.values.size();
}

// Registration stores only small descriptors. Invoke make_data inside the test
// body, and capture only small parameters (e.g. row count/seed), never datasets.
template <typename T>
struct ScalarDataSet {
    std::string name;
    // True when this descriptor can generate at least one null row. Ordinary
    // reader cases require a backend that accepts nulls; an explicit negative
    // builder case may deliberately select a non-nullable backend instead.
    bool requires_nullable{false};
    BackendInputShape input_shape{BackendInputShape::Scalar};
    Domain domain{Domain::Row};
    std::optional<DataType> logical_value_type;
    std::function<ScalarTestData<T>()> make_data;
};

class DataCatalog {
 public:
    template <typename T>
    void
    Add(ScalarDataSet<T> dataset) {
        if (dataset.name.empty() || !dataset.make_data) {
            throw std::logic_error("dataset requires a name and generator");
        }
        const auto key =
            std::make_pair(std::type_index(typeid(T)), dataset.name);
        if (!datasets_.emplace(key, std::move(dataset)).second) {
            throw std::logic_error(key.second + ": duplicate dataset for type");
        }
    }

    template <typename T>
    const ScalarDataSet<T>&
    Get(std::string_view name) const {
        const auto it = datasets_.find(
            std::make_pair(std::type_index(typeid(T)), std::string(name)));
        if (it == datasets_.end()) {
            throw std::logic_error(std::string(name) +
                                   ": unknown dataset/type");
        }
        return std::any_cast<const ScalarDataSet<T>&>(it->second);
    }

 private:
    // Only typed descriptors, never generated rows, are stored here.
    std::map<std::pair<std::type_index, std::string>, std::any> datasets_;
};

const DataCatalog&
ScalarDataSets();

void
RegisterTextAndCandidateDataSets(DataCatalog& catalog);

void
RegisterJsonDataSets(DataCatalog& catalog);

// Bind views after generation is complete. Keep both data and this adapter alive
// and unchanged until Build returns; no spans are cached in ScalarTestData.
template <typename T>
class ScalarTestInput {
 public:
    explicit ScalarTestInput(const ScalarTestData<T>& data) {
        if (data.validity.size() != data.values.size()) {
            throw std::logic_error("test validity size must match values");
        }
        if (!data.validity_present &&
            data.validity.count() != data.values.size()) {
            throw std::logic_error(
                "absent test validity may represent only all-valid rows");
        }

        std::span<const T> values;
        if constexpr (std::is_same_v<T, std::string_view>) {
            string_views_ = MakeStringViews(data.values);
            values = string_views_;
        } else if constexpr (std::is_same_v<T, ArrayView>) {
            array_views_.reserve(data.values.size());
            for (const auto& value : data.values) {
                array_views_.push_back(value.View());
            }
            values = array_views_;
        } else if constexpr (std::is_same_v<T, JsonProjectedString>) {
            json_projected_views_.reserve(data.values.size());
            for (const auto& value : data.values) {
                json_projected_views_.push_back(
                    JsonProjectedString{value.value, value.state});
            }
            values = json_projected_views_;
        } else if constexpr (std::is_same_v<T, bool>) {
            bool_values_ = std::make_unique<bool[]>(data.values.size());
            for (size_t i = 0; i < data.values.size(); ++i) {
                bool_values_[i] = data.values[i];
            }
            values = {bool_values_.get(), data.values.size()};
        } else {
            values = data.values;
        }

        const auto validity =
            data.validity_present
                ? ValidityView::FromPacked(
                      reinterpret_cast<const uint8_t*>(data.validity.data()))
                : ValidityView{};
        if (data.batch_sizes.empty()) {
            batches_.push_back({values, validity});
            return;
        }

        size_t offset = 0;
        batches_.reserve(data.batch_sizes.size());
        for (const auto size : data.batch_sizes) {
            if (size > values.size() - offset) {
                throw std::logic_error(
                    "test batch sizes exceed the number of values");
            }
            batches_.push_back(
                {values.subspan(offset, size),
                 validity ? validity.Subview(offset) : ValidityView{}});
            offset += size;
        }
        if (offset != values.size()) {
            throw std::logic_error(
                "test batch sizes must cover every input value");
        }
    }

    ScalarTestInput(const ScalarTestInput&) = delete;
    ScalarTestInput&
    operator=(const ScalarTestInput&) = delete;

    ScalarBuildInput<T>
    View() const {
        return {batches_};
    }

 private:
    // Created only after the owning data reaches its final location.
    std::vector<std::string_view> string_views_;
    std::vector<ArrayView> array_views_;
    std::vector<JsonProjectedString> json_projected_views_;
    std::unique_ptr<bool[]> bool_values_;
    std::vector<ScalarBuildBatch<T>> batches_;
};

}  // namespace milvus::index::test
