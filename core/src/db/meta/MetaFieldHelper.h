// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <string>
#include <tuple>
#include <unordered_set>

#include "db/meta/MetaTraits.h"
#include "db/snapshot/Resources.h"

namespace milvus::engine::meta {

template <typename F>
class MetaFieldIntegralHelper {
 public:
    using type = remove_cr_t<F>;
    using value_type = typename F::ValueType;
    static const constexpr char * name = F::Name;

 public:
    ~MetaFieldIntegralHelper() = default;
};

using MetaFieldIntegralHelperTuple =
    std::tuple<MetaFieldIntegralHelper<snapshot::MappingsField>,        MetaFieldIntegralHelper<snapshot::StateField>,
               MetaFieldIntegralHelper<snapshot::LsnField>,             MetaFieldIntegralHelper<snapshot::CreatedOnField>,
               MetaFieldIntegralHelper<snapshot::UpdatedOnField>,       MetaFieldIntegralHelper<snapshot::IdField>,
               MetaFieldIntegralHelper<snapshot::CollectionIdField>,    MetaFieldIntegralHelper<snapshot::SchemaIdField>,
               MetaFieldIntegralHelper<snapshot::NumField>,             MetaFieldIntegralHelper<snapshot::FtypeField>,
               MetaFieldIntegralHelper<snapshot::FEtypeField>,          MetaFieldIntegralHelper<snapshot::FieldIdField>,
               MetaFieldIntegralHelper<snapshot::FieldElementIdField>,  MetaFieldIntegralHelper<snapshot::PartitionIdField>,
               MetaFieldIntegralHelper<snapshot::SegmentIdField>,       MetaFieldIntegralHelper<snapshot::TypeNameField>,
               MetaFieldIntegralHelper<snapshot::NameField>,            MetaFieldIntegralHelper<snapshot::ParamsField>,
               MetaFieldIntegralHelper<snapshot::SizeField>,            MetaFieldIntegralHelper<snapshot::RowCountField>
               >;

template <typename R, typename H>
inline void
extract_field_name(R& res, H& helper, std::unordered_set<std::string>& names) {
    if constexpr(std::is_base_of_v<typename H::type, R>) {
        names.insert(std::string(H::name));
        return;
    } else {
        return;
    }
}

template <typename R>
inline std::unordered_set<std::string>
GetResFieldNames(typename R::Ptr res) {
    MetaFieldIntegralHelperTuple helpers;
    std::unordered_set<std::string> names;
    std::apply([&res, &helpers, &names](auto&... fh){((extract_field_name(*res.get(), fh, names)), ...);}, helpers);
    return names;
}

//////////////////////////////////////////////////////
template <typename F>
class MetaFieldSelectHelper {
 public:
    using type = remove_cr_t<F>;
    using value_type = typename F::ValueType;
    static const constexpr char * name = F::Name;

 public:
    explicit MetaFieldSelectHelper(const std::string& table) : table_(table) {
    }

    ~MetaFieldSelectHelper() = default;

    [[nodiscard]] std::string
    Table() const {
        return table_;
    }

 private:
    std::string table_;
};

}  // namespace milvus::engine::meta
