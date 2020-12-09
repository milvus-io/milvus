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
#include <utility>

#include "db/metax/MetaTraits.h"
#include "db/snapshot/Resources.h"
#include "utils/Status.h"

namespace milvus::engine::metax {

template <typename F>
class MetaResField {
 public:
    using Type = MetaResField<F>;
    using FType = remove_cr_t<F>;
    using VType = typename F::ValueType;
    static constexpr const char* Name = F::Name;

 public:
    MetaResField() : table_(""), value_(VType{}), filled_(false) {
    }

    explicit MetaResField(std::string table) : table_(std::move(table)) {
    }

    [[nodiscard]] std::string
    Table() const {
        return table_;
    }

    void
    SetTable(std::string table) {
        table_ = std::move(table);
    }

    [[nodiscard]] bool
    Filled() const {
        return filled_;
    }

    VType
    Get() const {
        return value_;
    }

    void
    Set(VType value) {
        value_ = value;
        filled_ = true;
    }

 private:
    std::string table_;
    VType value_;
    bool filled_ = false;
};

using MetaResFieldTuple = std::tuple<
    MetaResField<snapshot::MappingsField>, MetaResField<snapshot::StateField>, MetaResField<snapshot::LsnField>,
    MetaResField<snapshot::CreatedOnField>, MetaResField<snapshot::UpdatedOnField>, MetaResField<snapshot::IdField>,
    MetaResField<snapshot::CollectionIdField>, MetaResField<snapshot::SchemaIdField>, MetaResField<snapshot::NumField>,
    MetaResField<snapshot::FtypeField>, MetaResField<snapshot::FEtypeField>, MetaResField<snapshot::FieldIdField>,
    MetaResField<snapshot::FieldElementIdField>, MetaResField<snapshot::PartitionIdField>,
    MetaResField<snapshot::SegmentIdField>, MetaResField<snapshot::TypeNameField>, MetaResField<snapshot::NameField>,
    MetaResField<snapshot::ParamsField>, MetaResField<snapshot::SizeField>, MetaResField<snapshot::RowCountField> >;

}  // namespace milvus::engine::metax
