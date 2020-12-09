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

#include <iostream>
#include <memory>
#include <string>

#include "db/metax/MetaResField.h"
#include "db/metax/MetaTraits.h"
#include "db/snapshot/Resources.h"
#include "utils/Status.h"

namespace milvus::engine::metax {

template <typename R, typename F>
void
extract_field_value(std::shared_ptr<R> res, F& field) {
    //    if constexpr(!is_decay_base_of_v<typename F::FType, R>) {
    //        return;
    //    } else
    if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::MappingsField, R> &&
                  is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::FlushableMappingsField, R>) {
        field.Set(res->GetFlushIds());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::MappingsField, R>) {
        field.Set(res->GetMappings());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::StateField, R>) {
        field.Set(res->GetState());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::LsnField, R>) {
        field.Set(res->GetLsn());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::CreatedOnField, R>) {
        field.Set(res->GetCreatedTime());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::UpdatedOnField, R>) {
        field.Set(res->GetUpdatedTime());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::IdField, R>) {
        field.Set(res->GetID());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::CollectionIdField, R>) {
        field.Set(res->GetCollectionId());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::SchemaIdField, R>) {
        field.Set(res->GetSchemaId());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::NumField, R>) {
        field.Set(res->GetNum());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::FtypeField, R>) {
        field.Set(res->GetFtype());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::FEtypeField, R>) {
        field.Set(res->GetFEtype());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::FieldIdField, R>) {
        field.Set(res->GetFieldId());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::FieldElementIdField, R>) {
        field.Set(res->GetFieldElementId());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::PartitionIdField, R>) {
        field.Set(res->GetPartitionId());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::SegmentIdField, R>) {
        field.Set(res->GetSegmentId());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::TypeNameField, R>) {
        field.Set(res->GetTypeName());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::NameField, R>) {
        field.Set(res->GetName());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::ParamsField, R>) {
        field.Set(res->GetParams());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::SizeField, R>) {
        field.Set(res->GetSize());
        return;
    } else if constexpr (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::RowCountField, R>) {
        field.Set(res->GetRowCount());
        return;
    } else {
        static_assert(!is_decay_base_of_v<typename F::FType, R>, "Unknown Template class F::FType");
        return;
    }
}

template <typename R>
MetaResFieldTuple
GenFieldTupleFromRes(typename R::Ptr res) {
    MetaResFieldTuple fields;
    std::apply([&res, &fields](auto&... field) { ((extract_field_value(res, field)), ...); }, fields);
    std::apply([&res, &fields](auto&... field) { ((field.SetTable(R::Name)), ...); }, fields);

    return fields;
}

}  // namespace milvus::engine::metax
