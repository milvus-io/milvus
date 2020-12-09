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
#include <string>

#include "db/metax/MetaTraits.h"
#include "db/metax/MetaResField.h"
#include "db/snapshot/Resources.h"
#include "utils/Status.h"

namespace milvus::engine::metax {

template <typename R, typename F>
void
extract_field_value(std::shared_ptr<R> res, F& field) {
//    if constexpr(!is_decay_base_of_v<typename F::FType, R>) {
//        return;
//    } else
    if constexpr(is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::MappingsField, R> &&
                 is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::FlushableMappingsField, R>) {
        field.Set(res->GetFlushIds());
        return;
    } else if (is_decay_base_of_and_equal_of_v<typename F::FType, snapshot::MappingsField, R>) {
        field.Set(res->GetMappings());
        return;
    } else {
        return;
    }
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::StateField>) {
//        field.Set(res->GetState());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::LsnField>) {
//        field.Set(res->GetLsn());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::CreatedOnField>) {
//        field.Set(res->GetCreatedTime());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::UpdatedOnField>) {
//        field.Set(res->GetUpdatedTime());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::IdField>) {
//        field.Set(res->GetID());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::CollectionIdField>) {
//        field.Set(res->GetCollectionId());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::SchemaIdField>) {
//        field.Set(res->GetSchemaId());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::NumField>) {
//        field.Set(res->GetNum());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::FtypeField>) {
//        field.Set(res->GetFtype());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::FEtypeField>) {
//        field.Set(res->GetFEtype());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::FieldIdField>) {
//        field.Set(res->GetFieldId());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::FieldElementIdField>) {
//        field.Set(res->GetFieldElementId());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::PartitionIdField>) {
//        field.Set(res->GetPartitionId());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::SegmentIdField>) {
//        field.Set(res->GetSegmentId());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::TypeNameField>) {
//        field.Set(res->GetTypeName());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::NameField>) {
//        field.Set(res->GetName());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::ParamsField>) {
//        field.Set(res->GetParams());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::SizeField>) {
//        field.Set(res->GetSize());
//        return;
//    } else if constexpr(decay_equal_v<typename F::FType, snapshot::RowCountField>) {
//        field.Set(res->GetRowCount());
//        return;
//    } else {
//        static_assert(!is_decay_base_of_v<typename F::FType, R>, "Unknown Template class F::FType");
//        return;
//    }
}

template <typename R>
MetaResFieldTuple
GenFieldTupleFromRes(typename R::Ptr res) {
    MetaResFieldTuple fields;
    std::apply([&res, &fields](auto&... field){((extract_field_value(res, field)), ...);}, fields);
}

}  // namespace milvus::engine::metax
