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

#include "db/meta/MetaFieldHelper.h"
#include "db/meta/MetaFieldValueHelper.h"
#include "utils/Status.h"

namespace milvus::engine::meta {

template <typename F>
class MetaFieldGS {
 protected:
    using v_type = typename MetaFieldIntegralHelper<F>::value_type;
    using f_type = remove_cr_t<F>;
    static const constexpr char* name = F::Name;

 public:
    MetaFieldGS() = default;

    explicit MetaFieldGS(const std::string& table) : table_(table) {
    }

    ~MetaFieldGS() = default;

    Status
    SetTable(const std::string& table) {
        if (table_.empty()) {
            table_ = table;
            return Status::OK();
        }
        return Status(DB_ERROR, "Table has been set in Meta Field");
    }

    [[nodiscard]] std::string
    Table() const {
        return table_;
    }

    v_type
    Get() const {
        return value_;
    }

    [[nodiscard]] std::string
    Get2Str() const {
        return FieldValue2Str<v_type>(value_);
    }

    template <typename R>
    void
    Get2Res(typename R::Ptr res) {
        static_assert(is_decay_base_of_v<F, R>, "Template class F is not base of R");
        // TODO(yhz): may tell invoker there is some incorrect
        if constexpr (decay_equal_v<F, snapshot::MappingsField>) {
            if constexpr (decay_equal_v<F, snapshot::FlushableMappingsField>) {
                res->GetFlushIds() = value_;
                return;
            } else {
                res->GetMappings() = value_;
                return;
            }
        } else if constexpr (decay_equal_v<F, snapshot::StateField>) {
            res->ResetStatus();
            switch (value_) {
                case snapshot::PENDING: {
                    return;
                }
                case snapshot::ACTIVE: {
                    res->Activate();
                    return;
                }
                case snapshot::DEACTIVE: {
                    res->Deactivate();
                    return;
                }
                default: { return; }
            }
        } else if constexpr (decay_equal_v<F, snapshot::LsnField>) {
            res->SetLsn(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::CreatedOnField>) {
            res->SetCreatedTime(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::UpdatedOnField>) {
            res->SetUpdatedTime(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::IdField>) {
            res->SetID(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::CollectionIdField>) {
            res->SetCollectionId(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::SchemaIdField>) {
            res->SetSchemaId(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::NumField>) {
            res->SetNum(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::FtypeField>) {
            res->SetFtype(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::FEtypeField>) {
            res->SetFEtype(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::FieldIdField>) {
            res->SetFieldId(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::FieldElementIdField>) {
            res->SetFieldElementId(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::PartitionIdField>) {
            res->SetPartitionId(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::SegmentIdField>) {
            res->SetSegmentId(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::TypeNameField>) {
            res->SetTypeName(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::NameField>) {
            res->SetName(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::ParamsField>) {
            res->SetParams(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::SizeField>) {
            res->SetSize(value_);
            return;
        } else if constexpr (decay_equal_v<F, snapshot::RowCountField>) {
            res->SetRowCount(value_);
            return;
        } else {
            static_assert(!is_decay_base_of_v<F, R>, "Unknown Template class F");
            return;
        }
    }

    void
    Set(v_type v) {
        value_ = v;
    }

    void
    SetFromStr(std::string v) {
        value_ = Str2FieldValue<v_type>(v);
    }

    template <typename R>
    void
    SetFromRes(typename R::Ptr res) {
        static_assert(is_decay_base_of_v<F, R>, "Template class F is not base of R");
        // TODO(yhz): may tell invoker there is some incorrect
        if constexpr (decay_equal_v<F, snapshot::MappingsField>) {
            if constexpr (decay_equal_v<F, snapshot::FlushableMappingsField>) {
                value_ = res->GetFlushIds();
                return;
            } else {
                value_ = res->GetMappings();
                return;
            }
        } else if constexpr (decay_equal_v<F, snapshot::StateField>) {
            value_ = res->GetState();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::LsnField>) {
            value_ = res->GetLsn();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::CreatedOnField>) {
            value_ = res->GetCreatedTime();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::UpdatedOnField>) {
            value_ = res->GetUpdatedTime();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::IdField>) {
            value_ = res->GetID();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::CollectionIdField>) {
            value_ = res->GetCollectionId();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::SchemaIdField>) {
            value_ = res->GetSchemaId();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::NumField>) {
            value_ = res->GetNum();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::FtypeField>) {
            value_ = res->GetFtype();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::FEtypeField>) {
            value_ = res->GetFEtype();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::FieldIdField>) {
            value_ = res->GetFieldId();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::FieldElementIdField>) {
            value_ = res->GetFieldElementId();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::PartitionIdField>) {
            value_ = res->GetPartitionId();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::SegmentIdField>) {
            value_ = res->GetSegmentId();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::TypeNameField>) {
            value_ = res->GetTypeName();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::NameField>) {
            value_ = res->GetName();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::ParamsField>) {
            value_ = res->GetParams();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::SizeField>) {
            value_ = res->GetSize();
            return;
        } else if constexpr (decay_equal_v<F, snapshot::RowCountField>) {
            value_ = res->GetRowCount();
            return;
        } else {
            static_assert(!is_decay_base_of_v<F, R>, "Unknown Template class F");
            return;
        }
    }

 private:
    std::string table_;
    v_type value_;
};

}  // namespace milvus::engine::meta
