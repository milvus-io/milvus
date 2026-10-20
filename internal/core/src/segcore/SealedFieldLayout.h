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
#include <unordered_map>
#include <utility>

#include "common/Schema.h"
#include "segcore/LoadScope.h"
#include "common/IndexMeta.h"

namespace milvus::segcore {

// Immutable projection of an owned schema and a View's load scope. FieldMeta
// values remain in Schema; published reads retain this owner, not copied fields.
class SealedFieldLayout {
 public:
    SealedFieldLayout(std::shared_ptr<const Schema> schema,
                      LoadScope scope,
                      IndexMetaPtr indexes = nullptr)
        : schema_(std::move(schema)),
          index_meta_(std::move(indexes)),
          scope_(scope.WithDependencies(*schema_)) {
        std::unordered_set<std::string> external_columns;
        external_columns_ = std::make_shared<std::vector<std::string>>();
        for (auto id : schema_->get_field_ids()) {
            const auto& meta = (*schema_)[id];
            auto name = schema_->GetPhysicalColumnName(id);
            const bool vortex = meta.get_local_format() == LOCAL_FORMAT_VORTEX;
            auto [it, inserted] = vortex_columns_.emplace(name, vortex);
            if (!inserted) {
                it->second = it->second && vortex;
            }
            if (scope_.IsExplicit() && !scope_.Requires(id)) {
                continue;
            }
            field_ids_.push_back(id);
            if (schema_->IsExternalManifestStoredField(id) &&
                external_columns.emplace(name).second) {
                external_columns_->push_back(std::move(name));
            }
        }
    }

    // Own the exact metadata, independently of the C API handle and the View.
    // The parsed Schema is shared across bindings; index selection is not.
    const IndexMetaPtr&
    index_meta() const {
        return index_meta_;
    }
    // Legacy native constructors can supply index metadata independently.
    std::shared_ptr<const SealedFieldLayout>
    WithIndexMeta(IndexMetaPtr indexes) const {
        return std::make_shared<const SealedFieldLayout>(
            schema_, scope_, std::move(indexes));
    }
    bool
    Matches(const SchemaPtr& schema,
            const IndexMetaPtr& indexes,
            const LoadScope& scope) const {
        return schema_.get() == schema.get() && index_meta_ == indexes &&
               scope_.SameSelection(scope);
    }

    std::optional<FieldId>
    GetStructFieldId(const std::string& struct_name) const {
        return schema_->GetStructFieldId(struct_name);
    }

    const FieldMeta&
    operator[](FieldId id) const {
        AssertInfo(
            has_field(id), "field {} is outside sealed layout", id.get());
        return (*schema_)[id];
    }

    bool
    HasExplicitLoadScope() const {
        return scope_.IsExplicit();
    }
    bool
    AllowsLogicalField(FieldId id) const {
        return scope_.Contains(id);
    }
    bool
    IsFieldInLoadScope(FieldId id) const {
        return scope_.Requires(id);
    }
    bool
    ShouldPreload(FieldId id) const {
        return scope_.ShouldPreload(id);
    }
    bool
    ShouldLoadField(FieldId id) const {
        return scope_.ShouldPreload(id) && (id.get() < START_USER_FIELDID ||
                                            schema_->ShouldLoadRawField(id));
    }
    bool
    is_external_collection() const {
        return schema_->is_external_collection();
    }
    bool
    is_function_output(FieldId id) const {
        return has_field(id) && schema_->is_function_output(id);
    }
    std::pair<bool, bool>
    MmapEnabled(FieldId id) const {
        return schema_->MmapEnabled(has_field(id) ? id : FieldId(-1));
    }
    std::pair<bool, std::string>
    WarmupPolicy(FieldId id, bool vector, bool index) const {
        if (!scope_.ShouldPreload(id)) {
            return {true, "disable"};
        }
        return has_field(id) ? schema_->WarmupPolicy(id, vector, index)
                             : schema_->CollectionWarmupPolicy(vector, index);
    }
    bool
    has_field(FieldId id) const {
        return schema_->has_field(id) &&
               (!scope_.IsExplicit() || scope_.Requires(id));
    }
    const auto&
    get_field_ids() const {
        return field_ids_;
    }
    std::unordered_map<FieldId, FieldMeta>
    get_field_metas(const std::vector<FieldId>& ids) const {
        std::unordered_map<FieldId, FieldMeta> out;
        for (auto id : ids) {
            out.emplace(id, (*this)[id]);
        }
        return out;
    }
    std::optional<FieldId>
    get_primary_field_id() const {
        return schema_->get_primary_field_id();
    }
    std::optional<FieldId>
    get_dynamic_field_id() const {
        return schema_->get_dynamic_field_id();
    }
    size_t
    get_field_id_bitset_size() const {
        size_t size = 0;
        for (auto id : field_ids_) {
            if (id.get() >= START_USER_FIELDID) {
                size =
                    std::max(size, size_t(id.get() - START_USER_FIELDID + 1));
            }
        }
        return size;
    }
    uint64_t
    get_schema_version() const {
        return schema_->get_schema_version();
    }
    const std::string&
    get_external_source() const {
        return schema_->get_external_source();
    }
    const std::string&
    get_external_spec() const {
        return schema_->get_external_spec();
    }
    bool
    IsExternalDataField(FieldId id) const {
        return has_field(id) && schema_->IsExternalDataField(id);
    }
    bool
    IsExternalManifestStoredField(FieldId id) const {
        return (has_field(id) || id == TimestampFieldID) &&
               schema_->IsExternalManifestStoredField(id);
    }
    bool
    RequiresSourceInsertTimestamps() const {
        return schema_->RequiresSourceInsertTimestamps();
    }
    std::string
    get_storage_column_name(FieldId id) const {
        return schema_->GetPhysicalColumnName(id);
    }
    std::vector<FieldId>
    ResolveColumnFieldIds(const std::string& name) const {
        return schema_->ResolveColumnFieldIds(name);
    }
    bool
    IsVortexColumn(const std::string& name) const {
        auto it = vortex_columns_.find(name);
        return it != vortex_columns_.end() && it->second;
    }
    std::shared_ptr<std::vector<std::string>>
    GetExternalColumnNames() const {
        return external_columns_;
    }
    ArrowSchemaPtr
    ConvertToArrowSchema() const {
        return BuildFieldArrowSchema(schema_->get_fields(), field_ids_, false);
    }
    ArrowSchemaPtr
    ConvertToLoonArrowSchema(bool lob = false) const {
        if (lob) {
            auto cached = std::atomic_load(&loon_lob_schema_);
            if (cached) {
                return cached;
            }
        }
        auto result =
            BuildFieldArrowSchema(schema_->get_fields(), field_ids_, true, lob);
        if (lob) {
            std::atomic_store(&loon_lob_schema_, result);
        }
        return result;
    }
    std::pair<bool, std::string>
    CollectionWarmupPolicy(bool vector, bool index) const {
        return schema_->CollectionWarmupPolicy(vector, index);
    }
    // Legacy native callers without an explicit query plan need a projection.
    SchemaPtr
    MakePhysicalSchema() const {
        auto schema = std::make_shared<Schema>();
        for (auto id : field_ids_) {
            schema->AddField(FieldMeta((*schema_)[id]));
        }
        if (auto primary = get_primary_field_id()) {
            schema->set_primary_field_id(*primary);
        }
        if (auto dynamic = get_dynamic_field_id()) {
            schema->set_dynamic_field_id(*dynamic);
        }
        std::vector<int64_t> fields;
        for (auto id : field_ids_) {
            if (scope_.Requires(id)) {
                fields.push_back(id.get());
            }
        }
        schema->UpdateLoadFields(fields);
        schema->set_schema_version(get_schema_version());
        schema->set_external_source(get_external_source());
        schema->set_external_spec(get_external_spec());
        return schema;
    }

 private:
    std::shared_ptr<const Schema> schema_;
    IndexMetaPtr index_meta_;
    LoadScope scope_;
    std::vector<FieldId> field_ids_;
    std::unordered_map<std::string, bool> vortex_columns_;
    std::shared_ptr<std::vector<std::string>> external_columns_;
    mutable ArrowSchemaPtr loon_lob_schema_;
};

using SealedFieldLayoutPtr = std::shared_ptr<const SealedFieldLayout>;

}  // namespace milvus::segcore
