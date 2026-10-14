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

#include <array>
#include <memory>
#include <unordered_map>
#include <utility>

#include "common/Schema.h"
#include "segcore/LoadScope.h"

namespace milvus::segcore {

// Independently owned physical field descriptions used by loading metadata.
// This object has no logical schema identity and borrows nothing from Schema.
// In particular it remains usable after the preparing collection is released.
class SealedFieldLayout {
 public:
    SealedFieldLayout(const Schema* schema, LoadScope scope)
        : scope_(scope.WithDependencies(*schema)),
          external_(schema->is_external_collection()) {
        source_version_ = schema->get_schema_version();
        milvus_table_ = schema->is_milvus_table_external_collection();
        primary_ = schema->get_primary_field_id();
        dynamic_ = schema->get_dynamic_field_id();
        external_source_ = schema->get_external_source();
        external_spec_ = schema->get_external_spec();
        source_insert_timestamps_ = schema->RequiresSourceInsertTimestamps();
        external_columns_ = std::make_shared<std::vector<std::string>>();
        mmap_default_ = schema->MmapEnabled(FieldId(-1));
        for (size_t i = 0; i != warmup_default_.size(); ++i) {
            warmup_default_[i] =
                schema->CollectionWarmupPolicy(i >= 2, i % 2 != 0);
        }
        for (auto id : schema->get_field_ids()) {
            const auto& meta = (*schema)[id];
            auto storage_name = schema->GetPhysicalColumnName(id);
            // Manifest discovery can encounter unselected columns. Retain
            // only their storage-name mapping, not their field descriptions.
            storage_names_.emplace(id, storage_name);
            if (meta.is_external_field()) {
                external_names_.emplace(meta.get_external_field(), id);
            }
            if (scope_.IsExplicit() && !scope_.Requires(id)) {
                continue;
            }
            fields_.emplace(id, meta);
            field_ids_.push_back(id);
            if (schema->IsExternalDataField(id)) {
                external_fields_.emplace(id);
            }
            if (schema->IsExternalManifestStoredField(id)) {
                manifest_fields_.emplace(id);
                external_columns_->push_back(storage_name);
            }
            mmap_.emplace(id, schema->MmapEnabled(id));
            auto& policies = warmup_[id];
            for (size_t i = 0; i != policies.size(); ++i) {
                policies[i] = schema->WarmupPolicy(id, i >= 2, i % 2 != 0);
            }
            if (schema->is_function_output(id)) {
                function_outputs_.emplace(id);
            }
            // Preserve the raw-column policy independently of scope. BM25
            // outputs may be available from indexes without eager raw data.
            if (schema->ShouldLoadRawField(id)) {
                eager_fields_.emplace(id);
            }
        }
    }

    const FieldMeta&
    operator[](FieldId id) const {
        return fields_.at(id);
    }

    const auto&
    get_fields() const {
        return fields_;
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
    ShouldLoadField(FieldId id) const {
        return scope_.Requires(id) &&
               (id.get() < START_USER_FIELDID || eager_fields_.count(id) != 0);
    }

    bool
    is_external_collection() const {
        return external_;
    }

    bool
    is_function_output(FieldId id) const {
        return function_outputs_.count(id) != 0;
    }

    std::pair<bool, bool>
    MmapEnabled(FieldId id) const {
        auto it = mmap_.find(id);
        return it == mmap_.end() ? mmap_default_ : it->second;
    }

    std::pair<bool, std::string>
    WarmupPolicy(FieldId id, bool is_vector, bool is_index) const {
        auto it = warmup_.find(id);
        auto index = (is_vector ? 2 : 0) + (is_index ? 1 : 0);
        return (it == warmup_.end() ? warmup_default_ : it->second)[index];
    }

    bool
    has_field(FieldId id) const {
        return fields_.count(id) != 0;
    }

    const auto&
    get_field_ids() const {
        return field_ids_;
    }

    std::unordered_map<FieldId, FieldMeta>
    get_field_metas(const std::vector<FieldId>& ids) const {
        std::unordered_map<FieldId, FieldMeta> out;
        for (auto id : ids) {
            out.emplace(id, fields_.at(id));
        }
        return out;
    }

    std::optional<FieldId>
    get_primary_field_id() const {
        return primary_;
    }

    std::optional<FieldId>
    get_dynamic_field_id() const {
        return dynamic_;
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

    // Input provenance for legacy reopen adapters, never a query-schema
    // selection or physical compatibility proof.
    uint64_t
    get_schema_version() const {
        return source_version_;
    }

    const std::string&
    get_external_source() const {
        return external_source_;
    }
    const std::string&
    get_external_spec() const {
        return external_spec_;
    }

    bool
    IsExternalDataField(FieldId id) const {
        return external_fields_.count(id) != 0;
    }
    bool
    IsExternalManifestStoredField(FieldId id) const {
        return manifest_fields_.count(id) != 0 ||
               (id == TimestampFieldID && source_insert_timestamps_);
    }
    bool
    RequiresSourceInsertTimestamps() const {
        return source_insert_timestamps_;
    }

    const std::string&
    get_storage_column_name(FieldId id) const {
        return storage_names_.at(id);
    }

    FieldId
    ResolveColumnFieldId(const std::string& name) const {
        auto parsed = ParseFieldIdColumnName(name);
        if (external_) {
            if (milvus_table_ && parsed && storage_names_.count(*parsed)) {
                return *parsed;
            }
            if (auto it = external_names_.find(name);
                it != external_names_.end()) {
                return it->second;
            }
        }
        if (parsed) {
            return *parsed;
        }
        ThrowInfo(DataFormatBroken, "invalid physical column name: {}", name);
    }

    std::shared_ptr<std::vector<std::string>>
    GetExternalColumnNames() const {
        return external_columns_;
    }
    ArrowSchemaPtr
    ConvertToArrowSchema() const {
        return BuildFieldArrowSchema(fields_, field_ids_, false);
    }
    ArrowSchemaPtr
    ConvertToLoonArrowSchema(bool lob = false) const {
        if (lob) {
            auto cached = std::atomic_load(&loon_lob_schema_);
            if (cached) {
                return cached;
            }
        }
        auto result = BuildFieldArrowSchema(fields_, field_ids_, true, lob);
        if (lob) {
            std::atomic_store(&loon_lob_schema_, result);
        }
        return result;
    }

    std::pair<bool, std::string>
    CollectionWarmupPolicy(bool vector, bool index) const {
        return warmup_default_[(vector ? 2 : 0) + (index ? 1 : 0)];
    }

    // Compatibility for native callers without a logical plan. QN queries
    // use their explicitly bound plan schema instead.
    SchemaPtr
    MakePhysicalSchema() const {
        auto schema = std::make_shared<Schema>();
        for (auto id : field_ids_) {
            schema->AddField(FieldMeta(fields_.at(id)));
        }
        if (primary_) {
            schema->set_primary_field_id(*primary_);
        }
        if (dynamic_) {
            schema->set_dynamic_field_id(*dynamic_);
        }
        std::vector<int64_t> fields;
        for (auto id : field_ids_) {
            if (scope_.Requires(id)) {
                fields.push_back(id.get());
            }
        }
        schema->UpdateLoadFields(fields);
        schema->set_schema_version(source_version_);
        schema->set_external_source(external_source_);
        schema->set_external_spec(external_spec_);
        return schema;
    }

 private:
    uint64_t source_version_;
    std::optional<FieldId> primary_;
    std::optional<FieldId> dynamic_;
    std::vector<FieldId> field_ids_;
    std::string external_source_;
    std::string external_spec_;
    bool source_insert_timestamps_;
    std::unordered_map<FieldId, std::string> storage_names_;
    std::unordered_map<std::string, FieldId> external_names_;
    std::unordered_set<FieldId> external_fields_;
    std::unordered_set<FieldId> manifest_fields_;
    mutable ArrowSchemaPtr loon_lob_schema_;
    std::shared_ptr<std::vector<std::string>> external_columns_;
    LoadScope scope_;
    bool external_;
    bool milvus_table_;
    std::unordered_map<FieldId, FieldMeta> fields_;
    std::unordered_map<FieldId, std::pair<bool, bool>> mmap_;
    std::pair<bool, bool> mmap_default_;
    using WarmupPolicies = std::array<std::pair<bool, std::string>, 4>;
    WarmupPolicies warmup_default_;
    std::unordered_map<FieldId, WarmupPolicies> warmup_;
    std::unordered_set<FieldId> function_outputs_;
    std::unordered_set<FieldId> eager_fields_;
};

using SealedFieldLayoutPtr = std::shared_ptr<const SealedFieldLayout>;

}  // namespace milvus::segcore
