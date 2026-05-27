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

#include "segcore/external_utils_c.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/table.h"
#include "common/FieldMeta.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/manifest.h"
#include "milvus-storage/reader.h"
#include "pb/schema.pb.h"
#include "storage/Util.h"
#include "storage/loon_ffi/util.h"

static CStatus
MakeCStatusError(const char* msg) {
    CStatus s;
    s.error_code = milvus::UnexpectedError;
    s.error_msg = strdup(msg);
    return s;
}

CStatus
SampleExternalSegmentFieldSizes(const char* manifest_path,
                                int sample_rows,
                                int64_t collection_id,
                                const LoonProperties* c_properties,
                                CProto collection_schema,
                                CFieldMemSizeList* out) {
    try {
        if (manifest_path == nullptr || manifest_path[0] == '\0') {
            return MakeCStatusError("manifest_path is empty");
        }
        if (c_properties == nullptr) {
            return MakeCStatusError("properties is null");
        }

        // 1. Convert C properties to C++ Properties
        //    Properties are fully constructed by the Go caller (base config +
        //    extfs overrides), matching the pattern used by explore/manifest.
        auto properties = std::make_shared<milvus_storage::api::Properties>();
        for (size_t i = 0; i < c_properties->count; i++) {
            const auto& prop = c_properties->properties[i];
            if (prop.key != nullptr && prop.value != nullptr) {
                milvus_storage::api::SetValue(
                    *properties, prop.key, prop.value, true);
            }
        }

        // 2. Read manifest → ColumnGroups
        auto loon_manifest =
            GetLoonManifest(std::string(manifest_path), properties);
        auto cgs = std::make_shared<milvus_storage::api::ColumnGroups>(
            loon_manifest->columnGroups());

        // 3. Determine total rows from column group files
        int64_t total_rows = 0;
        for (const auto& cg : *cgs) {
            for (const auto& f : cg->files) {
                total_rows = std::max(total_rows, f.end_index);
            }
        }
        int64_t actual =
            std::min(static_cast<int64_t>(sample_rows), total_rows);
        if (actual <= 0) {
            return MakeCStatusError("no rows available for sampling");
        }

        // 4. Create schemaless Reader (nullptr schema → types from file)
        auto reader = milvus_storage::api::Reader::create(
            cgs, nullptr, nullptr, *properties);

        // 5. Take sample rows [0, 1, ..., actual-1]
        std::vector<int64_t> indices(actual);
        std::iota(indices.begin(), indices.end(), 0);

        auto result = reader->take(indices, 1);
        if (!result.ok()) {
            return MakeCStatusError(result.status().ToString().c_str());
        }
        auto table = result.ValueOrDie();
        auto num_rows = table->num_rows();
        if (num_rows == 0) {
            return MakeCStatusError("sample returned 0 rows");
        }

        std::vector<milvus::FieldMeta> external_fields;
        if (collection_schema.proto_blob != nullptr &&
            collection_schema.proto_size > 0) {
            milvus::proto::schema::CollectionSchema schema;
            auto ok = schema.ParseFromArray(collection_schema.proto_blob,
                                            collection_schema.proto_size);
            if (!ok) {
                return MakeCStatusError("failed to parse collection schema");
            }
            external_fields.reserve(schema.fields_size());
            for (const auto& field_schema : schema.fields()) {
                if (field_schema.external_field().empty()) {
                    continue;
                }
                external_fields.emplace_back(
                    milvus::FieldMeta::ParseFrom(field_schema));
            }
        }

        // 6. Calculate per-column Arrow buffer size (recursive for nested types)
        std::function<int64_t(const std::shared_ptr<arrow::ArrayData>&)>
            calcArrayDataSize =
                [&](const std::shared_ptr<arrow::ArrayData>& data) -> int64_t {
            int64_t total = 0;
            for (const auto& buf : data->buffers) {
                if (buf) {
                    total += buf->size();
                }
            }
            for (const auto& child : data->child_data) {
                total += calcArrayDataSize(child);
            }
            return total;
        };

        auto fill_size = [](CFieldMemSize& out_size,
                            const std::string& name,
                            int64_t avg_mem_bytes) {
            char* name_copy = static_cast<char*>(malloc(name.size() + 1));
            std::memcpy(name_copy, name.c_str(), name.size() + 1);
            out_size.field_name = name_copy;
            out_size.avg_mem_bytes = avg_mem_bytes;
        };

        std::vector<std::pair<std::string, int64_t>> sampled_sizes;
        if (!external_fields.empty()) {
            sampled_sizes.reserve(external_fields.size());
            for (const auto& field_meta : external_fields) {
                const auto& column_name = field_meta.get_external_field();
                auto chunked = table->GetColumnByName(column_name);
                if (chunked == nullptr) {
                    return MakeCStatusError(
                        fmt::format("Column '{}' not found in schema",
                                    column_name)
                            .c_str());
                }

                int64_t col_bytes = 0;
                for (int c = 0; c < chunked->num_chunks(); c++) {
                    auto chunk = milvus::storage::NormalizeExternalArrow(
                        chunked->chunk(c), field_meta);
                    col_bytes += calcArrayDataSize(chunk->data());
                }
                sampled_sizes.emplace_back(column_name, col_bytes / num_rows);
            }
        } else {
            int num_cols = table->num_columns();
            sampled_sizes.reserve(num_cols);
            for (int i = 0; i < num_cols; i++) {
                int64_t col_bytes = 0;
                auto chunked = table->column(i);
                for (int c = 0; c < chunked->num_chunks(); c++) {
                    auto chunk = milvus::storage::CanonicalizeArrowVariants(
                        chunked->chunk(c));
                    col_bytes += calcArrayDataSize(chunk->data());
                }

                sampled_sizes.emplace_back(table->field(i)->name(),
                                           col_bytes / num_rows);
            }
        }

        out->sizes = static_cast<CFieldMemSize*>(
            malloc(sizeof(CFieldMemSize) * sampled_sizes.size()));
        out->count = static_cast<int>(sampled_sizes.size());
        for (size_t i = 0; i < sampled_sizes.size(); i++) {
            fill_size(
                out->sizes[i], sampled_sizes[i].first, sampled_sizes[i].second);
        }

        LOG_INFO(
            "SampleExternalSegmentFieldSizes: sampled {} rows, {} "
            "columns from manifest {}",
            num_rows,
            sampled_sizes.size(),
            manifest_path);

        CStatus ok;
        ok.error_code = milvus::Success;
        ok.error_msg = nullptr;
        return ok;
    } catch (const std::exception& e) {
        return MakeCStatusError(e.what());
    }
}

void
FreeCFieldMemSizeList(CFieldMemSizeList* list) {
    if (list != nullptr && list->sizes != nullptr) {
        for (int i = 0; i < list->count; i++) {
            free(const_cast<char*>(list->sizes[i].field_name));
        }
        free(list->sizes);
        list->sizes = nullptr;
        list->count = 0;
    }
}
