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
#include <vector>

#include "arrow/array.h"
#include "arrow/table.h"
#include "log/Log.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/manifest.h"
#include "milvus-storage/reader.h"
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

        int num_cols = table->num_columns();
        out->sizes = static_cast<CFieldMemSize*>(
            malloc(sizeof(CFieldMemSize) * num_cols));
        out->count = num_cols;

        for (int i = 0; i < num_cols; i++) {
            int64_t col_bytes = 0;
            auto chunked = table->column(i);
            for (int c = 0; c < chunked->num_chunks(); c++) {
                col_bytes += calcArrayDataSize(chunked->chunk(c)->data());
            }

            auto name = table->field(i)->name();
            char* name_copy = static_cast<char*>(malloc(name.size() + 1));
            std::memcpy(name_copy, name.c_str(), name.size() + 1);

            out->sizes[i].field_name = name_copy;
            out->sizes[i].avg_mem_bytes = col_bytes / num_rows;
        }

        LOG_INFO(
            "SampleExternalSegmentFieldSizes: sampled {} rows, {} "
            "columns from manifest {}",
            num_rows,
            num_cols,
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
