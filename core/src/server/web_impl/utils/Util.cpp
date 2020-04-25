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

#include "server/web_impl/utils/Util.h"

#include "utils/ValidationUtil.h"

namespace milvus {
namespace server {
namespace web {

Status
CopyRowRecords(const OList<OList<OFloat32>::ObjectWrapper>::ObjectWrapper& records, std::vector<float>& vectors) {
    size_t tal_size = 0;
    records->forEach([&tal_size](const OList<OFloat32>::ObjectWrapper& row_item) { tal_size += row_item->count(); });

    vectors.resize(tal_size);
    size_t index_offset = 0;
    records->forEach([&vectors, &index_offset](const OList<OFloat32>::ObjectWrapper& row_item) {
        row_item->forEach(
            [&vectors, &index_offset](const OFloat32& item) { vectors[index_offset++] = item->getValue(); });
    });

    return Status::OK();
}

Status
CopyBinRowRecords(const OList<OList<OInt64>::ObjectWrapper>::ObjectWrapper& records, std::vector<uint8_t>& vectors) {
    size_t tal_size = 0;
    records->forEach([&tal_size](const OList<OInt64>::ObjectWrapper& item) { tal_size += item->count(); });

    vectors.resize(tal_size);
    size_t index_offset = 0;
    bool oor = false;
    records->forEach([&vectors, &index_offset, &oor](const OList<OInt64>::ObjectWrapper& row_item) {
        row_item->forEach([&vectors, &index_offset, &oor](const OInt64& item) {
            if (!oor) {
                int64_t value = item->getValue();
                if (0 > value || value > 255) {
                    oor = true;
                } else {
                    vectors[index_offset++] = static_cast<uint8_t>(value);
                }
            }
        });
    });

    return Status::OK();
}

Status
ParseQueryInteger(const OQueryParams& query_params, const std::string& key, int64_t& value, bool nullable) {
    auto query = query_params.get(key.c_str());
    if (nullptr != query.get() && query->getSize() > 0) {
        std::string value_str = query->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(value_str).ok()) {
            return Status(ILLEGAL_QUERY_PARAM,
                          "Query param \'offset\' is illegal, only non-negative integer supported");
        }

        value = std::stol(value_str);
    } else if (!nullable) {
        return Status(QUERY_PARAM_LOSS, "Query param \"" + key + "\" is required");
    }

    return Status::OK();
}

Status
ParseQueryStr(const OQueryParams& query_params, const std::string& key, std::string& value, bool nullable) {
    auto query = query_params.get(key.c_str());
    if (nullptr != query.get() && query->getSize() > 0) {
        value = query->std_str();
    } else if (!nullable) {
        return Status(QUERY_PARAM_LOSS, "Query param \"" + key + "\" is required");
    }

    return Status::OK();
}

Status
ParseQueryBool(const OQueryParams& query_params, const std::string& key, bool& value, bool nullable) {
    auto query = query_params.get(key.c_str());
    if (nullptr != query.get() && query->getSize() > 0) {
        std::string value_str = query->std_str();
        if (!ValidationUtil::ValidateStringIsBool(value_str).ok()) {
            return Status(ILLEGAL_QUERY_PARAM, "Query param \'all_required\' must be a bool");
        }
        value = value_str == "True" || value_str == "true";
        return Status::OK();
    }

    if (!nullable) {
        return Status(QUERY_PARAM_LOSS, "Query param \"" + key + "\" is required");
    }

    return Status::OK();
}

}  // namespace web
}  // namespace server
}  // namespace milvus
