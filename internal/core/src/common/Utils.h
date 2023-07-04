// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <google/protobuf/text_format.h>
#include <simdjson.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>

#include <fcntl.h>
#include <sys/mman.h>

#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "config/ConfigChunkManager.h"
#include "exceptions/EasyAssert.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
#define FIELD_DATA(data_array, type) (data_array->scalars().type##_data().data())

#define VEC_FIELD_DATA(data_array, type) (data_array->vectors().type##_vector().data())

inline DatasetPtr
GenDataset(const int64_t nb, const int64_t dim, const void* xb) {
    return knowhere::GenDataset(nb, dim, xb);
}

inline const float*
GetDatasetDistance(const DatasetPtr& dataset) {
    return knowhere::GetDatasetDistance(dataset);
}

inline const int64_t*
GetDatasetIDs(const DatasetPtr& dataset) {
    return knowhere::GetDatasetIDs(dataset);
}

inline int64_t
GetDatasetRows(const DatasetPtr& dataset) {
    return knowhere::GetDatasetRows(dataset);
}

inline const void*
GetDatasetTensor(const DatasetPtr& dataset) {
    return knowhere::GetDatasetTensor(dataset);
}

inline int64_t
GetDatasetDim(const DatasetPtr& dataset) {
    return knowhere::GetDatasetDim(dataset);
}

inline bool
PrefixMatch(const std::string_view str, const std::string_view prefix) {
    auto ret = strncmp(str.data(), prefix.data(), prefix.length());
    if (ret != 0) {
        return false;
    }

    return true;
}

inline DatasetPtr
GenIdsDataset(const int64_t count, const int64_t* ids) {
    auto ret_ds = std::make_shared<knowhere::Dataset>();
    knowhere::SetDatasetRows(ret_ds, count);
    knowhere::SetDatasetDim(ret_ds, 1);
    // INPUT_IDS will not be free in dataset destructor, which is similar to `SetIsOwner(false)`.
    knowhere::SetDatasetInputIDs(ret_ds, ids);
    return ret_ds;
}

inline bool
PostfixMatch(const std::string_view str, const std::string_view postfix) {
    if (postfix.length() > str.length()) {
        return false;
    }

    int offset = str.length() - postfix.length();
    auto ret = strncmp(str.data() + offset, postfix.data(), postfix.length());
    if (ret != 0) {
        return false;
    }
    return true;
}

inline int64_t
upper_align(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = value / align + (value % align != 0);
    return groups * align;
}

inline int64_t
upper_div(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = value / align + (value % align != 0);
    return groups;
}

inline bool
IsMetricType(const std::string& str, const knowhere::MetricType& metric_type) {
    return !strcasecmp(str.c_str(), metric_type.c_str());
}

inline bool
PositivelyRelated(const knowhere::MetricType& metric_type) {
    return IsMetricType(metric_type, knowhere::metric::IP);
}

}  // namespace milvus
