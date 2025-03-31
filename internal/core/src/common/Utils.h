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

#include <fcntl.h>
#include <fmt/core.h>
#include <google/protobuf/text_format.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstring>
#include <cmath>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "common/EasyAssert.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "knowhere/sparse_utils.h"
#include "simdjson.h"

namespace milvus {
#define FIELD_DATA(data_array, type) \
    (data_array->scalars().type##_data().data())

#define VEC_FIELD_DATA(data_array, type) \
    (data_array->vectors().type##_vector().data())

using CheckDataValid = std::function<bool(size_t)>;

inline DatasetPtr
GenDataset(const int64_t nb, const int64_t dim, const void* xb) {
    return knowhere::GenDataSet(nb, dim, xb);
}

inline const float*
GetDatasetDistance(const DatasetPtr& dataset) {
    return dataset->GetDistance();
}

inline const int64_t*
GetDatasetIDs(const DatasetPtr& dataset) {
    return dataset->GetIds();
}

inline int64_t
GetDatasetRows(const DatasetPtr& dataset) {
    return dataset->GetRows();
}

inline const void*
GetDatasetTensor(const DatasetPtr& dataset) {
    return dataset->GetTensor();
}

inline int64_t
GetDatasetDim(const DatasetPtr& dataset) {
    return dataset->GetDim();
}

inline const size_t*
GetDatasetLims(const DatasetPtr& dataset) {
    return dataset->GetLims();
}

inline bool
PrefixMatch(const std::string_view str, const std::string_view prefix) {
    if (prefix.length() > str.length()) {
        return false;
    }
    auto ret = strncmp(str.data(), prefix.data(), prefix.length());
    if (ret != 0) {
        return false;
    }

    return true;
}

inline DatasetPtr
GenIdsDataset(const int64_t count, const int64_t* ids) {
    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->SetRows(count);
    ret_ds->SetDim(1);
    ret_ds->SetIds(ids);
    ret_ds->SetIsOwner(false);
    return ret_ds;
}

inline DatasetPtr
GenResultDataset(const int64_t nq,
                 const int64_t topk,
                 const int64_t* ids,
                 const float* distance) {
    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->SetRows(nq);
    ret_ds->SetDim(topk);
    ret_ds->SetIds(ids);
    ret_ds->SetDistance(distance);
    ret_ds->SetIsOwner(true);
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
    //
    //    int i = postfix.length() - 1;
    //    int j = str.length() - 1;
    //    for (; i >= 0; i--, j--) {
    //        if (postfix[i] != str[j]) {
    //            return false;
    //        }
    //    }
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
IsMetricType(const std::string_view str,
             const knowhere::MetricType& metric_type) {
    return !strcasecmp(str.data(), metric_type.c_str());
}

inline bool
PositivelyRelated(const knowhere::MetricType& metric_type) {
    return IsMetricType(metric_type, knowhere::metric::IP) ||
           IsMetricType(metric_type, knowhere::metric::COSINE) ||
           IsMetricType(metric_type, knowhere::metric::BM25);
}

inline std::string
KnowhereStatusString(knowhere::Status status) {
    return knowhere::Status2String(status);
}

inline std::vector<IndexType>
DISK_INDEX_LIST() {
    static std::vector<IndexType> ret{
        knowhere::IndexEnum::INDEX_DISKANN,
    };
    return ret;
}

template <typename T>
inline bool
is_in_list(const T& t, std::function<std::vector<T>()> list_func) {
    auto l = list_func();
    return std::find(l.begin(), l.end(), t) != l.end();
}

inline bool
is_in_disk_list(const IndexType& index_type) {
    return is_in_list<IndexType>(index_type, DISK_INDEX_LIST);
}

template <typename T>
std::string
Join(const std::vector<T>& items, const std::string& delimiter) {
    std::stringstream ss;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i > 0) {
            ss << delimiter;
        }
        ss << items[i];
    }
    return ss.str();
}

inline std::string
PrintBitsetTypeView(const BitsetTypeView& view) {
    std::stringstream ss;
    for (auto i = 0; i < view.size(); ++i) {
        ss << int(view[i]);
    }
    return ss.str();
}

inline std::string
GetCommonPrefix(const std::string& str1, const std::string& str2) {
    size_t len = std::min(str1.length(), str2.length());
    size_t i = 0;
    while (i < len && str1[i] == str2[i]) ++i;
    return str1.substr(0, i);
}

inline knowhere::sparse::SparseRow<float>
CopyAndWrapSparseRow(const void* data,
                     size_t size,
                     const bool validate = false) {
    size_t num_elements =
        size / knowhere::sparse::SparseRow<float>::element_size();
    knowhere::sparse::SparseRow<float> row(num_elements);
    std::memcpy(row.data(), data, size);
    if (validate) {
        AssertInfo(
            size % knowhere::sparse::SparseRow<float>::element_size() == 0,
            "Invalid size for sparse row data");
        for (size_t i = 0; i < num_elements; ++i) {
            auto element = row[i];
            AssertInfo(std::isfinite(element.val),
                       "Invalid sparse row: NaN or Inf value");
            AssertInfo(element.val >= 0, "Invalid sparse row: negative value");
            AssertInfo(
                element.id < std::numeric_limits<uint32_t>::max(),
                "Invalid sparse row: id should be smaller than uint32 max");
            if (i > 0) {
                AssertInfo(row[i - 1].id < element.id,
                           "Invalid sparse row: id should be strict ascending");
            }
        }
    }
    return row;
}

// Iterable is a list of bytes, each is a byte array representation of a single
// sparse float row. This helper function converts such byte arrays into a list
// of knowhere::sparse::SparseRow<float>. The resulting list is a deep copy of
// the source data.
//
// Here in segcore we validate the sparse row data only for search requests,
// as the insert/upsert data are already validated in go code.
template <typename Iterable>
std::unique_ptr<knowhere::sparse::SparseRow<float>[]>
SparseBytesToRows(const Iterable& rows, const bool validate = false) {
    AssertInfo(rows.size() > 0, "at least 1 sparse row should be provided");
    auto res =
        std::make_unique<knowhere::sparse::SparseRow<float>[]>(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        res[i] = std::move(
            CopyAndWrapSparseRow(rows[i].data(), rows[i].size(), validate));
    }
    return res;
}

// SparseRowsToProto converts a list of knowhere::sparse::SparseRow<float> to
// a milvus::proto::schema::SparseFloatArray. The resulting proto is a deep copy
// of the source data. source(i) returns the i-th row to be copied.
inline void SparseRowsToProto(
    const std::function<const knowhere::sparse::SparseRow<float>*(size_t)>&
        source,
    int64_t rows,
    milvus::proto::schema::SparseFloatArray* proto) {
    int64_t max_dim = 0;
    for (size_t i = 0; i < rows; ++i) {
        const auto* row = source(i);
        if (row == nullptr) {
            // empty row
            proto->add_contents();
            continue;
        }
        max_dim = std::max(max_dim, row->dim());
        proto->add_contents(row->data(), row->data_byte_size());
    }
    proto->set_dim(max_dim);
}

class Defer {
 public:
    Defer(std::function<void()> fn) : fn_(fn) {
    }
    ~Defer() {
        fn_();
    }

 private:
    std::function<void()> fn_;
};

#define DeferLambda(fn) Defer Defer_##__COUNTER__(fn);

}  // namespace milvus
