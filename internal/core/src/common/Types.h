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
#include "utils/Types.h"
#include <faiss/MetricType.h>
#include <string>
#include <boost/align/aligned_allocator.hpp>
#include <vector>

namespace milvus {
using Timestamp = uint64_t;  // TODO: use TiKV-like timestamp
using engine::DataType;
using engine::FieldElementType;
using engine::QueryResult;
using MetricType = faiss::MetricType;

MetricType
GetMetricType(const std::string& type);
std::string
MetricTypeToName(MetricType metric_type);

// NOTE: dependent type
// used at meta-template programming
template <class...>
constexpr std::true_type always_true{};

template <class...>
constexpr std::false_type always_false{};

template <typename T>
using aligned_vector = std::vector<T, boost::alignment::aligned_allocator<T, 512>>;

}  // namespace milvus
