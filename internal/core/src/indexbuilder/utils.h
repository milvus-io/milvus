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

#include <algorithm>
#include <string>
#include <tuple>
#include <vector>
#include <functional>

#include <knowhere/common/Typedef.h>
#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus::indexbuilder {

std::vector<knowhere::IndexType>
NM_List();

std::vector<knowhere::IndexType>
BIN_List();

std::vector<knowhere::IndexType>
Need_ID_List();

std::vector<knowhere::IndexType>
Need_BuildAll_list();

std::vector<std::tuple<knowhere::IndexType, knowhere::MetricType>>
unsupported_index_combinations();

template <typename T>
bool
is_in_list(const T& t, std::function<std::vector<T>()> list_func);

bool
is_in_bin_list(const knowhere::IndexType& index_type);

bool
is_in_nm_list(const knowhere::IndexType& index_type);

bool
is_in_need_build_all_list(const knowhere::IndexType& index_type);

bool
is_in_need_id_list(const knowhere::IndexType& index_type);

bool
is_unsupported(const knowhere::IndexType& index_type, const knowhere::MetricType& metric_type);

}  // namespace milvus::indexbuilder
