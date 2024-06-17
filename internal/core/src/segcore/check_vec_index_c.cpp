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

#include <string>
#include "check_vec_index_c.h"
#include "common/Types.h"
#include "knowhere/comp/knowhere_check.h"

bool
CheckVecIndexWithDataType(const char* index_type, enum CDataType data_type) {
    return knowhere::KnowhereCheck::IndexTypeAndDataTypeCheck(
        std::string(index_type), knowhere::VecType(data_type));
}
