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
#include "common/Types.h"

namespace milvus {

enum class SystemFieldType {
    Invalid = 0,
    RowId = 1,
    Timestamp = 2,
};

class SystemProperty {
 public:
    static const SystemProperty&
    Instance();

 public:
    virtual bool
    SystemFieldVerify(const FieldName& field_name, FieldId field_id) const = 0;

    virtual SystemFieldType
    GetSystemFieldType(FieldId field_id) const = 0;

    virtual SystemFieldType
    GetSystemFieldType(FieldName field_name) const = 0;
};

}  // namespace milvus
