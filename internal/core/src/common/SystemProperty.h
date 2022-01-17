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

#include <string>

#include "Types.h"
#include "utils/Json.h"

namespace milvus {

enum class SystemFieldType {
    Invalid = 0,
    RowId = 1,
    Timestamp = 2,
};

class SystemProperty {
 public:
    SystemProperty() = default;
    ~SystemProperty() = default;

    static const SystemProperty&
    Instance();
    SystemProperty(const SystemProperty&) = delete;

    SystemProperty&
    operator=(const SystemProperty&) = delete;

 protected:
    SystemProperty(SystemProperty&&) = default;
    SystemProperty&
    operator=(SystemProperty&&) = default;

 public:
    virtual bool
    SystemFieldVerify(const FieldName& field_name, FieldId field_id) const = 0;

    virtual SystemFieldType
    GetSystemFieldType(FieldId field_id) const = 0;

    virtual SystemFieldType
    GetSystemFieldType(FieldName field_name) const = 0;

    virtual bool
    IsSystem(FieldId field_id) const = 0;

    virtual bool
    IsSystem(FieldName field_name) const = 0;
};

}  // namespace milvus
