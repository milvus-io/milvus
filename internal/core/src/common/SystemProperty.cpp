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

#include <yaml-cpp/yaml.h>

#include "SystemProperty.h"
#include "Consts.h"
#include "exceptions/EasyAssert.h"

namespace milvus {
class SystemPropertyImpl : public SystemProperty {
 public:
    bool
    SystemFieldVerify(const FieldName& field_name, FieldId field_id) const override {
        if (!IsSystem(field_name)) {
            return false;
        }
        if (!IsSystem(field_id)) {
            return false;
        }
        auto left_id = name_to_types_.at(field_name);
        auto right_id = id_to_types_.at(field_id);
        return left_id == right_id;
    }

    SystemFieldType
    GetSystemFieldType(FieldName field_name) const override {
        Assert(IsSystem(field_name));
        return name_to_types_.at(field_name);
    }

    SystemFieldType
    GetSystemFieldType(FieldId field_id) const override {
        Assert(IsSystem(field_id));
        return id_to_types_.at(field_id);
    }

    bool
    IsSystem(FieldId field_id) const override {
        return id_to_types_.count(field_id);
    }

    bool
    IsSystem(FieldName field_name) const override {
        return name_to_types_.count(field_name);
    }

    friend SystemPropertyImpl&
    InstanceImpl();

 private:
    std::map<FieldName, SystemFieldType> name_to_types_;
    std::map<FieldId, SystemFieldType> id_to_types_;
};

SystemPropertyImpl&
InstanceImpl() {
    static auto impl = [] {
        SystemPropertyImpl impl;
        using Type = SystemFieldType;

        impl.name_to_types_.emplace(FieldName("RowID"), Type::RowId);
        impl.id_to_types_.emplace(RowFieldID, Type::RowId);

        impl.name_to_types_.emplace(FieldName("Timestamp"), Type::Timestamp);
        impl.id_to_types_.emplace(TimestampFieldID, Type::Timestamp);

        return impl;
    }();
    return impl;
}

const SystemProperty&
SystemProperty::Instance() {
    return InstanceImpl();
}

};  // namespace milvus
