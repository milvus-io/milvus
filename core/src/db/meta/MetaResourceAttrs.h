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

#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/meta/MetaNames.h"
#include "db/snapshot/ResourceContext.h"
#include "db/snapshot/Resources.h"
#include "utils/Json.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

// using namespace snapshot;
using snapshot::MappingT;
using snapshot::ResourceContext;

/////////////////////////// Macros ///////////////////////////////
#define NULLPTR_CHECK(ptr)                                                  \
    if (ptr == nullptr) {                                                   \
        return Status(SERVER_UNSUPPORTED_ERROR, "Convert pointer failed."); \
    }

//////////////////////////////////////////////////////////////////
Status
ResourceAttrMapOf(const std::string& table, std::vector<std::string>& attrs);

//////////////////////////////////////////////////////////////////
inline void
int2str(const int64_t& ival, std::string& val) {
    val = std::to_string(ival);
}

inline void
uint2str(const uint64_t& uival, std::string& val) {
    val = std::to_string(uival);
}

inline void
state2str(const snapshot::State& sval, std::string& val) {
    val = std::to_string(sval);
}

inline void
mappings2str(const MappingT& mval, std::string& val) {
    auto value_json = json::array();
    for (auto& m : mval) {
        value_json.emplace_back(m);
    }

    val = "\'" + value_json.dump() + "\'";
}

inline void
str2str(const std::string& sval, std::string& val) {
    val = "\'" + sval + "\'";
}

inline void
json2str(const json& jval, std::string& val) {
    val = "\'" + jval.dump() + "\'";
}

template <typename ResourceT>
inline Status
AttrValue2Str(typename ResourceContext<ResourceT>::ResPtr src, const std::string& attr, std::string& value) {
    int64_t int_value;
    uint64_t uint_value;
    snapshot::State state_value;
    MappingT mapping_value;
    std::string str_value;
    json json_value;

    if (attr == F_ID) {
        auto id_field = std::dynamic_pointer_cast<snapshot::IdField>(src);
        int_value = id_field->GetID();
        int2str(int_value, value);
    } else if (F_COLLECTON_ID == attr) {
        auto collection_id_field = std::dynamic_pointer_cast<snapshot::CollectionIdField>(src);
        int_value = collection_id_field->GetCollectionId();
        int2str(int_value, value);
    } else if (F_CREATED_ON == attr) {
        auto created_field = std::dynamic_pointer_cast<snapshot::CreatedOnField>(src);
        int_value = created_field->GetCreatedTime();
        int2str(int_value, value);
    } else if (F_UPDATED_ON == attr) {
        auto updated_field = std::dynamic_pointer_cast<snapshot::UpdatedOnField>(src);
        int_value = updated_field->GetUpdatedTime();
        int2str(int_value, value);
    } else if (F_SCHEMA_ID == attr) {
        auto schema_id_field = std::dynamic_pointer_cast<snapshot::SchemaIdField>(src);
        int_value = schema_id_field->GetSchemaId();
        int2str(int_value, value);
    } else if (F_NUM == attr) {
        auto num_field = std::dynamic_pointer_cast<snapshot::NumField>(src);
        int_value = num_field->GetNum();
        int2str(int_value, value);
    } else if (F_FTYPE == attr) {
        auto ftype_field = std::dynamic_pointer_cast<snapshot::FtypeField>(src);
        int_value = (int)ftype_field->GetFtype();
        int2str(int_value, value);
    } else if (F_FETYPE == attr) {
        auto fetype_field = std::dynamic_pointer_cast<snapshot::FEtypeField>(src);
        int_value = (int)fetype_field->GetFEtype();
        int2str(int_value, value);
    } else if (F_FIELD_ID == attr) {
        auto field_id_field = std::dynamic_pointer_cast<snapshot::FieldIdField>(src);
        int_value = field_id_field->GetFieldId();
        int2str(int_value, value);
    } else if (F_FIELD_ELEMENT_ID == attr) {
        auto element_id_field = std::dynamic_pointer_cast<snapshot::FieldElementIdField>(src);
        int_value = element_id_field->GetFieldElementId();
        int2str(int_value, value);
    } else if (F_PARTITION_ID == attr) {
        auto partition_id_field = std::dynamic_pointer_cast<snapshot::PartitionIdField>(src);
        int_value = partition_id_field->GetPartitionId();
        int2str(int_value, value);
    } else if (F_SEGMENT_ID == attr) {
        auto segment_id_field = std::dynamic_pointer_cast<snapshot::SegmentIdField>(src);
        int_value = segment_id_field->GetSegmentId();
        int2str(int_value, value);
    } /* Uint field */ else if (F_LSN == attr) {
        auto lsn_field = std::dynamic_pointer_cast<snapshot::LsnField>(src);
        uint_value = lsn_field->GetLsn();
        uint2str(uint_value, value);
    } else if (F_SIZE == attr) {
        auto size_field = std::dynamic_pointer_cast<snapshot::SizeField>(src);
        uint_value = size_field->GetSize();
        uint2str(uint_value, value);
    } else if (F_ROW_COUNT == attr) {
        auto row_count_field = std::dynamic_pointer_cast<snapshot::RowCountField>(src);
        uint_value = row_count_field->GetRowCount();
        uint2str(uint_value, value);
    } else if (F_STATE == attr) {
        auto state_field = std::dynamic_pointer_cast<snapshot::StateField>(src);
        state_value = state_field->GetState();
        state2str(state_value, value);
    } else if (F_MAPPINGS == attr) {
        if (auto mappings_field = std::dynamic_pointer_cast<snapshot::FlushableMappingsField>(src)) {
            mapping_value = mappings_field->GetFlushIds();
            mappings2str(mapping_value, value);
        } else if (auto mappings_field = std::dynamic_pointer_cast<snapshot::MappingsField>(src)) {
            mapping_value = mappings_field->GetMappings();
            mappings2str(mapping_value, value);
        }
    } else if (F_NAME == attr) {
        auto name_field = std::dynamic_pointer_cast<snapshot::NameField>(src);
        str_value = name_field->GetName();
        str2str(str_value, value);
    } else if (F_TYPE_NAME == attr) {
        auto type_name_field = std::dynamic_pointer_cast<snapshot::TypeNameField>(src);
        str_value = type_name_field->GetTypeName();
        str2str(str_value, value);
    } else if (F_PARAMS == attr) {
        auto params_field = std::dynamic_pointer_cast<snapshot::ParamsField>(src);
        json_value = params_field->GetParams();
        json2str(json_value, value);
    } else {
        return Status(SERVER_UNSUPPORTED_ERROR, "Unknown field attr: " + attr);
    }

    return Status::OK();
}

template <typename ResourceT>
inline Status
ResourceContextAddAttrMap(snapshot::ResourceContextPtr<ResourceT> src,
                          std::unordered_map<std::string, std::string>& attr_map) {
    std::vector<std::string> attrs;
    auto status = ResourceAttrMapOf(ResourceT::Name, attrs);
    if (!status.ok()) {
        return status;
    }

    for (auto& attr : attrs) {
        if (attr == F_ID) {
            continue;
        }

        std::string value;
        AttrValue2Str<ResourceT>(src->Resource(), attr, value);
        attr_map[attr] = value;
    }

    return Status::OK();
}

template <typename ResourceT>
inline Status
ResourceContextUpdateAttrMap(snapshot::ResourceContextPtr<ResourceT> res,
                             std::unordered_map<std::string, std::string>& attr_map) {
    std::string value;
    for (auto& attr : res->Attrs()) {
        AttrValue2Str<ResourceT>(res->Resource(), attr, value);
        attr_map[attr] = value;
    }

    return Status::OK();
}

/////////////////////////////////////////////////////////////////////
template <typename T>
inline void
ResourceFieldToSqlStr(const T& t, std::string& val) {
    val = "";
}

template <>
inline void
ResourceFieldToSqlStr<int64_t>(const int64_t& ival, std::string& val) {
    int2str(ival, val);
}

template <>
inline void
ResourceFieldToSqlStr<uint64_t>(const uint64_t& uival, std::string& val) {
    uint2str(uival, val);
}

template <>
inline void
ResourceFieldToSqlStr<snapshot::State>(const snapshot::State& sval, std::string& val) {
    state2str(sval, val);
}

template <>
inline void
ResourceFieldToSqlStr<MappingT>(const MappingT& mval, std::string& val) {
    mappings2str(mval, val);
}

template <>
inline void
ResourceFieldToSqlStr<std::string>(const std::string& sval, std::string& val) {
    str2str(sval, val);
}

template <>
inline void
ResourceFieldToSqlStr<json>(const json& jval, std::string& val) {
    json2str(jval, val);
}

}  // namespace milvus::engine::meta
