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
#include <unordered_map>

#include "db/impl/MetaFields.h"
#include "db/snapshot/ResourceContext.h"
#include "db/snapshot/Resources.h"
#include "utils/Json.h"

namespace milvus::engine {

using namespace snapshot;

/////////////////////////// Macros ///////////////////////////////
#define NULLPTR_CHECK(ptr)                                                  \
    if (ptr == nullptr) {                                                   \
        return Status(SERVER_UNSUPPORTED_ERROR, "Convert pointer failed."); \
    }

//////////////////////////////////////////////////////////////////
extern const std::unordered_map<std::string, std::vector<std::string>> ResourceAttrMap;

//////////////////////////////////////////////////////////////////
//template <typename ResourceT>
//inline Status
//IntValueOfAttr(typename ResourceContext<ResourceT>::ResPtr src, const std::string& attr, int64_t& value) {
//    if (attr == F_ID) {
//        auto id_src = std::static_pointer_cast<IdField>(src);
//        NULLPTR_CHECK(id_src);
//        value = id_src->GetID();
//    } else if (attr == F_COLLECTON_ID) {
//        auto collection_id_src = std::static_pointer_cast<CollectionIdField>(src);
//        NULLPTR_CHECK(collection_id_src);
//        value = collection_id_src->GetCollectionId();
//    } else if (attr == F_CREATED_ON) {
//        auto con_src = std::static_pointer_cast<CreatedOnField>(src);
//        NULLPTR_CHECK(con_src);
//        value = con_src->GetCreatedTime();
//    } else if (attr == F_UPDATED_ON) {
//        auto uon_src = std::static_pointer_cast<UpdatedOnField>(src);
//        NULLPTR_CHECK(uon_src);
//        value = uon_src->GetUpdatedTime();
//    } else if (attr == F_SCHEMA_ID) {
//        auto schema_id_src = std::static_pointer_cast<SchemaIdField>(src);
//        NULLPTR_CHECK(schema_id_src);
//        value = schema_id_src->GetSchemaId();
//    } else if (attr == F_NUM) {
//        auto num_src = std::static_pointer_cast<NumField>(src);
//        NULLPTR_CHECK(num_src);
//        value = num_src->GetNum();
//    } else if (attr == F_FTYPE) {
//        auto num_src = std::static_pointer_cast<FtypeField>(src);
//        NULLPTR_CHECK(num_src);
//        value = num_src->GetFtype();
//    } else if (attr == F_FIELD_ID) {
//        auto field_id_src = std::static_pointer_cast<FieldIdField>(src);
//        NULLPTR_CHECK(field_id_src);
//        value = field_id_src->GetFieldId();
//    } else if (attr == F_FIELD_ELEMENT_ID) {
//        auto field_element_id_src = std::static_pointer_cast<FieldElementIdField>(src);
//        NULLPTR_CHECK(field_element_id_src);
//        value = field_element_id_src->GetFieldElementId();
//    } else if (attr == F_PARTITION_ID) {
//        auto partition_id_src = std::static_pointer_cast<PartitionIdField>(src);
//        NULLPTR_CHECK(partition_id_src);
//        value = partition_id_src->GetPartitionId();
//    } else if (attr == F_SEGMENT_ID) {
//        auto segment_id_src = std::static_pointer_cast<SegmentIdField>(src);
//        NULLPTR_CHECK(segment_id_src);
//        value = segment_id_src->GetSegmentId();
//    } else {
//        return Status(SERVER_UNSUPPORTED_ERROR, "Unknown field name of int type");
//    }
//
//    return Status::OK();
//}
//
//template <typename ResourceT>
//inline Status
//UintValueOfAttr(ResourceContext<ResourceT>::ResPtr src, const std::string& attr, uint64_t& value) {
//    if (attr == F_LSN) {
//        auto lsn_src = std::static_pointer_cast<LsnField>(src);
//        if (nullptr == lsn_src) {
//            return Status(SERVER_UNSUPPORTED_ERROR, "Convert pointer failed.");
//        }
//        value = lsn_src->GetLsn();
//    } else if (attr == F_SIZE) {
//        auto size_src = std::static_pointer_cast<SizeField>(src);
//        if (nullptr == size_src) {
//            return Status(SERVER_UNSUPPORTED_ERROR, "Convert pointer failed.");
//        }
//        value = size_src->GetSize();
//    } else if (attr == F_ROW_COUNT) {
//        auto row_count_src = std::static_pointer_cast<RowCountField>(src);
//        if (nullptr == row_count_src) {
//            return Status(SERVER_UNSUPPORTED_ERROR, "Convert pointer failed.");
//        }
//        value = row_count_src->GetRowCount();
//    } else {
//        return Status(SERVER_UNSUPPORTED_ERROR, "Unknown field name of int type");
//    }
//
//    return Status::OK();
//}

//template <typename ResourceT>
//inline Status
//StateValueOfAttr(ResourceContextPtr<ResourceT> src, const std::string& attr, State& value) {
//    if (attr == "status") {
//        auto status_src = std::static_pointer_cast<StateField>(src);
//        if (status_src == nullptr) {
//            return Status(SERVER_UNSUPPORTED_ERROR, "Convert pointer failed.");
//        }
//        value = status_src->GetState();
//    } else {
//        return Status(SERVER_UNSUPPORTED_ERROR, "Unknown field name of int type");
//    }
//
//    return Status::OK();
//}

//template <typename ResourceT>
//inline Status
//MappingValueOfAttr(ResourceContextPtr<ResourceT> src, const std::string& attr, MappingT& value) {
//    if (attr == "mappings") {
//        auto mappings_src = std::static_pointer_cast<MappingsField>(src);
//        if (mappings_src == nullptr) {
//            return Status(SERVER_UNSUPPORTED_ERROR, "Convert point to MappingsField point failed.");
//        }
//        value = mappings_src->GetMappings();
//    } else {
//        return Status(SERVER_UNSUPPORTED_ERROR, "Unknown field name of int type");
//    }
//
//    return Status::OK();
//}

//template <typename ResourceT>
//inline Status
//StringValueOfAttr(ResourceContextPtr<ResourceT> src, const std::string& attr, std::string& value) {
//    if (attr == F_NAME) {
//        auto name_src = std::static_pointer_cast<NameField>(src);
//        if (name_src == nullptr) {
//            return Status(SERVER_UNSUPPORTED_ERROR, "Convert point to NameField point failed.");
//        }
//        value = name_src->GetName();
//    } else {
//        return Status(SERVER_UNSUPPORTED_ERROR, "Unknown field name of int type");
//    }
//    return Status::OK();
//}

//template <typename ResourceT>
//inline Status
//JsonValueOfAttr(ResourceContextPtr<ResourceT> src, const std::string& attr, json& value) {
//    if (attr == F_PARAMS) {
//        auto param_src = std::static_pointer_cast<ParamsField>(src);
//        if (param_src == nullptr) {
//            return Status(SERVER_UNSUPPORTED_ERROR, "Convert point to NameField point failed.");
//        }
//        value = param_src->GetParams();
//    } else {
//        return Status(SERVER_UNSUPPORTED_ERROR, "Unknown field name of int type");
//    }
//    return Status::OK();
//}

template <typename ResourceT>
inline Status
AttrValue2Str(typename ResourceContext<ResourceT>::ResPtr src, const std::string& attr, std::string& value) {
    int64_t int_value;
    uint64_t uint_value;
    State state_value;
    MappingT mapping_value;
    std::string str_value;
    json json_value;

    auto int2str = [](const int64_t& ival, std::string& val) {
        val = std::to_string(ival);
    };

    auto uint2str = [](const uint64_t& uival, std::string& val) {
        val = std::to_string(uival);
    };

    auto state2str = [](const State& sval, std::string& val) {
        val = std::to_string(sval);
    };

    auto mappings2str = [](const MappingT& mval, std::string& val) {
        auto value_json = json::array();
        for (auto& m : mval) {
            value_json.emplace_back(m);
        }

        val = "\'" + value_json.dump() + "\'";
    };

    auto str2str = [](const std::string& sval, std::string& val) {
        val = "\'" + sval + "\'";
    };

    auto json2str = [](const json& jval, std::string& val) {
        val = "\'" + jval.dump() + "\'";
    };

    // TODO: try use static_pointer_cast
    if (attr == F_ID) {
        auto id_field = std::dynamic_pointer_cast<IdField>(src);
        int_value = id_field->GetID();
        int2str(int_value, value);
    } else if (F_COLLECTON_ID == attr) {
        auto collection_id_field = std::dynamic_pointer_cast<CollectionIdField>(src);
        int_value = collection_id_field->GetCollectionId();
        int2str(int_value, value);
    } else if (F_CREATED_ON == attr) {
        auto created_field = std::dynamic_pointer_cast<CreatedOnField>(src);
        int_value = created_field->GetCreatedTime();
        int2str(int_value, value);
    } else if (F_UPDATED_ON == attr) {
        auto updated_field = std::dynamic_pointer_cast<UpdatedOnField>(src);
        int_value = updated_field->GetUpdatedTime();
        int2str(int_value, value);
    } else if (F_SCHEMA_ID == attr) {
        auto schema_id_field = std::dynamic_pointer_cast<SchemaIdField>(src);
        int_value = schema_id_field->GetSchemaId();
        int2str(int_value, value);
    } else if (F_NUM == attr) {
        auto num_field = std::dynamic_pointer_cast<NumField>(src);
        int_value = num_field->GetNum();
        int2str(int_value, value);
    } else if (F_FTYPE == attr) {
        auto ftype_field = std::dynamic_pointer_cast<FtypeField>(src);
        int_value = ftype_field->GetFtype();
        int2str(int_value, value);
    } else if (F_FIELD_ID == attr) {
        auto field_id_field = std::dynamic_pointer_cast<FieldIdField>(src);
        int_value = field_id_field->GetFieldId();
        int2str(int_value, value);
    } else if (F_FIELD_ELEMENT_ID == attr) {
        auto element_id_field = std::dynamic_pointer_cast<FieldElementIdField>(src);
        int_value = element_id_field->GetFieldElementId();
        int2str(int_value, value);
    } else if (F_PARTITION_ID == attr) {
        auto partition_id_field = std::dynamic_pointer_cast<PartitionIdField>(src);
        int_value = partition_id_field->GetPartitionId();
        int2str(int_value, value);
    } else if (F_SEGMENT_ID == attr) {
        auto segment_id_field = std::dynamic_pointer_cast<SegmentIdField>(src);
        int_value = segment_id_field->GetSegmentId();
        int2str(int_value, value);
    } /* Uint field */ else if (F_LSN == attr) {
        auto lsn_field = std::dynamic_pointer_cast<LsnField>(src);
        uint_value = lsn_field->GetLsn();
        uint2str(uint_value, value);
    } else if (F_SIZE == attr) {
        auto size_field = std::dynamic_pointer_cast<SizeField>(src);
        uint_value = size_field->GetSize();
        uint2str(uint_value, value);
    } else if (F_ROW_COUNT == attr) {
        auto row_count_field = std::dynamic_pointer_cast<RowCountField>(src);
        uint_value = row_count_field->GetRowCount();
        uint2str(uint_value, value);
    } else if (F_STATE == attr) {
        auto state_field = std::dynamic_pointer_cast<StateField>(src);
        state_value = state_field->GetState();
        state2str(state_value, value);
    } else if (F_MAPPINGS == attr) {
        auto mappings_field = std::dynamic_pointer_cast<MappingsField>(src);
        mapping_value = mappings_field->GetMappings();
        mappings2str(mapping_value, value);
    } else if (F_NAME == attr) {
        auto name_field = std::dynamic_pointer_cast<NameField>(src);
        str_value = name_field->GetName();
        str2str(str_value, value);
    } else if (F_PARAMS == attr) {
        auto params_field = std::dynamic_pointer_cast<ParamsField>(src);
        json_value = params_field->GetParams();
        json2str(json_value, value);
    } else {
        return Status(SERVER_UNSUPPORTED_ERROR, "Unknown field attr: " + attr);
    }

    return Status::OK();
}

template <typename ResourceT>
inline Status
ResourceContextAddAttrMap(ResourceContextPtr<ResourceT> src, std::map<std::string, std::string>& attr_map) {
    std::vector<std::string> attrs = ResourceAttrMap.at(ResourceT::Name);

    for (auto& attr : attrs) {
        std::string value;
        AttrValue2Str<ResourceT>(src->Resource(), attr, value);
        attr_map[attr] = value;
    }

    return Status::OK();
}

template <typename ResourceT>
inline Status
ResourceContextToAddSql(ResourceContextPtr<ResourceT> res, std::string& sql) {

    std::map<std::string, std::string> attr_map;
    ResourceContextAddAttrMap<ResourceT>(res, attr_map);

    sql = "INSERT INTO " + res->Table();
    std::string field_names = "(";
    std::string values = "(";
    for (auto& kv : attr_map) {
        field_names += kv.first + ",";
        values += kv.second + ",";
    }
    field_names.erase(field_names.end() - 1, field_names.end());
    values.erase(values.end() - 1, values.end());
    sql += field_names + ")" + "VALUES" + values + ")";

    return Status::OK();
}

template <typename ResourceT>
inline Status
ResourceContextUpdateAttrMap(ResourceContextPtr<ResourceT> res, std::map<std::string, std::string>& attr_map) {
    std::string value;
    for (auto& attr: res->Attrs()) {
        AttrValue2Str<ResourceT>(res->Resource(), attr, value);
        attr_map[attr] = value;
    }

    return Status::OK();
}

template <typename ResourceT>
inline Status
ResourceContextToUpdateSql(ResourceContextPtr<ResourceT> res, std::string& sql) {
    std::map<std::string, std::string> attr_map;
    ResourceContextUpdateAttrMap<ResourceT>(res, attr_map);

    sql = "UPDATE " + res->Table() + " SET ";
    std::string field_pairs;
    for (auto& attr_kv: attr_map) {
        field_pairs += attr_kv.first + "=" + attr_kv.second + ",";
    }

    field_pairs.erase(field_pairs.end() - 1, field_pairs.end());
    sql += field_pairs;

    std::string id_value;
    AttrValue2Str<ResourceT>(res->Resource(), "id", id_value);
    sql += " WHERE id = " + id_value;

    return Status::OK();
}

template <typename ResourceT>
inline Status
ResourceContextDeleteAttrMap(ResourceContextPtr<ResourceT> res, std::map<std::string, std::string>& attr_map) {
    std::string id_value;
    AttrValue2Str<ResourceT>(res->Resource(), F_ID, id_value);
    attr_map[F_ID] = id_value;

    return Status::OK();
}

template <typename ResourceT>
inline Status
ResourceContextToDeleteSql(ResourceContextPtr<ResourceT> res, std::string& sql) {
    auto id_value = std::to_string(res->ID());
    sql = "DELETE FROM " + res->Table() + " WHERE id = " + id_value;
    return Status::OK();
}

}