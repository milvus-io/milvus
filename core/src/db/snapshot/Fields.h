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

#include <string>
#include <utility>

#include "db/snapshot/ResourceTypes.h"

using milvus::engine::utils::GetMicroSecTimeStamp;

namespace milvus::engine::snapshot {

class MappingsField {
 public:
    //    using Type = MappingsField;
    using ValueType = MappingT;
    static constexpr const char* Name = "mappings";

    explicit MappingsField(MappingT mappings = {}) : mappings_(std::move(mappings)) {
    }

    const MappingT&
    GetMappings() const {
        return mappings_;
    }
    MappingT&
    GetMappings() {
        return mappings_;
    }

 protected:
    MappingT mappings_;
};

class FlushableMappingsField : public MappingsField {
 public:
    explicit FlushableMappingsField(MappingT ids = {}) : ids_(std::move(ids)) {
    }

    void
    UpdateFlushIds() {
        if (ids_.size() == 0) {
            ids_ = {1};
        } else {
            ids_ = {*(ids_.begin()) + 1};
        }
    }

    Status
    LoadIds(const std::string& base_path, const std::string& prefix = "");
    Status
    FlushIds(const std::string& base_path, const std::string& prefix = "");

    const MappingT&
    GetFlushIds() const {
        return ids_;
    }
    MappingT&
    GetFlushIds() {
        return ids_;
    }

 protected:
    MappingT ids_;
    bool loaded_ = false;
};

class StateField {
 public:
    //    using Type = StateField;
    using ValueType = State;
    static constexpr const char* Name = "state";

    explicit StateField(State state = PENDING) : state_(state) {
    }

    State
    GetState() const {
        return state_;
    }

    bool
    IsActive() const {
        return state_ == ACTIVE;
    }

    bool
    IsDeactive() const {
        return state_ == DEACTIVE;
    }

    bool
    Activate() {
        if (IsDeactive())
            return false;
        state_ = ACTIVE;
        return true;
    }

    void
    Deactivate() {
        state_ = DEACTIVE;
    }

    void
    ResetStatus() {
        state_ = PENDING;
    }

 protected:
    State state_;
};

class LsnField {
 public:
    using ValueType = LSN_TYPE;
    static constexpr const char* Name = "lsn";

    explicit LsnField(LSN_TYPE lsn = 0) : lsn_(lsn) {
    }

    const LSN_TYPE&
    GetLsn() const {
        return lsn_;
    }

    void
    SetLsn(const LSN_TYPE& lsn) {
        lsn_ = lsn;
    }

 protected:
    LSN_TYPE lsn_;
};

class CreatedOnField {
 public:
    using ValueType = TS_TYPE;
    static constexpr const char* Name = "created_on";

    explicit CreatedOnField(TS_TYPE created_on = GetMicroSecTimeStamp()) : created_on_(created_on) {
    }

    TS_TYPE
    GetCreatedTime() const {
        return created_on_;
    }

    void
    SetCreatedTime(const TS_TYPE& time) {
        created_on_ = time;
    }

 protected:
    TS_TYPE created_on_;
};

class UpdatedOnField {
 public:
    using ValueType = TS_TYPE;
    static constexpr const char* Name = "updated_on";

    explicit UpdatedOnField(TS_TYPE updated_on = GetMicroSecTimeStamp()) : updated_on_(updated_on) {
    }

    TS_TYPE
    GetUpdatedTime() const {
        return updated_on_;
    }

    void
    SetUpdatedTime(const TS_TYPE& time) {
        updated_on_ = time;
    }

 protected:
    TS_TYPE updated_on_;
};

class IdField {
 public:
    using ValueType = ID_TYPE;
    static constexpr const char* Name = "id";

    explicit IdField(ID_TYPE id) : id_(id) {
    }

    ID_TYPE
    GetID() const {
        return id_;
    }
    void
    SetID(ID_TYPE id) {
        id_ = id;
    }
    bool
    HasAssigned() {
        return id_ > 0;
    }

 protected:
    ID_TYPE id_;
};

class CollectionIdField {
 public:
    using ValueType = ID_TYPE;
    static constexpr const char* Name = "collection_id";

    explicit CollectionIdField(ID_TYPE id) : collection_id_(id) {
    }

    ID_TYPE
    GetCollectionId() const {
        return collection_id_;
    }

    void
    SetCollectionId(ID_TYPE id) {
        collection_id_ = id;
    }

 protected:
    ID_TYPE collection_id_;
};

class SchemaIdField {
 public:
    using ValueType = ID_TYPE;
    static constexpr const char* Name = "schema_id";

    explicit SchemaIdField(ID_TYPE id) : schema_id_(id) {
    }

    ID_TYPE
    GetSchemaId() const {
        return schema_id_;
    }
    void
    SetSchemaId(ID_TYPE schema_id) {
        schema_id_ = schema_id;
    }

 protected:
    ID_TYPE schema_id_;
};

class NumField {
 public:
    using ValueType = NUM_TYPE;
    static constexpr const char* Name = "num";

    explicit NumField(NUM_TYPE num) : num_(num) {
    }

    NUM_TYPE
    GetNum() const {
        return num_;
    }
    void
    SetNum(NUM_TYPE num) {
        num_ = num;
    }

 protected:
    NUM_TYPE num_;
};

class FtypeField {
 public:
    using ValueType = FTYPE_TYPE;
    static constexpr const char* Name = "ftype";

    explicit FtypeField(FTYPE_TYPE ftype) : ftype_(ftype) {
    }

    FTYPE_TYPE
    GetFtype() const {
        return ftype_;
    }

    void
    SetFtype(FTYPE_TYPE ftype) {
        ftype_ = ftype;
    }

 protected:
    FTYPE_TYPE ftype_;
};

class FEtypeField {
 public:
    using ValueType = FETYPE_TYPE;
    static constexpr const char* Name = "fetype";

    explicit FEtypeField(FETYPE_TYPE fetype) : fetype_(fetype) {
    }

    FETYPE_TYPE
    GetFEtype() const {
        return fetype_;
    }

    void
    SetFEtype(FETYPE_TYPE fetype) {
        fetype_ = fetype;
    }

 protected:
    FETYPE_TYPE fetype_;
};

class FieldIdField {
 public:
    using ValueType = ID_TYPE;
    static constexpr const char* Name = "field_id";

    explicit FieldIdField(ID_TYPE id) : field_id_(id) {
    }

    ID_TYPE
    GetFieldId() const {
        return field_id_;
    }

    void
    SetFieldId(ID_TYPE id) {
        field_id_ = id;
    }

 protected:
    ID_TYPE field_id_;
};

class FieldElementIdField {
 public:
    using ValueType = ID_TYPE;
    static constexpr const char* Name = "field_element_id";

    explicit FieldElementIdField(ID_TYPE id) : field_element_id_(id) {
    }

    ID_TYPE
    GetFieldElementId() const {
        return field_element_id_;
    }

    void
    SetFieldElementId(ID_TYPE id) {
        field_element_id_ = id;
    }

 protected:
    ID_TYPE field_element_id_;
};

class PartitionIdField {
 public:
    using ValueType = ID_TYPE;
    static constexpr const char* Name = "partition_id";

    explicit PartitionIdField(ID_TYPE id) : partition_id_(id) {
    }

    ID_TYPE
    GetPartitionId() const {
        return partition_id_;
    }

    void
    SetPartitionId(ID_TYPE id) {
        partition_id_ = id;
    }

 protected:
    ID_TYPE partition_id_;
};

class SegmentIdField {
 public:
    using ValueType = ID_TYPE;
    static constexpr const char* Name = "segment_id";

    explicit SegmentIdField(ID_TYPE id) : segment_id_(id) {
    }

    ID_TYPE
    GetSegmentId() const {
        return segment_id_;
    }

    void
    SetSegmentId(ID_TYPE id) {
        segment_id_ = id;
    }

 protected:
    ID_TYPE segment_id_;
};

class TypeNameField {
 public:
    using ValueType = std::string;
    static constexpr const char* Name = "TypeName";

    explicit TypeNameField(std::string val) : type_name_(std::move(val)) {
    }

    const std::string&
    GetTypeName() const {
        return type_name_;
    }

    void
    SetTypeName(const std::string& val) {
        type_name_ = val;
    }

 protected:
    std::string type_name_;
};

class NameField {
 public:
    using ValueType = std::string;
    static constexpr const char* Name = "name";

    explicit NameField(std::string name) : name_(std::move(name)) {
    }

    const std::string&
    GetName() const {
        return name_;
    }

    void
    SetName(const std::string& name) {
        name_ = name;
    }

 protected:
    std::string name_;
};

class ParamsField {
 public:
    using ValueType = json;
    static constexpr const char* Name = "params";

    explicit ParamsField(const json& params) : params_(params) {
    }

    const json&
    GetParams() const {
        return params_;
    }

    void
    SetParams(const json& params) {
        params_ = params;
    }

 protected:
    json params_;
};

class SizeField {
 public:
    using ValueType = SIZE_TYPE;
    static constexpr const char* Name = "size";

    explicit SizeField(SIZE_TYPE size) : size_(size) {
    }

    SIZE_TYPE
    GetSize() const {
        return size_;
    }

    void
    SetSize(SIZE_TYPE size) {
        size_ = size;
    }

 protected:
    SIZE_TYPE size_;
};

class RowCountField {
 public:
    using ValueType = SIZE_TYPE;
    static constexpr const char* Name = "row_count";

    explicit RowCountField(SIZE_TYPE size) : size_(size) {
    }

    SIZE_TYPE
    GetRowCount() const {
        return size_;
    }

    void
    SetRowCount(SIZE_TYPE row_cnt) {
        size_ = row_cnt;
    }

 protected:
    SIZE_TYPE size_;
};

}  // namespace milvus::engine::snapshot
