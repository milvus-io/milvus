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

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "db/Utils.h"
#include "db/snapshot/BaseResource.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/ScopedResource.h"
#include "utils/Json.h"

using milvus::engine::utils::GetMicroSecTimeStamp;

namespace milvus::engine::snapshot {

static const json JEmpty = {};

class MappingsField {
 public:
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

class StateField {
 public:
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
    static constexpr const char* Name = "ftype";

    explicit FtypeField(FTYPE_TYPE type) : ftype_(type) {
    }

    FTYPE_TYPE
    GetFtype() const {
        return ftype_;
    }

    void
    SetFtype(FTYPE_TYPE type) {
        ftype_ = type;
    }

 protected:
    FTYPE_TYPE ftype_;
};

class FieldIdField {
 public:
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

class NameField {
 public:
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

///////////////////////////////////////////////////////////////////////////////

class Collection : public BaseResource<Collection>,
                   public NameField,
                   public ParamsField,
                   public IdField,
                   public LsnField,
                   public StateField,
                   public CreatedOnField,
                   public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<Collection>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<Collection>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "Collection";

    explicit Collection(const std::string& name, const json& params = JEmpty, ID_TYPE id = 0, LSN_TYPE lsn = 0,
                        State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp(),
                        TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
};

using CollectionPtr = Collection::Ptr;

class CollectionCommit : public BaseResource<CollectionCommit>,
                         public CollectionIdField,
                         public SchemaIdField,
                         public MappingsField,
                         public RowCountField,
                         public SizeField,
                         public IdField,
                         public LsnField,
                         public StateField,
                         public CreatedOnField,
                         public UpdatedOnField {
 public:
    static constexpr const char* Name = "CollectionCommit";
    using Ptr = std::shared_ptr<CollectionCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<CollectionCommit>>;
    using VecT = std::vector<Ptr>;
    CollectionCommit(ID_TYPE collection_id, ID_TYPE schema_id, const MappingT& mappings = {}, SIZE_TYPE row_cnt = 0,
                     SIZE_TYPE size = 0, ID_TYPE id = 0, LSN_TYPE lsn = 0, State status = PENDING,
                     TS_TYPE created_on = GetMicroSecTimeStamp(), TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
};

using CollectionCommitPtr = CollectionCommit::Ptr;

///////////////////////////////////////////////////////////////////////////////

class Partition : public BaseResource<Partition>,
                  public NameField,
                  public CollectionIdField,
                  public IdField,
                  public LsnField,
                  public StateField,
                  public CreatedOnField,
                  public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<Partition>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<Partition>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "Partitions";

    Partition(const std::string& name, ID_TYPE collection_id, ID_TYPE id = 0, LSN_TYPE lsn = 0, State status = PENDING,
              TS_TYPE created_on = GetMicroSecTimeStamp(), TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
};

using PartitionPtr = Partition::Ptr;

class PartitionCommit : public BaseResource<PartitionCommit>,
                        public CollectionIdField,
                        public PartitionIdField,
                        public MappingsField,
                        public RowCountField,
                        public SizeField,
                        public IdField,
                        public LsnField,
                        public StateField,
                        public CreatedOnField,
                        public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<PartitionCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<PartitionCommit>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "PartitionCommit";

    PartitionCommit(ID_TYPE collection_id, ID_TYPE partition_id, const MappingT& mappings = {}, SIZE_TYPE row_cnt = 0,
                    SIZE_TYPE size = 0, ID_TYPE id = 0, LSN_TYPE lsn = 0, State status = PENDING,
                    TS_TYPE created_on = GetMicroSecTimeStamp(), TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());

    std::string
    ToString() const override;
};

using PartitionCommitPtr = PartitionCommit::Ptr;

///////////////////////////////////////////////////////////////////////////////

class Segment : public BaseResource<Segment>,
                public CollectionIdField,
                public PartitionIdField,
                public NumField,
                public IdField,
                public LsnField,
                public StateField,
                public CreatedOnField,
                public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<Segment>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<Segment>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "Segment";

    explicit Segment(ID_TYPE collection_id, ID_TYPE partition_id, ID_TYPE num = 0, ID_TYPE id = 0, LSN_TYPE lsn = 0,
                     State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp(),
                     TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());

    std::string
    ToString() const override;
};

using SegmentPtr = Segment::Ptr;

class SegmentCommit : public BaseResource<SegmentCommit>,
                      public SchemaIdField,
                      public PartitionIdField,
                      public SegmentIdField,
                      public MappingsField,
                      public RowCountField,
                      public SizeField,
                      public IdField,
                      public LsnField,
                      public StateField,
                      public CreatedOnField,
                      public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<SegmentCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<SegmentCommit>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "SegmentCommit";

    SegmentCommit(ID_TYPE schema_id, ID_TYPE partition_id, ID_TYPE segment_id, const MappingT& mappings = {},
                  SIZE_TYPE row_cnt = 0, SIZE_TYPE size = 0, ID_TYPE id = 0, LSN_TYPE lsn = 0, State status = PENDING,
                  TS_TYPE created_on = GetMicroSecTimeStamp(), TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());

    std::string
    ToString() const override;
};

using SegmentCommitPtr = SegmentCommit::Ptr;

///////////////////////////////////////////////////////////////////////////////

class SegmentFile : public BaseResource<SegmentFile>,
                    public CollectionIdField,
                    public PartitionIdField,
                    public SegmentIdField,
                    public FieldElementIdField,
                    public RowCountField,
                    public SizeField,
                    public IdField,
                    public LsnField,
                    public StateField,
                    public CreatedOnField,
                    public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<SegmentFile>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<SegmentFile>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "SegmentFile";

    SegmentFile(ID_TYPE collection_id, ID_TYPE partition_id, ID_TYPE segment_id, ID_TYPE field_element_id,
                SIZE_TYPE row_cnt = 0, SIZE_TYPE size = 0, ID_TYPE id = 0, LSN_TYPE lsn = 0, State status = PENDING,
                TS_TYPE created_on = GetMicroSecTimeStamp(), TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
};

using SegmentFilePtr = SegmentFile::Ptr;

///////////////////////////////////////////////////////////////////////////////

class SchemaCommit : public BaseResource<SchemaCommit>,
                     public CollectionIdField,
                     public MappingsField,
                     public IdField,
                     public LsnField,
                     public StateField,
                     public CreatedOnField,
                     public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<SchemaCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<SchemaCommit>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "SchemaCommit";

    explicit SchemaCommit(ID_TYPE collection_id, const MappingT& mappings = {}, ID_TYPE id = 0, LSN_TYPE lsn = 0,
                          State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp(),
                          TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
};

using SchemaCommitPtr = SchemaCommit::Ptr;

///////////////////////////////////////////////////////////////////////////////

class Field : public BaseResource<Field>,
              public NameField,
              public NumField,
              public FtypeField,
              public ParamsField,
              public IdField,
              public LsnField,
              public StateField,
              public CreatedOnField,
              public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<Field>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<Field>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "Field";

    Field(const std::string& name, NUM_TYPE num, FTYPE_TYPE ftype, const json& params = JEmpty, ID_TYPE id = 0,
          LSN_TYPE lsn = 0, State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp(),
          TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
};

using FieldPtr = Field::Ptr;

class FieldCommit : public BaseResource<FieldCommit>,
                    public CollectionIdField,
                    public FieldIdField,
                    public MappingsField,
                    public IdField,
                    public LsnField,
                    public StateField,
                    public CreatedOnField,
                    public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<FieldCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<FieldCommit>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "FieldCommit";

    FieldCommit(ID_TYPE collection_id, ID_TYPE field_id, const MappingT& mappings = {}, ID_TYPE id = 0,
                LSN_TYPE lsn = 0, State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp(),
                TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
};

using FieldCommitPtr = FieldCommit::Ptr;

///////////////////////////////////////////////////////////////////////////////

class FieldElement : public BaseResource<FieldElement>,
                     public CollectionIdField,
                     public FieldIdField,
                     public NameField,
                     public FtypeField,
                     public ParamsField,
                     public IdField,
                     public LsnField,
                     public StateField,
                     public CreatedOnField,
                     public UpdatedOnField {
 public:
    using Ptr = std::shared_ptr<FieldElement>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using SetT = std::set<Ptr>;
    using ScopedMapT = std::map<ID_TYPE, ScopedResource<FieldElement>>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "FieldElement";
    FieldElement(ID_TYPE collection_id, ID_TYPE field_id, const std::string& name, FTYPE_TYPE ftype,
                 const json& params = JEmpty, ID_TYPE id = 0, LSN_TYPE lsn = 0, State status = PENDING,
                 TS_TYPE created_on = GetMicroSecTimeStamp(), TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
};

using FieldElementPtr = FieldElement::Ptr;

}  // namespace milvus::engine::snapshot
