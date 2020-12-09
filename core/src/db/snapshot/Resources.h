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
#include "db/snapshot/Fields.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/ScopedResource.h"
#include "utils/Json.h"
#include "utils/Status.h"

using milvus::engine::utils::GetMicroSecTimeStamp;

namespace milvus::engine::snapshot {

static const json JEmpty = {};



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
                         public FlushableMappingsField,
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
                        public FlushableMappingsField,
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
                    public FEtypeField,
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
                FETYPE_TYPE fetype, SIZE_TYPE row_cnt = 0, SIZE_TYPE size = 0, ID_TYPE id = 0, LSN_TYPE lsn = 0,
                State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp(),
                TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
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
                     public FEtypeField,
                     public TypeNameField,
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
    FieldElement(ID_TYPE collection_id, ID_TYPE field_id, const std::string& name, FETYPE_TYPE fetype,
                 const std::string& type_name = "", const json& params = JEmpty, ID_TYPE id = 0, LSN_TYPE lsn = 0,
                 State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp(),
                 TS_TYPE UpdatedOnField = GetMicroSecTimeStamp());
};

using FieldElementPtr = FieldElement::Ptr;

}  // namespace milvus::engine::snapshot
