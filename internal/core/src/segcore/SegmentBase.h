#pragma once
#include <vector>

#include "IndexMeta.h"
#include "utils/Types.h"
#include "segcore/SegmentDefs.h"
// #include "knowhere/index/Index.h"
// #include "knowhere/index/IndexType.h"
#include "query/GeneralQuery.h"

namespace milvus {
namespace segcore {
// using engine::DataChunk;
// using engine::DataChunkPtr;
using engine::QueryResult;

int
TestABI();

class SegmentBase {
 public:
    // definitions
    enum class SegmentState {
        Invalid = 0,
        Open,   // able to insert data
        Closed  // able to build index
    };

 public:
    virtual ~SegmentBase() = default;
    // SegmentBase(std::shared_ptr<FieldsInfo> collection);

    virtual int64_t
    PreInsert(int64_t size) = 0;

    virtual Status
    Insert(int64_t reserved_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           const RowBasedRawData& values) = 0;

    virtual int64_t
    PreDelete(int64_t size) = 0;
    // TODO: add id into delete log, possibly bitmap

    virtual Status
    Delete(int64_t reserved_offset, int64_t size, const int64_t* row_ids, const Timestamp* timestamps) = 0;

    // query contains metadata of
    virtual Status
    Query(query::QueryPtr query, Timestamp timestamp, QueryResult& results) = 0;

    // // THIS FUNCTION IS REMOVED
    // virtual Status
    // GetEntityByIds(Timestamp timestamp, const std::vector<Id>& ids, DataChunkPtr& results) = 0;

    // stop receive insert requests
    virtual Status
    Close() = 0;

    //    // to make all data inserted visible
    //    // maybe a no-op?
    //    virtual Status
    //    Flush(Timestamp timestamp) = 0;

    // watch changes
    // NOTE: Segment will use this ptr as correct

    virtual Status
    DropRawData(std::string_view field_name) = 0;

    virtual Status
    LoadRawData(std::string_view field_name, const char* blob, int64_t blob_size) = 0;

    virtual Status
    BuildIndex(IndexMetaPtr index_meta) = 0;

    virtual int64_t
    GetMemoryUsageInBytes() = 0;

 public:
    virtual ssize_t
    get_row_count() const = 0;

    virtual SegmentState
    get_state() const = 0;

    virtual ssize_t
    get_deleted_count() const = 0;
};

using SegmentBasePtr = std::unique_ptr<SegmentBase>;

SegmentBasePtr
CreateSegment(SchemaPtr schema);

}  // namespace segcore
}  // namespace milvus
