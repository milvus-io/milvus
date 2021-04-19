#pragma once
#include <vector>

// #include "db/Types.h"
#include "dog_segment/SegmentDefs.h"
// #include "knowhere/index/Index.h"
#include "query/GeneralQuery.h"
using idx_t = int64_t;

namespace milvus {
namespace dog_segment {
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
    // single threaded
    virtual Status
    Insert(int64_t size, const idx_t* primary_keys, const Timestamp* timestamps, const DogDataChunk& values, std::pair<Timestamp, Timestamp> timestamp_range) = 0;

    // TODO: add id into delete log, possibly bitmap
    // single threaded
    virtual Status
    Delete(int64_t size, const idx_t* primary_keys, const Timestamp* timestamps, std::pair<Timestamp, Timestamp> timestamp_range) = 0;

    // query contains metadata of
    // multi-threaded
    virtual Status
    Query(const query::QueryPtr& query, Timestamp timestamp, QueryResult& results) = 0;

    // // THIS FUNCTION IS REMOVED
    // virtual Status
    // GetEntityByIds(Timestamp timestamp, const std::vector<Id>& ids, DataChunkPtr& results) = 0;

    // stop receive insert requests
    // single threaded
    virtual Status
    Close() = 0;

    //    // to make all data inserted visible
    //    // maybe a no-op?
    //    virtual Status
    //    Flush(Timestamp timestamp) = 0;

    // BuildIndex With Paramaters, must with Frozen State
    // This function is atomic
    // NOTE: index_params contains serveral policies for several index
    virtual Status
    BuildIndex(std::shared_ptr<IndexConfig> index_params) = 0;

    // Remove Index
    virtual Status
    DropIndex(std::string_view field_name) = 0;

    virtual Status
    DropRawData(std::string_view field_name) = 0;

    virtual Status
    LoadRawData(std::string_view field_name, const char* blob, int64_t blob_size) = 0;

 public:
    virtual ssize_t
    get_row_count() const = 0;

    virtual SegmentState
    get_state() const = 0;

    virtual ssize_t
    get_deleted_count() const = 0;

};

using SegmentBasePtr = std::unique_ptr<SegmentBase>;

SegmentBasePtr CreateSegment(SchemaPtr& ptr);

}  // namespace engine
}  // namespace milvus
