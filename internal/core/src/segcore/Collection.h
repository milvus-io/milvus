#pragma once

#include "segcore/Partition.h"
#include "SegmentDefs.h"

namespace milvus::segcore {

class Collection {
 public:
    explicit Collection(std::string& collection_name, std::string& schema);

    void
    parse();

 public:
    SchemaPtr&
    get_schema() {
        return schema_;
    }

    IndexMetaPtr&
    get_index() {
        return index_;
    }

    std::string&
    get_collection_name() {
        return collection_name_;
    }

 private:
    IndexMetaPtr index_;
    std::string collection_name_;
    std::string schema_json_;
    SchemaPtr schema_;
};

using CollectionPtr = std::unique_ptr<Collection>;

}  // namespace milvus::segcore
