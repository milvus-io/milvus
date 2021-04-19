#pragma once

#include "common/Schema.h"
#include "IndexMeta.h"

namespace milvus::segcore {

class Collection {
 public:
    explicit Collection(const std::string& collection_proto);

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

    const std::string&
    get_collection_name() {
        return collection_name_;
    }

 private:
    IndexMetaPtr index_;
    std::string collection_name_;
    std::string collection_proto_;
    SchemaPtr schema_;
};

using CollectionPtr = std::unique_ptr<Collection>;

}  // namespace milvus::segcore
