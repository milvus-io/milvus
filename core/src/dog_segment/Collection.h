#pragma once

#include "dog_segment/Partition.h"
#include "SegmentDefs.h"

namespace milvus::dog_segment {

class Collection {
public:
    explicit Collection(std::string &collection_name, std::string &schema);

    // TODO: set index
    void set_index();

    // TODO: config to schema
    void parse();

public:
    SchemaPtr& get_schema() {
      return schema_;
    }

    std::string& get_collection_name() {
      return collection_name_;
    }

private:
    // TODO: add Index ptr
    // IndexPtr index_ = nullptr;
    std::string collection_name_;
    std::string schema_json_;
    SchemaPtr schema_;
};

using CollectionPtr = std::unique_ptr<Collection>;

}
