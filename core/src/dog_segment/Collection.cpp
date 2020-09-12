#include "Collection.h"

namespace milvus::dog_segment {

Collection::Collection(std::string &collection_name, std::string &schema):
      collection_name_(collection_name), schema_json_(schema) {
    parse();
}

void
Collection::set_index() {}

void
Collection::parse() {
    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    schema->AddField("age", DataType::INT32);
    schema_ = schema;
}

}

