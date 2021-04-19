#include "Collection.h"

namespace milvus::dog_segment {

Collection::Collection(std::string &collection_name, std::string &schema):
      collection_name_(collection_name), schema_json_(schema){}

void
Collection::set_index() {}

void
Collection::parse() {}

void
Collection::AddNewPartition() {}

}
