#include "collection_c.h"
#include "Collection.h"

CCollection
NewCollection(const char* collection_name, const char* schema_conf) {
  auto name = std::string(collection_name);
  auto conf = std::string(schema_conf);

  auto collection = std::make_unique<milvus::dog_segment::Collection>(name, conf);

  // TODO: delete print
  std::cout << "create collection " << collection_name << std::endl;
  return (void*)collection.release();
}

void
DeleteCollection(CCollection collection) {
  auto col = (milvus::dog_segment::Collection*)collection;

  // TODO: delete print
  std::cout << "delete collection " << col->get_collection_name() << std::endl;
  delete col;
}
