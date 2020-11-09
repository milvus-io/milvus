#include <iostream>
#include "collection_c.h"
#include "Collection.h"

CCollection
NewCollection(const char* collection_proto) {
    auto proto = std::string(collection_proto);

    auto collection = std::make_unique<milvus::segcore::Collection>(proto);

    return (void*)collection.release();
}

void
DeleteCollection(CCollection collection) {
    auto col = (milvus::segcore::Collection*)collection;

    // TODO: delete print
    std::cout << "delete collection " << col->get_collection_name() << std::endl;
    delete col;
}
