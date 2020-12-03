// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <iostream>
#include "segcore/collection_c.h"
#include "segcore/Collection.h"

CCollection
NewCollection(const char* collection_proto) {
    auto proto = std::string(collection_proto);

    auto collection = std::make_unique<milvus::segcore::Collection>(proto);

    // TODO: delete print
    std::cout << "create collection " << collection->get_collection_name() << std::endl;

    return (void*)collection.release();
}

void
DeleteCollection(CCollection collection) {
    auto col = (milvus::segcore::Collection*)collection;

    // TODO: delete print
    std::cout << "delete collection " << col->get_collection_name() << std::endl;
    delete col;
}
