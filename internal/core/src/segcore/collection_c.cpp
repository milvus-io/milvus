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

#include "common/type_c.h"
#ifdef __linux__
#include <malloc.h>
#endif

#include <iostream>
#include "segcore/collection_c.h"
#include "segcore/Collection.h"

CStatus
NewCollection(const void* schema_proto_blob,
              const int64_t length,
              CCollection* newCollection) {
    try {
        auto collection = std::make_unique<milvus::segcore::Collection>(
            schema_proto_blob, length);
        *newCollection = collection.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
SetIndexMeta(CCollection collection,
             const void* proto_blob,
             const int64_t length) {
    try {
        auto col = static_cast<milvus::segcore::Collection*>(collection);
        col->parseIndexMeta(proto_blob, length);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
DeleteCollection(CCollection collection) {
    auto col = static_cast<milvus::segcore::Collection*>(collection);
    delete col;
}

const char*
GetCollectionName(CCollection collection) {
    auto col = static_cast<milvus::segcore::Collection*>(collection);
    return strdup(col->get_collection_name().data());
}
