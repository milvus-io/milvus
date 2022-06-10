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

#ifndef __APPLE__
#include <malloc.h>
#endif

#include <iostream>
#include "common/CGoHelper.h"
#include "common/type_c.h"
#include "segcore/collection_c.h"
#include "segcore/Collection.h"

CStatus
NewCollection(const char* schema_proto_blob, CCollection* c_collection) {
    try {
        auto proto = std::string(schema_proto_blob);
        auto collection = std::make_unique<milvus::segcore::Collection>(proto);
        *c_collection = collection.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DeleteCollection(CCollection collection) {
    try {
        auto col = reinterpret_cast<milvus::segcore::Collection*>(collection);
        delete col;
#ifdef __linux__
        malloc_trim(0);
#endif
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}
