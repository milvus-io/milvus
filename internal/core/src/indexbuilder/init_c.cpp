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

#include <stdlib.h>
#include <string.h>
#include <string>

#include "common/CGoCatch.h"
#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "config/ConfigKnowhere.h"
#include "indexbuilder/init_c.h"

void
IndexBuilderInit(const char* conf_file) {
    try {
        milvus::config::KnowhereInitImpl(conf_file);
    }
    CGO_CATCH_AND_LOG("IndexBuilderInit")
}

// return value must be freed by the caller
char*
IndexBuilderSetSimdType(const char* value) {
    // Returns NULL on failure; the Go caller ignores the return value, so a
    // swallowed exception degrades to keeping the default SIMD type instead
    // of an exception crossing the C boundary (AssertInfo here would throw
    // straight through cgo and terminate the process).
    try {
        auto real_type = milvus::config::KnowhereSetSimdType(value);
        char* ret = reinterpret_cast<char*>(malloc(real_type.length() + 1));
        if (ret == nullptr) {
            return nullptr;
        }
        milvus::fastmem::FastMemcpy(ret, real_type.c_str(), real_type.length());
        ret[real_type.length()] = 0;
        return ret;
    }
    CGO_CATCH_AND_LOG("IndexBuilderSetSimdType")
    return nullptr;
}
