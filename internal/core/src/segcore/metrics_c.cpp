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
#include <memory>
#include <string>

#include "common/CGoCatch.h"
#include "common/FastMem.h"
#include "knowhere/prometheus_client.h"
#include "segcore/metrics_c.h"

char*
GetKnowhereMetrics() {
    // Returns NULL on failure. The Go caller converts with C.GoString, which
    // maps NULL to "" — metrics gathering degrades gracefully instead of an
    // exception crossing the C boundary (AssertInfo here would throw straight
    // through cgo and terminate the process).
    try {
        auto str = knowhere::prometheusClient->GetMetrics();
        auto len = str.length();
        char* res = (char*)malloc(len + 1);
        if (res == nullptr) {
            return nullptr;
        }
        milvus::fastmem::FastMemcpy(res, str.data(), len);
        res[len] = '\0';
        return res;
    }
    CGO_CATCH_AND_LOG("GetKnowhereMetrics")
    return nullptr;
}
