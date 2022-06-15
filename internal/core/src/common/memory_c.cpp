// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifdef __linux__
#include <malloc.h>
#include <rapidxml/rapidxml.hpp>
#endif

#include "common/CGoHelper.h"
#include "common/memory_c.h"
#include "exceptions/EasyAssert.h"
#include "log/Log.h"

void
DoMallocTrim() {
#ifdef __linux__
    malloc_trim(0);
#endif
}

uint64_t
ParseMallocInfo() {
#ifdef __linux__
    char* mem_buffer;
    size_t buffer_size;
    FILE* stream;
    stream = open_memstream(&mem_buffer, &buffer_size);
    // malloc_info(0, stdout);

    /*
     * The malloc_info() function exports an XML string that describes
     * the current state of the memory-allocation implementation in the caller.
     * The exported XML string includes information about `fast` and `rest`.
     * According to the implementation of glibc, `fast` calculates ths size of all the
     * fastbins, and `rest` calculates the size of all the bins except fastbins.
     * ref: <https://man7.org/linux/man-pages/man3/malloc_info.3.html>
     *      <https://sourceware.org/glibc/wiki/MallocInternals>
     *      <https://code.woboq.org/userspace/glibc/malloc/malloc.c.html#5378>
     */
    malloc_info(0, stream);
    fflush(stream);

    rapidxml::xml_document<> doc;  // character type defaults to char
    doc.parse<0>(mem_buffer);      // 0 means default parse flags

    rapidxml::xml_node<>* malloc_root_node = doc.first_node();
    auto total_fast_node = malloc_root_node->first_node()->next_sibling("total");
    AssertInfo(total_fast_node, "null total_fast_node detected when ParseMallocInfo");
    auto total_fast_size = std::stoul(total_fast_node->first_attribute("size")->value());

    auto total_rest_node = total_fast_node->next_sibling("total");
    AssertInfo(total_fast_node, "null total_rest_node detected when ParseMallocInfo");
    auto total_rest_size = std::stoul(total_rest_node->first_attribute("size")->value());

    fclose(stream);
    free(mem_buffer);

    return total_fast_size + total_rest_size;
#else
    return 0;  // malloc_trim is unnecessary
#endif
}

CStatus
PurgeMemory(uint64_t max_bins_size) {
    try {
        auto fast_and_rest_total = ParseMallocInfo();
        if (fast_and_rest_total >= max_bins_size) {
            LOG_SEGCORE_DEBUG_ << "Purge memory fragmentation, max_bins_size(bytes) = " << max_bins_size
                               << ", fast_and_rest_total(bytes) = " << fast_and_rest_total;
            DoMallocTrim();
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}
