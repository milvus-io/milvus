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

#include <cstring>

#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace milvus {
namespace knowhere {

// TODO(linxj): Get From Config File
static size_t magic_num = 2;

size_t
MemoryIOWriter::operator()(const void* ptr, size_t size, size_t nitems) {
    auto total_need = size * nitems + rp;

    if (!data_) {  // data == nullptr
        total = total_need * magic_num;
        rp = size * nitems;
        data_ = new uint8_t[total];
        memcpy(data_, ptr, rp);
        return nitems;
    }

    if (total_need > total) {
        total = total_need * magic_num;
        auto new_data = new uint8_t[total];
        memcpy(new_data, data_, rp);
        delete[] data_;
        data_ = new_data;

        memcpy((data_ + rp), ptr, size * nitems);
        rp = total_need;
    } else {
        memcpy((data_ + rp), ptr, size * nitems);
        rp = total_need;
    }

    return nitems;
}

size_t
MemoryIOReader::operator()(void* ptr, size_t size, size_t nitems) {
    if (rp >= total) {
        return 0;
    }
    size_t nremain = (total - rp) / size;
    if (nremain < nitems) {
        nitems = nremain;
    }
    memcpy(ptr, (data_ + rp), size * nitems);
    rp += size * nitems;
    return nitems;
}

}  // namespace knowhere
}  // namespace milvus
