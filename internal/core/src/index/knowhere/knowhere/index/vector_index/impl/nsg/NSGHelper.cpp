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

#include "knowhere/index/vector_index/impl/nsg/NSGHelper.h"

namespace milvus {
namespace knowhere {
namespace impl {

// TODO: impl search && insert && return insert pos. why not just find and swap?
int
InsertIntoPool(Neighbor* addr, unsigned K, Neighbor nn) {
    //>> Fix: Add assert
    //    for (unsigned int i = 0; i < K; ++i) {
    //        assert(addr[i].id != nn.id);
    //    }

    // find the location to insert
    int left = 0, right = K - 1;
    if (addr[left].distance > nn.distance) {
        //>> Fix: memmove overflow, dump when vector<Neighbor> deconstruct
        memmove(&addr[left + 1], &addr[left], (K - 1) * sizeof(Neighbor));
        addr[left] = nn;
        return left;
    }
    if (addr[right].distance < nn.distance) {
        addr[K] = nn;
        return K;
    }
    while (left < right - 1) {
        int mid = (left + right) / 2;
        if (addr[mid].distance > nn.distance) {
            right = mid;
        } else {
            left = mid;
        }
    }
    // check equal ID

    while (left > 0) {
        if (addr[left].distance < nn.distance) {  // pos is right
            break;
        }
        if (addr[left].id == nn.id) {
            return K + 1;
        }
        left--;
    }
    if (addr[left].id == nn.id || addr[right].id == nn.id) {
        return K + 1;
    }

    //>> Fix: memmove overflow, dump when vector<Neighbor> deconstruct
    memmove(&addr[right + 1], &addr[right], (K - 1 - right) * sizeof(Neighbor));
    addr[right] = nn;
    return right;
}

}  // namespace impl
}  // namespace knowhere
}  // namespace milvus
