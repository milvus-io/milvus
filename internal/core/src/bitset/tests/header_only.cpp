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

#define BITSET_HEADER_ONLY
#include <type_traits>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/detail/element_wise.h"

int
main() {
    using Policy = milvus::bitset::detail::ElementWiseBitsetPolicy<uint64_t>;
    using Owner = milvus::bitset::Bitset<Policy, std::vector<uint8_t>, true>;
    Owner bits(32771, true);
    const auto view = bits.view();
    static_assert(std::is_same_v<decltype(view.data()), const uint64_t*>);
    if (view.count() != bits.size() || !view.all())
        return 1;
    bits.reset(5003);
    if (view.count() != bits.size() - 1 || view[5003] || view.all())
        return 2;
    Owner source(140, false);
    bits.inplace_and(source.view(7, 130), 130, 13);
    if (!view.view(13, 130).none() || !view[12] || !view[143])
        return 3;
    bits.flip(13, 130);
    if (!view.view(13, 130).all())
        return 4;
    return 0;
}
