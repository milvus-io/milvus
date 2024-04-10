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

#include "instruction_set.h"

#ifdef __linux__
#include <sys/auxv.h>
#endif

namespace milvus {
namespace bitset {
namespace detail {
namespace arm {

InstructionSet::InstructionSet() {
}

#ifdef __linux__

#if defined(HWCAP_SVE)
bool
InstructionSet::supports_sve() {
    const unsigned long cap = getauxval(AT_HWCAP);
    return ((cap & HWCAP_SVE) == HWCAP_SVE);
}
#else
bool
InstructionSet::supports_sve() {
    return false;
}
#endif

#else
bool
InstructionSet::supports_sve() {
    return false;
}
#endif

}  // namespace arm
}  // namespace detail
}  // namespace bitset
}  // namespace milvus
