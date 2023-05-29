// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

// -*- c++ -*-

#include "hook.h"

#include <iostream>
#include <mutex>
#include <string>

#include "ref.h"
#include "log/Log.h"
#if defined(__x86_64__)
#include "avx2.h"
#include "avx512.h"
#include "sse2.h"
#include "sse4.h"
#include "instruction_set.h"
#endif

namespace milvus {
namespace simd {

#if defined(__x86_64__)
bool use_avx512 = true;
bool use_avx2 = true;
bool use_sse4_2 = true;
bool use_sse2 = true;

bool use_bitset_sse2;
bool use_find_term_sse2;
bool use_find_term_sse4_2;
bool use_find_term_avx2;
bool use_find_term_avx512;
#endif

decltype(get_bitset_block) get_bitset_block = GetBitsetBlockRef;
FindTermPtr<bool> find_term_bool = FindTermRef<bool>;
FindTermPtr<int8_t> find_term_int8 = FindTermRef<int8_t>;
FindTermPtr<int16_t> find_term_int16 = FindTermRef<int16_t>;
FindTermPtr<int32_t> find_term_int32 = FindTermRef<int32_t>;
FindTermPtr<int64_t> find_term_int64 = FindTermRef<int64_t>;
FindTermPtr<float> find_term_float = FindTermRef<float>;
FindTermPtr<double> find_term_double = FindTermRef<double>;

#if defined(__x86_64__)
bool
cpu_support_avx512() {
    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.AVX512F() && instruction_set_inst.AVX512DQ() &&
            instruction_set_inst.AVX512BW());
}

bool
cpu_support_avx2() {
    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.AVX2());
}

bool
cpu_support_sse4_2() {
    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.SSE42());
}

bool
cpu_support_sse2() {
    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.SSE2());
}
#endif

void
bitset_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        // For now, sse2 has best performance
        get_bitset_block = GetBitsetBlockSSE2;
        use_bitset_sse2 = true;
    } else if (use_avx2 && cpu_support_avx2()) {
        simd_type = "AVX2";
        // For now, sse2 has best performance
        get_bitset_block = GetBitsetBlockSSE2;
        use_bitset_sse2 = true;
    } else if (use_sse4_2 && cpu_support_sse4_2()) {
        simd_type = "SSE4";
        get_bitset_block = GetBitsetBlockSSE2;
        use_bitset_sse2 = true;
    } else if (use_sse2 && cpu_support_sse2()) {
        simd_type = "SSE2";
        get_bitset_block = GetBitsetBlockSSE2;
        use_bitset_sse2 = true;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "bitset hook simd type: " << simd_type;
}

void
find_term_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        find_term_bool = FindTermAVX512<bool>;
        find_term_int8 = FindTermAVX512<int8_t>;
        find_term_int16 = FindTermAVX512<int16_t>;
        find_term_int32 = FindTermAVX512<int32_t>;
        find_term_int64 = FindTermAVX512<int64_t>;
        find_term_float = FindTermAVX512<float>;
        find_term_double = FindTermAVX512<double>;
        use_find_term_avx512 = true;
    } else if (use_avx2 && cpu_support_avx2()) {
        simd_type = "AVX2";
        find_term_bool = FindTermAVX2<bool>;
        find_term_int8 = FindTermAVX2<int8_t>;
        find_term_int16 = FindTermAVX2<int16_t>;
        find_term_int32 = FindTermAVX2<int32_t>;
        find_term_int64 = FindTermAVX2<int64_t>;
        find_term_float = FindTermAVX2<float>;
        find_term_double = FindTermAVX2<double>;
        use_find_term_avx2 = true;
    } else if (use_sse4_2 && cpu_support_sse4_2()) {
        simd_type = "SSE4";
        find_term_bool = FindTermSSE4<bool>;
        find_term_int8 = FindTermSSE4<int8_t>;
        find_term_int16 = FindTermSSE4<int16_t>;
        find_term_int32 = FindTermSSE4<int32_t>;
        find_term_int64 = FindTermSSE4<int64_t>;
        find_term_float = FindTermSSE4<float>;
        find_term_double = FindTermSSE4<double>;
        use_find_term_sse4_2 = true;
    } else if (use_sse2 && cpu_support_sse2()) {
        simd_type = "SSE2";
        find_term_bool = FindTermSSE2<bool>;
        find_term_int8 = FindTermSSE2<int8_t>;
        find_term_int16 = FindTermSSE2<int16_t>;
        find_term_int32 = FindTermSSE2<int32_t>;
        find_term_int64 = FindTermSSE2<int64_t>;
        find_term_float = FindTermSSE2<float>;
        find_term_double = FindTermSSE2<double>;
        use_find_term_sse2 = true;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "find term hook simd type: " << simd_type;
}

static int init_hook_ = []() {
    bitset_hook();
    find_term_hook();
    return 0;
}();

}  // namespace simd
}  // namespace milvus
