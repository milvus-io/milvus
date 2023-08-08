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
#endif

decltype(get_bitset_block) get_bitset_block = GetBitsetBlockRef;

#define DECLARE_FIND_TERM_PTR(type) \
    FindTermPtr<type> find_term_##type = FindTermRef<type>;
DECLARE_FIND_TERM_PTR(bool)
DECLARE_FIND_TERM_PTR(int8_t)
DECLARE_FIND_TERM_PTR(int16_t)
DECLARE_FIND_TERM_PTR(int32_t)
DECLARE_FIND_TERM_PTR(int64_t)
DECLARE_FIND_TERM_PTR(float)
DECLARE_FIND_TERM_PTR(double)

#define DECLARE_COMPARE_VAL_PTR(prefix, RefFunc, type) \
    CompareValPtr<type> prefix##_##type = RefFunc<type>;

DECLARE_COMPARE_VAL_PTR(equal_val, EqualValRef, bool)
DECLARE_COMPARE_VAL_PTR(equal_val, EqualValRef, int8_t)
DECLARE_COMPARE_VAL_PTR(equal_val, EqualValRef, int16_t)
DECLARE_COMPARE_VAL_PTR(equal_val, EqualValRef, int32_t)
DECLARE_COMPARE_VAL_PTR(equal_val, EqualValRef, int64_t)
DECLARE_COMPARE_VAL_PTR(equal_val, EqualValRef, float)
DECLARE_COMPARE_VAL_PTR(equal_val, EqualValRef, double)

DECLARE_COMPARE_VAL_PTR(less_val, LessValRef, bool)
DECLARE_COMPARE_VAL_PTR(less_val, LessValRef, int8_t)
DECLARE_COMPARE_VAL_PTR(less_val, LessValRef, int16_t)
DECLARE_COMPARE_VAL_PTR(less_val, LessValRef, int32_t)
DECLARE_COMPARE_VAL_PTR(less_val, LessValRef, int64_t)
DECLARE_COMPARE_VAL_PTR(less_val, LessValRef, float)
DECLARE_COMPARE_VAL_PTR(less_val, LessValRef, double)

DECLARE_COMPARE_VAL_PTR(greater_val, GreaterValRef, bool)
DECLARE_COMPARE_VAL_PTR(greater_val, GreaterValRef, int8_t)
DECLARE_COMPARE_VAL_PTR(greater_val, GreaterValRef, int16_t)
DECLARE_COMPARE_VAL_PTR(greater_val, GreaterValRef, int32_t)
DECLARE_COMPARE_VAL_PTR(greater_val, GreaterValRef, int64_t)
DECLARE_COMPARE_VAL_PTR(greater_val, GreaterValRef, float)
DECLARE_COMPARE_VAL_PTR(greater_val, GreaterValRef, double)

DECLARE_COMPARE_VAL_PTR(less_equal_val, LessEqualValRef, bool)
DECLARE_COMPARE_VAL_PTR(less_equal_val, LessEqualValRef, int8_t)
DECLARE_COMPARE_VAL_PTR(less_equal_val, LessEqualValRef, int16_t)
DECLARE_COMPARE_VAL_PTR(less_equal_val, LessEqualValRef, int32_t)
DECLARE_COMPARE_VAL_PTR(less_equal_val, LessEqualValRef, int64_t)
DECLARE_COMPARE_VAL_PTR(less_equal_val, LessEqualValRef, float)
DECLARE_COMPARE_VAL_PTR(less_equal_val, LessEqualValRef, double)

DECLARE_COMPARE_VAL_PTR(greater_equal_val, GreaterEqualValRef, bool)
DECLARE_COMPARE_VAL_PTR(greater_equal_val, GreaterEqualValRef, int8_t)
DECLARE_COMPARE_VAL_PTR(greater_equal_val, GreaterEqualValRef, int16_t)
DECLARE_COMPARE_VAL_PTR(greater_equal_val, GreaterEqualValRef, int32_t)
DECLARE_COMPARE_VAL_PTR(greater_equal_val, GreaterEqualValRef, int64_t)
DECLARE_COMPARE_VAL_PTR(greater_equal_val, GreaterEqualValRef, float)
DECLARE_COMPARE_VAL_PTR(greater_equal_val, GreaterEqualValRef, double)

DECLARE_COMPARE_VAL_PTR(not_equal_val, NotEqualValRef, bool)
DECLARE_COMPARE_VAL_PTR(not_equal_val, NotEqualValRef, int8_t)
DECLARE_COMPARE_VAL_PTR(not_equal_val, NotEqualValRef, int16_t)
DECLARE_COMPARE_VAL_PTR(not_equal_val, NotEqualValRef, int32_t)
DECLARE_COMPARE_VAL_PTR(not_equal_val, NotEqualValRef, int64_t)
DECLARE_COMPARE_VAL_PTR(not_equal_val, NotEqualValRef, float)
DECLARE_COMPARE_VAL_PTR(not_equal_val, NotEqualValRef, double)

#define DECLARE_COMPARE_COL_PTR(prefix, RefFunc, type) \
    CompareColPtr<type> prefix##_##type = RefFunc<type>;

DECLARE_COMPARE_COL_PTR(equal_col, EqualColumnRef, bool)
DECLARE_COMPARE_COL_PTR(equal_col, EqualColumnRef, int8_t)
DECLARE_COMPARE_COL_PTR(equal_col, EqualColumnRef, int16_t)
DECLARE_COMPARE_COL_PTR(equal_col, EqualColumnRef, int32_t)
DECLARE_COMPARE_COL_PTR(equal_col, EqualColumnRef, int64_t)
DECLARE_COMPARE_COL_PTR(equal_col, EqualColumnRef, float)
DECLARE_COMPARE_COL_PTR(equal_col, EqualColumnRef, double)

DECLARE_COMPARE_COL_PTR(less_col, LessColumnRef, bool)
DECLARE_COMPARE_COL_PTR(less_col, LessColumnRef, int8_t)
DECLARE_COMPARE_COL_PTR(less_col, LessColumnRef, int16_t)
DECLARE_COMPARE_COL_PTR(less_col, LessColumnRef, int32_t)
DECLARE_COMPARE_COL_PTR(less_col, LessColumnRef, int64_t)
DECLARE_COMPARE_COL_PTR(less_col, LessColumnRef, float)
DECLARE_COMPARE_COL_PTR(less_col, LessColumnRef, double)

DECLARE_COMPARE_COL_PTR(greater_col, GreaterColumnRef, bool)
DECLARE_COMPARE_COL_PTR(greater_col, GreaterColumnRef, int8_t)
DECLARE_COMPARE_COL_PTR(greater_col, GreaterColumnRef, int16_t)
DECLARE_COMPARE_COL_PTR(greater_col, GreaterColumnRef, int32_t)
DECLARE_COMPARE_COL_PTR(greater_col, GreaterColumnRef, int64_t)
DECLARE_COMPARE_COL_PTR(greater_col, GreaterColumnRef, float)
DECLARE_COMPARE_COL_PTR(greater_col, GreaterColumnRef, double)

DECLARE_COMPARE_COL_PTR(less_equal_col, LessEqualColumnRef, bool)
DECLARE_COMPARE_COL_PTR(less_equal_col, LessEqualColumnRef, int8_t)
DECLARE_COMPARE_COL_PTR(less_equal_col, LessEqualColumnRef, int16_t)
DECLARE_COMPARE_COL_PTR(less_equal_col, LessEqualColumnRef, int32_t)
DECLARE_COMPARE_COL_PTR(less_equal_col, LessEqualColumnRef, int64_t)
DECLARE_COMPARE_COL_PTR(less_equal_col, LessEqualColumnRef, float)
DECLARE_COMPARE_COL_PTR(less_equal_col, LessEqualColumnRef, double)

DECLARE_COMPARE_COL_PTR(greater_equal_col, GreaterEqualColumnRef, bool)
DECLARE_COMPARE_COL_PTR(greater_equal_col, GreaterEqualColumnRef, int8_t)
DECLARE_COMPARE_COL_PTR(greater_equal_col, GreaterEqualColumnRef, int16_t)
DECLARE_COMPARE_COL_PTR(greater_equal_col, GreaterEqualColumnRef, int32_t)
DECLARE_COMPARE_COL_PTR(greater_equal_col, GreaterEqualColumnRef, int64_t)
DECLARE_COMPARE_COL_PTR(greater_equal_col, GreaterEqualColumnRef, float)
DECLARE_COMPARE_COL_PTR(greater_equal_col, GreaterEqualColumnRef, double)

DECLARE_COMPARE_COL_PTR(not_equal_col, NotEqualColumnRef, bool)
DECLARE_COMPARE_COL_PTR(not_equal_col, NotEqualColumnRef, int8_t)
DECLARE_COMPARE_COL_PTR(not_equal_col, NotEqualColumnRef, int16_t)
DECLARE_COMPARE_COL_PTR(not_equal_col, NotEqualColumnRef, int32_t)
DECLARE_COMPARE_COL_PTR(not_equal_col, NotEqualColumnRef, int64_t)
DECLARE_COMPARE_COL_PTR(not_equal_col, NotEqualColumnRef, float)
DECLARE_COMPARE_COL_PTR(not_equal_col, NotEqualColumnRef, double)

#if defined(__x86_64__)
bool
cpu_support_avx512() {
    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.AVX512F() && instruction_set_inst.AVX512DQ() &&
            instruction_set_inst.AVX512BW() && instruction_set_inst.AVX512VL());
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
    } else if (use_avx2 && cpu_support_avx2()) {
        simd_type = "AVX2";
        // For now, sse2 has best performance
        get_bitset_block = GetBitsetBlockAVX2;
    } else if (use_sse4_2 && cpu_support_sse4_2()) {
        simd_type = "SSE4";
        get_bitset_block = GetBitsetBlockSSE2;
    } else if (use_sse2 && cpu_support_sse2()) {
        simd_type = "SSE2";
        get_bitset_block = GetBitsetBlockSSE2;
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
        find_term_int8_t = FindTermAVX512<int8_t>;
        find_term_int16_t = FindTermAVX512<int16_t>;
        find_term_int32_t = FindTermAVX512<int32_t>;
        find_term_int64_t = FindTermAVX512<int64_t>;
        find_term_float = FindTermAVX512<float>;
        find_term_double = FindTermAVX512<double>;
    } else if (use_avx2 && cpu_support_avx2()) {
        simd_type = "AVX2";
        find_term_bool = FindTermAVX2<bool>;
        find_term_int8_t = FindTermAVX2<int8_t>;
        find_term_int16_t = FindTermAVX2<int16_t>;
        find_term_int32_t = FindTermAVX2<int32_t>;
        find_term_int64_t = FindTermAVX2<int64_t>;
        find_term_float = FindTermAVX2<float>;
        find_term_double = FindTermAVX2<double>;
    } else if (use_sse4_2 && cpu_support_sse4_2()) {
        simd_type = "SSE4";
        find_term_bool = FindTermSSE4<bool>;
        find_term_int8_t = FindTermSSE4<int8_t>;
        find_term_int16_t = FindTermSSE4<int16_t>;
        find_term_int32_t = FindTermSSE4<int32_t>;
        find_term_int64_t = FindTermSSE4<int64_t>;
        find_term_float = FindTermSSE4<float>;
        find_term_double = FindTermSSE4<double>;
    } else if (use_sse2 && cpu_support_sse2()) {
        simd_type = "SSE2";
        find_term_bool = FindTermSSE2<bool>;
        find_term_int8_t = FindTermSSE2<int8_t>;
        find_term_int16_t = FindTermSSE2<int16_t>;
        find_term_int32_t = FindTermSSE2<int32_t>;
        find_term_int64_t = FindTermSSE2<int64_t>;
        find_term_float = FindTermSSE2<float>;
        find_term_double = FindTermSSE2<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "find term hook simd type: " << simd_type;
}

void
equal_val_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        equal_val_int8_t = EqualValAVX512<int8_t>;
        equal_val_int16_t = EqualValAVX512<int16_t>;
        equal_val_int32_t = EqualValAVX512<int32_t>;
        equal_val_int64_t = EqualValAVX512<int64_t>;
        equal_val_float = EqualValAVX512<float>;
        equal_val_double = EqualValAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "equal val hook simd type: " << simd_type;
}

void
less_val_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        less_val_int8_t = LessValAVX512<int8_t>;
        less_val_int16_t = LessValAVX512<int16_t>;
        less_val_int32_t = LessValAVX512<int32_t>;
        less_val_int64_t = LessValAVX512<int64_t>;
        less_val_float = LessValAVX512<float>;
        less_val_double = LessValAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "less than val hook simd type: " << simd_type;
}

void
greater_val_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        greater_val_int8_t = GreaterValAVX512<int8_t>;
        greater_val_int16_t = GreaterValAVX512<int16_t>;
        greater_val_int32_t = GreaterValAVX512<int32_t>;
        greater_val_int64_t = GreaterValAVX512<int64_t>;
        greater_val_float = GreaterValAVX512<float>;
        greater_val_double = GreaterValAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "greater than val hook simd type: " << simd_type;
}

void
less_equal_val_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        less_equal_val_int8_t = LessEqualValAVX512<int8_t>;
        less_equal_val_int16_t = LessEqualValAVX512<int16_t>;
        less_equal_val_int32_t = LessEqualValAVX512<int32_t>;
        less_equal_val_int64_t = LessEqualValAVX512<int64_t>;
        less_equal_val_float = LessEqualValAVX512<float>;
        less_equal_val_double = LessEqualValAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "less equal than val hook simd type: " << simd_type;
}

void
greater_equal_val_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        greater_equal_val_int8_t = GreaterEqualValAVX512<int8_t>;
        greater_equal_val_int16_t = GreaterEqualValAVX512<int16_t>;
        greater_equal_val_int32_t = GreaterEqualValAVX512<int32_t>;
        greater_equal_val_int64_t = GreaterEqualValAVX512<int64_t>;
        greater_equal_val_float = GreaterEqualValAVX512<float>;
        greater_equal_val_double = GreaterEqualValAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "greater equal than val hook simd type: " << simd_type;
}

void
not_equal_val_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        not_equal_val_int8_t = NotEqualValAVX512<int8_t>;
        not_equal_val_int16_t = NotEqualValAVX512<int16_t>;
        not_equal_val_int32_t = NotEqualValAVX512<int32_t>;
        not_equal_val_int64_t = NotEqualValAVX512<int64_t>;
        not_equal_val_float = NotEqualValAVX512<float>;
        not_equal_val_double = NotEqualValAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "not equal val hook simd type: " << simd_type;
}

void
equal_col_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        equal_col_int8_t = EqualColumnAVX512<int8_t>;
        equal_col_int16_t = EqualColumnAVX512<int16_t>;
        equal_col_int32_t = EqualColumnAVX512<int32_t>;
        equal_col_int64_t = EqualColumnAVX512<int64_t>;
        equal_col_float = EqualColumnAVX512<float>;
        equal_col_double = EqualColumnAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "equal column hook simd type: " << simd_type;
}

void
less_col_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        less_col_int8_t = LessColumnAVX512<int8_t>;
        less_col_int16_t = LessColumnAVX512<int16_t>;
        less_col_int32_t = LessColumnAVX512<int32_t>;
        less_col_int64_t = LessColumnAVX512<int64_t>;
        less_col_float = LessColumnAVX512<float>;
        less_col_double = LessColumnAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "less than column hook simd type: " << simd_type;
}

void
greater_col_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        greater_col_int8_t = GreaterColumnAVX512<int8_t>;
        greater_col_int16_t = GreaterColumnAVX512<int16_t>;
        greater_col_int32_t = GreaterColumnAVX512<int32_t>;
        greater_col_int64_t = GreaterColumnAVX512<int64_t>;
        greater_col_float = GreaterColumnAVX512<float>;
        greater_col_double = GreaterColumnAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "greater than column hook simd type: " << simd_type;
}

void
less_equal_col_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        less_equal_col_int8_t = LessEqualColumnAVX512<int8_t>;
        less_equal_col_int16_t = LessEqualColumnAVX512<int16_t>;
        less_equal_col_int32_t = LessEqualColumnAVX512<int32_t>;
        less_equal_col_int64_t = LessEqualColumnAVX512<int64_t>;
        less_equal_col_float = LessEqualColumnAVX512<float>;
        less_equal_col_double = LessEqualColumnAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "less equal than column hook simd type: " << simd_type;
}

void
greater_equal_col_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        greater_equal_col_int8_t = GreaterEqualColumnAVX512<int8_t>;
        greater_equal_col_int16_t = GreaterEqualColumnAVX512<int16_t>;
        greater_equal_col_int32_t = GreaterEqualColumnAVX512<int32_t>;
        greater_equal_col_int64_t = GreaterEqualColumnAVX512<int64_t>;
        greater_equal_col_float = GreaterEqualColumnAVX512<float>;
        greater_equal_col_double = GreaterEqualColumnAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "greater equal than column hook simd type: "
                      << simd_type;
}

void
not_equal_col_hook() {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    std::string simd_type = "REF";
#if defined(__x86_64__)
    // Only support avx512 for now
    if (use_avx512 && cpu_support_avx512()) {
        simd_type = "AVX512";
        not_equal_col_int8_t = NotEqualColumnAVX512<int8_t>;
        not_equal_col_int16_t = NotEqualColumnAVX512<int16_t>;
        not_equal_col_int32_t = NotEqualColumnAVX512<int32_t>;
        not_equal_col_int64_t = NotEqualColumnAVX512<int64_t>;
        not_equal_col_float = NotEqualColumnAVX512<float>;
        not_equal_col_double = NotEqualColumnAVX512<double>;
    }
#endif
    // TODO: support arm cpu
    LOG_SEGCORE_INFO_ << "not equal column hook simd type: " << simd_type;
}

static int init_hook_ = []() {
    bitset_hook();
    find_term_hook();
    equal_val_hook();
    less_val_hook();
    greater_val_hook();
    less_equal_val_hook();
    greater_equal_val_hook();
    not_equal_val_hook();
    equal_col_hook();
    less_col_hook();
    greater_col_hook();
    less_equal_col_hook();
    greater_equal_col_hook();
    not_equal_col_hook();
    return 0;
}();

}  // namespace simd
}  // namespace milvus
