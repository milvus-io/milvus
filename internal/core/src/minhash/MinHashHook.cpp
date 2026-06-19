// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "MinHashHook.h"

#include "fusion_compute/fusion_compute_native.h"
#include "glog/logging.h"
#include "log/Log.h"

#if defined(__x86_64__) || defined(_M_X64)
#include "fusion_compute/x86/fusion_compute_avx2.h"
#include "fusion_compute/x86/fusion_compute_avx512.h"
#ifdef _MSC_VER
#include <intrin.h>
#else
#include <cpuid.h>
#endif
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#include "fusion_compute/arm/fusion_compute_neon.h"
#ifndef __APPLE__
// SVE is only supported on Linux aarch64, not on macOS (Apple Silicon doesn't have SVE)
#include "fusion_compute/arm/fusion_compute_sve.h"
#endif
#ifdef _MSC_VER
#include <processthreadsapi.h>
#elif defined(__APPLE__)
// macOS doesn't have sys/auxv.h - use sysctlbyname for CPU feature detection
#include <sys/sysctl.h>
#else
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif
#endif

namespace milvus {
namespace minhash {

// Initialize function pointers with native (fallback) implementations
LinearAndFindMinFunc linear_and_find_min_impl = linear_and_find_min_native;
LinearAndFindMinBatch8Func linear_and_find_min_batch8_impl =
    linear_and_find_min_batch8_native;

namespace {

#if defined(__x86_64__) || defined(_M_X64)
bool
cpu_support_avx512() {
#ifdef _MSC_VER
    int cpuInfo[4];
    __cpuidex(cpuInfo, 7, 0);
    int ebx = cpuInfo[1];
    // Check AVX512F (bit 16), AVX512DQ (bit 17), AVX512BW (bit 30)
    return (ebx & (1 << 16)) && (ebx & (1 << 17)) && (ebx & (1 << 30));
#else
    unsigned int eax, ebx, ecx, edx;
    if (!__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        return false;
    }
    // Check AVX512F (bit 16), AVX512DQ (bit 17), AVX512BW (bit 30)
    return (ebx & (1 << 16)) && (ebx & (1 << 17)) && (ebx & (1 << 30));
#endif
}

bool
cpu_support_avx2() {
#ifdef _MSC_VER
    int cpuInfo[4];
    __cpuidex(cpuInfo, 7, 0);
    int ebx = cpuInfo[1];
    return (ebx & (1 << 5));  // AVX2 bit
#else
    unsigned int eax, ebx, ecx, edx;
    if (!__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        return false;
    }
    return (ebx & (1 << 5));  // AVX2 bit
#endif
}
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
bool
cpu_support_sve() {
#ifdef _MSC_VER
    // Windows ARM doesn't expose SVE via IsProcessorFeaturePresent
    return false;
#elif defined(__APPLE__)
    // Apple Silicon doesn't support SVE
    return false;
#else
    unsigned long hwcap = getauxval(AT_HWCAP);
    return (hwcap & HWCAP_SVE) != 0;
#endif
}

bool
cpu_support_neon() {
#ifdef _MSC_VER
    return IsProcessorFeaturePresent(PF_ARM_V8_INSTRUCTIONS_AVAILABLE);
#elif defined(__APPLE__)
    // All Apple Silicon chips support NEON (ASIMD)
    return true;
#else
    unsigned long hwcap = getauxval(AT_HWCAP);
    return (hwcap & HWCAP_ASIMD) != 0;
#endif
}
#endif

}  // anonymous namespace

void
minhash_hook_init() {
    static bool initialized = false;
    if (initialized) {
        return;
    }
    initialized = true;

#if defined(__x86_64__) || defined(_M_X64)
    if (cpu_support_avx512()) {
        linear_and_find_min_impl = linear_and_find_min_avx512;
        linear_and_find_min_batch8_impl = linear_and_find_min_batch8_avx512;
        LOG_INFO("MinHash initialized with AVX512 instruction set");
    } else if (cpu_support_avx2()) {
        linear_and_find_min_impl = linear_and_find_min_avx2;
        linear_and_find_min_batch8_impl = linear_and_find_min_batch8_avx2;
        LOG_INFO("MinHash initialized with AVX2 instruction set");
    } else {
        linear_and_find_min_impl = linear_and_find_min_native;
        linear_and_find_min_batch8_impl = linear_and_find_min_batch8_native;
        LOG_INFO("MinHash initialized with native (scalar) instruction set");
    }
#elif defined(__aarch64__) || defined(_M_ARM64)
#ifndef __APPLE__
    // SVE is only available on Linux aarch64
    if (cpu_support_sve()) {
        linear_and_find_min_impl = linear_and_find_min_sve;
        linear_and_find_min_batch8_impl = linear_and_find_min_batch8_sve;
        LOG_INFO("MinHash initialized with SVE instruction set");
    } else
#endif
        if (cpu_support_neon()) {
        linear_and_find_min_impl = linear_and_find_min_neon;
        linear_and_find_min_batch8_impl = linear_and_find_min_batch8_neon;
        LOG_INFO("MinHash initialized with NEON instruction set");
    } else {
        linear_and_find_min_impl = linear_and_find_min_native;
        linear_and_find_min_batch8_impl = linear_and_find_min_batch8_native;
        LOG_INFO("MinHash initialized with native (scalar) instruction set");
    }
#else
    linear_and_find_min_impl = linear_and_find_min_native;
    linear_and_find_min_batch8_impl = linear_and_find_min_batch8_native;
    LOG_INFO("MinHash initialized with native (scalar) instruction set");
#endif
}

}  // namespace minhash
}  // namespace milvus
