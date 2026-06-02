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

// ═══════════════════════════════════════════════════════════════════════════
// Runtime dispatcher + baseline SIMD implementation.
//
// Compiled with default flags (no extra -mavx2 etc.), so the baseline uses:
//   x86-64 → SSE2 (always available)
//   aarch64 → NEON (always available)
//
// At first call, detects CPU features and selects the best per-arch impl.
// ═══════════════════════════════════════════════════════════════════════════

#include "SimdFilter.h"
#include "SimdFilterImpl.h"

// ── CPU feature detection ───────────────────────────────────────────────

#if defined(__x86_64__)
#include <cpuid.h>

// Read XCR0 via XGETBV to check which SIMD state the OS has enabled.
// Without this, CPUID may report AVX/AVX-512 support but executing those
// instructions will SIGILL in VMs or containers where the OS hasn't
// enabled the corresponding state-save bits.
static unsigned int
xgetbv(unsigned int xcr) {
    unsigned int eax, edx;
    __asm__ volatile("xgetbv" : "=a"(eax), "=d"(edx) : "c"(xcr));
    return eax;
}

static bool
osSupportsAvx() {
    unsigned int eax, ebx, ecx, edx;
    // Check OSXSAVE (bit 27 of ECX from CPUID leaf 1)
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        return false;
    }
    if (!(ecx & (1u << 27))) {
        return false;  // XGETBV not enabled by OS
    }
    // XCR0 bit 1 (SSE state) + bit 2 (AVX state)
    unsigned int xcr0 = xgetbv(0);
    return (xcr0 & 0x6) == 0x6;
}

static bool
osSupportsAvx512() {
    if (!osSupportsAvx()) {
        return false;
    }
    // XCR0 bits 5 (opmask), 6 (ZMM_Hi256), 7 (Hi16_ZMM)
    unsigned int xcr0 = xgetbv(0);
    return (xcr0 & 0xE0) == 0xE0;
}

static bool
hasAvx512bw() {
    if (!osSupportsAvx512()) {
        return false;
    }
    unsigned int eax, ebx, ecx, edx;
    if (!__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        return false;
    }
    // Must match flags in CMakeLists.txt: -mavx512f -mavx512bw -mavx512vl -mavx512dq
    constexpr unsigned int kAvx512F = 1u << 16;
    constexpr unsigned int kAvx512DQ = 1u << 17;
    constexpr unsigned int kAvx512BW = 1u << 30;
    constexpr unsigned int kAvx512VL = 1u << 31;
    return (ebx & (kAvx512F | kAvx512DQ | kAvx512BW | kAvx512VL)) ==
           (kAvx512F | kAvx512DQ | kAvx512BW | kAvx512VL);
}

static bool
hasAvx2() {
    if (!osSupportsAvx()) {
        return false;
    }
    unsigned int eax, ebx, ecx, edx;
    if (!__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        return false;
    }
    return (ebx & (1u << 5));  // AVX2 = bit 5 of EBX
}

#endif

// ── Baseline implementation (SSE2 / NEON) ───────────────────────────────

namespace milvus {
namespace exec {
namespace baseline {

// Explicitly use the guaranteed-available baseline arch so that this code
// remains SSE2/NEON even if the project adds global -mavx2 flags.
#if defined(__x86_64__)
using Arch = xsimd::sse2;
#elif defined(__aarch64__)
using Arch = xsimd::neon64;
#else
using Arch = xsimd::default_arch;
#endif

template <typename T>
void
filterChunk(
    const T* data, int size, uint8_t* bitmap, const T* vals, int num_vals) {
    detail::filterChunkImpl<T, Arch>(data, size, bitmap, vals, num_vals);
}

template <typename T>
int
laneCount() {
    return xsimd::batch<T, Arch>::size;
}

#define INSTANTIATE(T)                                                    \
    template void filterChunk<T>(const T*, int, uint8_t*, const T*, int); \
    template int laneCount<T>();

INSTANTIATE(int8_t)
INSTANTIATE(uint8_t)
INSTANTIATE(int16_t)
INSTANTIATE(int32_t)
INSTANTIATE(int64_t)
INSTANTIATE(float)
INSTANTIATE(double)
#undef INSTANTIATE

}  // namespace baseline

// ── Forward declarations for higher-tier implementations ────────────────

#if defined(__x86_64__)
namespace avx2 {
template <typename T>
void
filterChunk(const T*, int, uint8_t*, const T*, int);
template <typename T>
int
laneCount();
}  // namespace avx2
namespace avx512 {
template <typename T>
void
filterChunk(const T*, int, uint8_t*, const T*, int);
template <typename T>
int
laneCount();
}  // namespace avx512
#endif

// ── Dispatcher ──────────────────────────────────────────────────────────

namespace {

template <typename T>
using FilterFn = void (*)(const T*, int, uint8_t*, const T*, int);

template <typename T>
FilterFn<T>
selectFilterImpl() {
#if defined(__x86_64__)
    if (hasAvx512bw()) {
        return avx512::filterChunk<T>;
    }
    if (hasAvx2()) {
        return avx2::filterChunk<T>;
    }
#endif
    return baseline::filterChunk<T>;
}

template <typename T>
using LaneCountFn = int (*)();

template <typename T>
LaneCountFn<T>
selectLaneCountImpl() {
#if defined(__x86_64__)
    if (hasAvx512bw()) {
        return avx512::laneCount<T>;
    }
    if (hasAvx2()) {
        return avx2::laneCount<T>;
    }
#endif
    return baseline::laneCount<T>;
}

}  // anonymous namespace

// ── Public API ──────────────────────────────────────────────────────────

template <typename T>
void
simdFilterChunk(
    const T* data, int size, uint8_t* bitmap, const T* vals, int num_vals) {
    // Resolved once on first call, cached in static local.
    static const auto impl = selectFilterImpl<T>();
    impl(data, size, bitmap, vals, num_vals);
}

template <typename T>
int
simdLaneCount() {
    static const auto impl = selectLaneCountImpl<T>();
    return impl();
}

// Explicit instantiations for all supported numeric types.
#define INSTANTIATE(T)                                                        \
    template void simdFilterChunk<T>(const T*, int, uint8_t*, const T*, int); \
    template int simdLaneCount<T>();

INSTANTIATE(int8_t)
INSTANTIATE(uint8_t)
INSTANTIATE(int16_t)
INSTANTIATE(int32_t)
INSTANTIATE(int64_t)
INSTANTIATE(float)
INSTANTIATE(double)
#undef INSTANTIATE

}  // namespace exec
}  // namespace milvus
