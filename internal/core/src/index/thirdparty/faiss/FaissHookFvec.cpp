
// -*- c++ -*-

#include <iostream>
#include <mutex>

#include <faiss/FaissHookFvec.h>
#include <faiss/utils/distances_simd.h>
#include <faiss/utils/distances_simd_avx.h>
#include <faiss/utils/distances_simd_avx512.h>
#include <faiss/utils/instruction_set.h>

namespace faiss {

bool faiss_use_avx512 = true;
bool faiss_use_avx2 = true;
bool faiss_use_sse4_2 = true;

/* set default to AVX */
fvec_func_ptr fvec_inner_product = fvec_inner_product_avx;
fvec_func_ptr fvec_L2sqr = fvec_L2sqr_avx;
fvec_func_ptr fvec_L1 = fvec_L1_avx;
fvec_func_ptr fvec_Linf = fvec_Linf_avx;

/*****************************************************************************/

bool cpu_support_avx512() {
    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.AVX512F() &&
            instruction_set_inst.AVX512DQ() &&
            instruction_set_inst.AVX512BW());
}

bool cpu_support_avx2() {
    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.AVX2());
}

bool cpu_support_sse4_2() {
    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.SSE42());
}

void hook_fvec(std::string& simd_type) {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);

    // fvec hook can be controlled outside
    if (faiss_use_avx512 && cpu_support_avx512()) {
        fvec_inner_product = fvec_inner_product_avx512;
        fvec_L2sqr = fvec_L2sqr_avx512;
        fvec_L1 = fvec_L1_avx512;
        fvec_Linf = fvec_Linf_avx512;

        simd_type = "AVX512";
    } else if (faiss_use_avx2 && cpu_support_avx2()) {
        fvec_inner_product = fvec_inner_product_avx;
        fvec_L2sqr = fvec_L2sqr_avx;
        fvec_L1 = fvec_L1_avx;
        fvec_Linf = fvec_Linf_avx;

        simd_type = "AVX2";
    } else if (faiss_use_sse4_2 && cpu_support_sse4_2()) {
        fvec_inner_product = fvec_inner_product_sse;
        fvec_L2sqr = fvec_L2sqr_sse;
        fvec_L1 = fvec_L1_sse;
        fvec_Linf = fvec_Linf_sse;

        simd_type = "SSE4_2";
    } else {
        fvec_inner_product = fvec_inner_product_ref;
        fvec_L2sqr = fvec_L2sqr_ref;
        fvec_L1 = fvec_L1_ref;
        fvec_Linf = fvec_Linf_ref;

        simd_type = "REF";
    }
}

} // namespace faiss
