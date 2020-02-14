
// -*- c++ -*-

#include <iostream>

#include <faiss/FaissHook.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/distances_avx512.h>
#include <faiss/utils/instruction_set.h>

namespace faiss {

fvec_func_ptr fvec_inner_product = nullptr;
fvec_func_ptr fvec_L2sqr = nullptr;
fvec_func_ptr fvec_L1 = nullptr;
fvec_func_ptr fvec_Linf = nullptr;

void hook_init() {
    if (InstructionSet::AVX512F() && InstructionSet::AVX512DQ()) {
        fvec_inner_product = fvec_inner_product_avx512;
        fvec_L2sqr = fvec_L2sqr_avx512;
        fvec_L1 = fvec_L1_avx512;
        fvec_Linf = fvec_Linf_avx512;
        std::cout << "FAISS hook AVX512" << std::endl;
    } else if (InstructionSet::AVX2()) {
        fvec_inner_product = fvec_inner_product_avx;
        fvec_L2sqr = fvec_L2sqr_avx;
        fvec_L1 = fvec_L1_avx;
        fvec_Linf = fvec_Linf_avx;
        std::cout << "FAISS hook AVX" << std::endl;
    } else if (InstructionSet::SSE()) {
        fvec_inner_product = fvec_inner_product_sse;
        fvec_L2sqr = fvec_L2sqr_sse;
        fvec_L1 = fvec_L1_sse;
        fvec_Linf = fvec_Linf_sse;
        std::cout << "FAISS hook SSE" << std::endl;
    } else {
        FAISS_ASSERT_MSG(false, "CPU not supported!");
    }
}

} // namespace faiss
