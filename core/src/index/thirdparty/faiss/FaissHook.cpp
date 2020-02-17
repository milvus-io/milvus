
// -*- c++ -*-

#include <iostream>

#include <faiss/FaissHook.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/distances_avx512.h>
#include <faiss/utils/instruction_set.h>

namespace faiss {

bool faiss_use_avx512 = true;

/* set default to AVX */
fvec_func_ptr fvec_inner_product = fvec_inner_product_avx;
fvec_func_ptr fvec_L2sqr = fvec_L2sqr_avx;
fvec_func_ptr fvec_L1 = fvec_L1_avx;
fvec_func_ptr fvec_Linf = fvec_Linf_avx;

void hook_init() {
    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    if (faiss_use_avx512 && instruction_set_inst.AVX512F() && instruction_set_inst.AVX512DQ()) {
        fvec_inner_product = fvec_inner_product_avx512;
        fvec_L2sqr = fvec_L2sqr_avx512;
        fvec_L1 = fvec_L1_avx512;
        fvec_Linf = fvec_Linf_avx512;
        std::cout << "FAISS hook AVX512" << std::endl;
    } else if (instruction_set_inst.AVX2()) {
        fvec_inner_product = fvec_inner_product_avx;
        fvec_L2sqr = fvec_L2sqr_avx;
        fvec_L1 = fvec_L1_avx;
        fvec_Linf = fvec_Linf_avx;
        std::cout << "FAISS hook AVX" << std::endl;
    } else if (instruction_set_inst.SSE()) {
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
