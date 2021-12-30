
// -*- c++ -*-

#include <iostream>
#include <mutex>

#include <faiss/FaissHook.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/ScalarQuantizerDC.h>
#include <faiss/impl/ScalarQuantizerDC_avx.h>
#include <faiss/impl/ScalarQuantizerDC_avx512.h>
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

sq_get_distance_computer_func_ptr sq_get_distance_computer = sq_get_distance_computer_avx;
sq_sel_quantizer_func_ptr sq_sel_quantizer = sq_select_quantizer_avx;
sq_sel_inv_list_scanner_func_ptr sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_avx;

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

void hook_init(std::string& cpu_flag) {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);

    if (faiss_use_avx512 && cpu_support_avx512()) {
        /* for IVFFLAT */
        fvec_inner_product = fvec_inner_product_avx512;
        fvec_L2sqr = fvec_L2sqr_avx512;
        fvec_L1 = fvec_L1_avx512;
        fvec_Linf = fvec_Linf_avx512;

        /* for IVFSQ */
        sq_get_distance_computer = sq_get_distance_computer_avx512;
        sq_sel_quantizer = sq_select_quantizer_avx512;
        sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_avx512;

        cpu_flag = "AVX512";
    } else if (faiss_use_avx2 && cpu_support_avx2()) {
        /* for IVFFLAT */
        fvec_inner_product = fvec_inner_product_avx;
        fvec_L2sqr = fvec_L2sqr_avx;
        fvec_L1 = fvec_L1_avx;
        fvec_Linf = fvec_Linf_avx;

        /* for IVFSQ */
        sq_get_distance_computer = sq_get_distance_computer_avx;
        sq_sel_quantizer = sq_select_quantizer_avx;
        sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_avx;

        cpu_flag = "AVX2";
    } else if (faiss_use_sse4_2 && cpu_support_sse4_2()) {
        /* for IVFFLAT */
        fvec_inner_product = fvec_inner_product_sse;
        fvec_L2sqr = fvec_L2sqr_sse;
        fvec_L1 = fvec_L1_sse;
        fvec_Linf = fvec_Linf_sse;

        /* for IVFSQ */
        sq_get_distance_computer = sq_get_distance_computer_ref;
        sq_sel_quantizer = sq_select_quantizer_ref;
        sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_ref;

        cpu_flag = "SSE4_2";
    } else {
        /* for IVFFLAT */
        fvec_inner_product = fvec_inner_product_ref;
        fvec_L2sqr = fvec_L2sqr_ref;
        fvec_L1 = fvec_L1_ref;
        fvec_Linf = fvec_Linf_ref;

        /* for IVFSQ */
        sq_get_distance_computer = sq_get_distance_computer_ref;
        sq_sel_quantizer = sq_select_quantizer_ref;
        sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_ref;

        cpu_flag = "REF";
    }
}

} // namespace faiss
