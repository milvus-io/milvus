
// -*- c++ -*-

#include <iostream>
#include <mutex>

#include <faiss/FaissHook.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/ScalarQuantizerDC.h>
#include <faiss/impl/ScalarQuantizerDC_avx.h>
#include <faiss/impl/ScalarQuantizerDC_avx512.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/distances_avx.h>
#include <faiss/utils/distances_avx512.h>
#include <faiss/utils/instruction_set.h>

namespace faiss {

bool faiss_use_avx512 = true;
bool faiss_use_avx2 = true;
bool faiss_use_sse = true;

/* set default to AVX */
fvec_func_ptr fvec_inner_product = fvec_inner_product_avx;
fvec_func_ptr fvec_L2sqr = fvec_L2sqr_avx;
fvec_func_ptr fvec_L1 = fvec_L1_avx;
fvec_func_ptr fvec_Linf = fvec_Linf_avx;

sq_get_distance_computer_func_ptr sq_get_distance_computer = sq_get_distance_computer_avx;
sq_sel_quantizer_func_ptr sq_sel_quantizer = sq_select_quantizer_avx;
sq_sel_inv_list_scanner_func_ptr sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_avx;

/*****************************************************************************/

bool support_avx512() {
//    std::cout << "Check if support avx512" << std::endl;
    if (!faiss_use_avx512) {
//        std::cout << "faiss_use_avx512 is " << faiss_use_avx512 << std::endl;
        return false;
    }

    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.AVX512F() &&
            instruction_set_inst.AVX512DQ() &&
            instruction_set_inst.AVX512BW());
//    std::cout << "InstructionSet check " << supported << std::endl;
//    return supported;
}

bool support_avx2() {
//    std::cout << "Check if support avx2" << std::endl;
    if (!faiss_use_avx2) {
//        std::cout << "faiss_use_avx2 is " << faiss_use_avx2 << std::endl;
        return false;
    }

    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.AVX2());
//    std::cout << "InstructionSet check " << supported << std::endl;
//    return supported;
}

bool support_sse() {
//    std::cout << "Check if support sse" << std::endl;
    if (!faiss_use_sse) {
//        std::cout << "faiss_use_sse is " << faiss_use_sse << std::endl;
        return false;
    }

    InstructionSet& instruction_set_inst = InstructionSet::GetInstance();
    return (instruction_set_inst.SSE42());
//    std::cout << "InstructionSet check " << supported << std::endl;
//    return supported;
}

bool hook_init(std::string& cpu_flag) {
//    std::cout << "Start init faiss hook ..." << std::endl;
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);

    if (support_avx512()) {
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
//        std::cout << "Set AVX512" << std::endl;
    } else if (support_avx2()) {
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
//        std::cout << "Set AVX2" << std::endl;
    } else if (support_sse()) {
        /* for IVFFLAT */
        fvec_inner_product = fvec_inner_product_sse;
        fvec_L2sqr = fvec_L2sqr_sse;
        fvec_L1 = fvec_L1_sse;
        fvec_Linf = fvec_Linf_sse;

        /* for IVFSQ */
        sq_get_distance_computer = sq_get_distance_computer_ref;
        sq_sel_quantizer = sq_select_quantizer_ref;
        sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_ref;

        cpu_flag = "SSE42";
//        std::cout << "Set SSE42" << std::endl;
    } else {
        cpu_flag = "UNSUPPORTED";
//        std::cout << "CPU unsupported" << std::endl;
        return false;
    }

    return true;
}

} // namespace faiss
