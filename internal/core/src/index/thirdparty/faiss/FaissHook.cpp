
// -*- c++ -*-

#include <iostream>
#include <mutex>

#include <faiss/FaissHook.h>
#include <faiss/impl/ScalarQuantizerDC.h>
#include <faiss/impl/ScalarQuantizerDC_avx.h>
#include <faiss/impl/ScalarQuantizerDC_avx512.h>

namespace faiss {

/* set default to AVX */
sq_get_distance_computer_func_ptr sq_get_distance_computer = sq_get_distance_computer_avx;
sq_sel_quantizer_func_ptr sq_sel_quantizer = sq_select_quantizer_avx;
sq_sel_inv_list_scanner_func_ptr sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_avx;

/*****************************************************************************/

void hook_init(std::string& simd_type) {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);

    // SQ8 always hook best SIMD
    std::string type;
    if (cpu_support_avx512()) {
        /* for IVFSQ */
        sq_get_distance_computer = sq_get_distance_computer_avx512;
        sq_sel_quantizer = sq_select_quantizer_avx512;
        sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_avx512;

        type = "AVX512";
    } else if (faiss_use_avx2 && cpu_support_avx2()) {
        /* for IVFSQ */
        sq_get_distance_computer = sq_get_distance_computer_avx;
        sq_sel_quantizer = sq_select_quantizer_avx;
        sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_avx;

        type = "AVX2";
    } else if (faiss_use_sse4_2 && cpu_support_sse4_2()) {
        /* for IVFSQ */
        sq_get_distance_computer = sq_get_distance_computer_ref;
        sq_sel_quantizer = sq_select_quantizer_ref;
        sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_ref;

        type = "SSE4_2";
    } else {
        /* for IVFSQ */
        sq_get_distance_computer = sq_get_distance_computer_ref;
        sq_sel_quantizer = sq_select_quantizer_ref;
        sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_ref;

        type = "REF";
    }
    std::cout << "FAISS SQ8 hook " << type << std::endl;

    hook_fvec(simd_type);
}

} // namespace faiss
