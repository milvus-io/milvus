
// -*- c++ -*-

#include <iostream>
#include <mutex>

#include <faiss/FaissHook.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/ScalarQuantizerDC.h>
#include <faiss/impl/ScalarQuantizerDC_avx.h>
#include <faiss/impl/ScalarQuantizerDC_avx512.h>
#include <faiss/utils/distances.h>
//#include <faiss/utils/distances_avx.h>
//#include <faiss/utils/distances_avx512.h>

namespace faiss {

bool faiss_use_avx512 = false;
bool faiss_use_avx2 = false;
bool faiss_use_sse4_2 = false;

/* set default to AVX */
fvec_func_ptr fvec_inner_product = fvec_inner_product_ref;
fvec_func_ptr fvec_L2sqr = fvec_L2sqr_ref;
fvec_func_ptr fvec_L1 = fvec_L1_ref;
fvec_func_ptr fvec_Linf = fvec_Linf_ref;
sq_get_distance_computer_func_ptr sq_get_distance_computer = sq_get_distance_computer_ref;
sq_sel_quantizer_func_ptr sq_sel_quantizer = sq_select_quantizer_ref;
sq_sel_inv_list_scanner_func_ptr sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_ref;

/*****************************************************************************/

bool cpu_support_avx512() {
   return false;
}

bool cpu_support_avx2() {
    return false;
}

bool cpu_support_sse() {
    return false;
}

void hook_init(std::string& cpu_flag) {
    static std::mutex hook_mutex;
    std::lock_guard<std::mutex> lock(hook_mutex);
    
   cpu_flag = "aarch64 >>>";
    
   
   //cpu
   fvec_inner_product = fvec_inner_product_aarch64;
   fvec_L2sqr = fvec_L2sqr_aarch64;
   fvec_L1 = fvec_L1_aarch64;
   fvec_Linf = fvec_Linf_aarch64;

   /* for IVFSQ */
   sq_get_distance_computer = sq_get_distance_computer_ref;
   sq_sel_quantizer = sq_select_quantizer_ref;
   sq_sel_inv_list_scanner = sq_select_inverted_list_scanner_ref;

   //cpu_flag = "REF";

   return ;
}

} // namespace faiss
