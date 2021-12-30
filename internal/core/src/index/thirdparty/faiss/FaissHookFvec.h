
// -*- c++ -*-

#pragma once

#include <string>

namespace faiss {

typedef float (*fvec_func_ptr)(const float*, const float*, size_t);

extern bool faiss_use_avx512;
extern bool faiss_use_avx2;
extern bool faiss_use_sse4_2;

extern fvec_func_ptr fvec_inner_product;
extern fvec_func_ptr fvec_L2sqr;
extern fvec_func_ptr fvec_L1;
extern fvec_func_ptr fvec_Linf;

bool cpu_support_avx512();
bool cpu_support_avx2();
bool cpu_support_sse4_2();

void hook_fvec(std::string& simd_type);

} // namespace faiss
