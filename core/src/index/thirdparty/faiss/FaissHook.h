
// -*- c++ -*-

#pragma once

#include <stddef.h>

namespace faiss {

typedef float (*fvec_func_ptr)(const float*, const float*, size_t);

extern bool faiss_use_avx512;

extern fvec_func_ptr fvec_inner_product;
extern fvec_func_ptr fvec_L2sqr;
extern fvec_func_ptr fvec_L1;
extern fvec_func_ptr fvec_Linf;

extern bool support_avx512();

extern void hook_init();

} // namespace faiss
