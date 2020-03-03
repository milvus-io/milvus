
// -*- c++ -*-

#pragma once

#include <vector>
#include <stddef.h>
#include <string>
#include <faiss/impl/ScalarQuantizerOp.h>

namespace faiss {

typedef float (*fvec_func_ptr)(const float*, const float*, size_t);

typedef SQDistanceComputer* (*sq_get_func_ptr)(QuantizerType, size_t, const std::vector<float>&);
typedef Quantizer* (*sq_sel_func_ptr)(QuantizerType, size_t, const std::vector<float>&);


extern bool faiss_use_avx512;

extern fvec_func_ptr fvec_inner_product;
extern fvec_func_ptr fvec_L2sqr;
extern fvec_func_ptr fvec_L1;
extern fvec_func_ptr fvec_Linf;

extern sq_get_func_ptr sq_get_distance_computer_L2;
extern sq_get_func_ptr sq_get_distance_computer_IP;
extern sq_sel_func_ptr sq_sel_quantizer;

extern bool support_avx512();

extern std::string hook_init();

} // namespace faiss
