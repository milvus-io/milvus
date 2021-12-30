
// -*- c++ -*-

#pragma once

#include <vector>
#include <stddef.h>
#include <string>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/impl/ScalarQuantizerOp.h>
#include <faiss/MetricType.h>

namespace faiss {

typedef float (*fvec_func_ptr)(const float*, const float*, size_t);

typedef SQDistanceComputer* (*sq_get_distance_computer_func_ptr)(MetricType, QuantizerType, size_t, const std::vector<float>&);
typedef Quantizer* (*sq_sel_quantizer_func_ptr)(QuantizerType, size_t, const std::vector<float>&);
typedef InvertedListScanner* (*sq_sel_inv_list_scanner_func_ptr)(MetricType, const ScalarQuantizer*, const Index*, size_t, bool, bool);

extern bool faiss_use_avx512;
extern bool faiss_use_avx2;
extern bool faiss_use_sse4_2;

extern fvec_func_ptr fvec_inner_product;
extern fvec_func_ptr fvec_L2sqr;
extern fvec_func_ptr fvec_L1;
extern fvec_func_ptr fvec_Linf;

extern sq_get_distance_computer_func_ptr sq_get_distance_computer;
extern sq_sel_quantizer_func_ptr sq_sel_quantizer;
extern sq_sel_inv_list_scanner_func_ptr sq_sel_inv_list_scanner;

bool cpu_support_avx512();
bool cpu_support_avx2();
bool cpu_support_sse4_2();

void hook_init(std::string& cpu_flag);

} // namespace faiss
