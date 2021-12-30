
// -*- c++ -*-

#pragma once

#include <string>
#include <faiss/FaissHookFvec.h>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/MetricType.h>

namespace faiss {

typedef SQDistanceComputer* (*sq_get_distance_computer_func_ptr)(MetricType, QuantizerType, size_t, const std::vector<float>&);
typedef Quantizer* (*sq_sel_quantizer_func_ptr)(QuantizerType, size_t, const std::vector<float>&);
typedef InvertedListScanner* (*sq_sel_inv_list_scanner_func_ptr)(MetricType, const ScalarQuantizer*, const Index*, size_t, bool, bool);

extern sq_get_distance_computer_func_ptr sq_get_distance_computer;
extern sq_sel_quantizer_func_ptr sq_sel_quantizer;
extern sq_sel_inv_list_scanner_func_ptr sq_sel_inv_list_scanner;

void hook_init(std::string& simd_type);

} // namespace faiss
