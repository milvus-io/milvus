/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include <cstring>
#include "AutoTune.h"
#include "AutoTune_c.h"
#include "macros_impl.h"

using faiss::Index;
using faiss::ParameterRange;
using faiss::ParameterSpace;

/** Build and index with the sequence of processing steps described in
 *  the string.
 */
int faiss_index_factory(FaissIndex** p_index, int d, const char* description, FaissMetricType metric) {
    try {
        *p_index = reinterpret_cast<FaissIndex*>(faiss::index_factory(
            d, description, static_cast<faiss::MetricType>(metric)));
    } CATCH_AND_HANDLE
}

const char* faiss_ParameterRange_name(const FaissParameterRange* range) {
    return reinterpret_cast<const ParameterRange*>(range)->name.c_str();
}

void faiss_ParameterRange_values(FaissParameterRange* range, double** p_values, size_t* p_size) {
    auto& values = reinterpret_cast<ParameterRange*>(range)->values;
    *p_values = values.data();
    *p_size = values.size();
}

int faiss_ParameterSpace_new(FaissParameterSpace** space) {
    try {
        auto new_space = new ParameterSpace();
        *space = reinterpret_cast<FaissParameterSpace*>(new_space);
    } CATCH_AND_HANDLE
}

DEFINE_DESTRUCTOR(ParameterSpace)

size_t faiss_ParameterSpace_n_combinations(const FaissParameterSpace* space) {
    return reinterpret_cast<const ParameterSpace*>(space)->n_combinations();
}

int faiss_ParameterSpace_combination_name(const FaissParameterSpace* space, size_t cno, char* char_buffer, size_t size) {
    try {
        auto rep = reinterpret_cast<const ParameterSpace*>(space)->combination_name(cno);
        strncpy(char_buffer, rep.c_str(), size);
    } CATCH_AND_HANDLE
}

int faiss_ParameterSpace_set_index_parameters(const FaissParameterSpace* space, FaissIndex* cindex, const char* param_string) {
    try {
        auto index = reinterpret_cast<Index*>(cindex);
        reinterpret_cast<const ParameterSpace*>(space)->set_index_parameters(index, param_string);
    } CATCH_AND_HANDLE
}

/// set a combination of parameters on an index
int faiss_ParameterSpace_set_index_parameters_cno(const FaissParameterSpace* space, FaissIndex* cindex, size_t cno) {
    try {
        auto index = reinterpret_cast<Index*>(cindex);
        reinterpret_cast<const ParameterSpace*>(space)->set_index_parameters(index, cno);
    } CATCH_AND_HANDLE
}

int faiss_ParameterSpace_set_index_parameter(const FaissParameterSpace* space, FaissIndex* cindex, const char * name, double value) {
    try {
        auto index = reinterpret_cast<Index*>(cindex);
        reinterpret_cast<const ParameterSpace*>(space)->set_index_parameter(index, name, value);
    } CATCH_AND_HANDLE
}

void faiss_ParameterSpace_display(const FaissParameterSpace* space) {
    reinterpret_cast<const ParameterSpace*>(space)->display();
}

int faiss_ParameterSpace_add_range(FaissParameterSpace* space, const char* name, FaissParameterRange** p_range) {
    try {
        ParameterRange& range = reinterpret_cast<ParameterSpace*>(space)->add_range(name);
        if (p_range) {
            *p_range = reinterpret_cast<FaissParameterRange*>(&range);
        }
    } CATCH_AND_HANDLE
}
