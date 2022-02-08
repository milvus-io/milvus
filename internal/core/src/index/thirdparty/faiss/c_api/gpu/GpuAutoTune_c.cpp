/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "GpuAutoTune_c.h"
#include "GpuClonerOptions_c.h"
#include "macros_impl.h"
#include "Index.h"
#include "gpu/GpuAutoTune.h"
#include "gpu/GpuClonerOptions.h"
#include <vector>

using faiss::Index;
using faiss::gpu::GpuResources;
using faiss::gpu::GpuClonerOptions;
using faiss::gpu::GpuMultipleClonerOptions;

int faiss_index_gpu_to_cpu(const FaissIndex* gpu_index, FaissIndex** p_out) {
    try {
        auto cpu_index = faiss::gpu::index_gpu_to_cpu(
            reinterpret_cast<const Index*>(gpu_index)
        );
        *p_out = reinterpret_cast<FaissIndex*>(cpu_index);
    } CATCH_AND_HANDLE
}

/// converts any CPU index that can be converted to GPU
int faiss_index_cpu_to_gpu(FaissGpuResources* resources, int device, const FaissIndex *index, FaissGpuIndex** p_out) {
    try {
        auto res = reinterpret_cast<GpuResources*>(resources);
        auto gpu_index = faiss::gpu::index_cpu_to_gpu(
            res, device, reinterpret_cast<const Index*>(index)
        );
        *p_out = reinterpret_cast<FaissGpuIndex*>(gpu_index);
    } CATCH_AND_HANDLE
}

int faiss_index_cpu_to_gpu_with_options(
    FaissGpuResources* resources, int device,
    const FaissIndex *index, const FaissGpuClonerOptions* options,
    FaissGpuIndex** p_out)
{
    try {
        auto res = reinterpret_cast<GpuResources*>(resources);
        auto gpu_index = faiss::gpu::index_cpu_to_gpu(
            res, device, reinterpret_cast<const Index*>(index),
            reinterpret_cast<const GpuClonerOptions*>(options));
        *p_out = reinterpret_cast<FaissGpuIndex*>(gpu_index);
    } CATCH_AND_HANDLE
}

int faiss_index_cpu_to_gpu_multiple(
    FaissGpuResources* const* resources_vec,
    const int* devices, size_t devices_size,
    const FaissIndex* index, FaissGpuIndex** p_out)
{
    try {
        std::vector<GpuResources*> res(devices_size);
        for (auto i = 0u; i < devices_size; ++i) {
            res[i] = reinterpret_cast<GpuResources*>(resources_vec[i]);
        }

        std::vector<int> dev(devices, devices + devices_size);

        auto gpu_index = faiss::gpu::index_cpu_to_gpu_multiple(
            res, dev, reinterpret_cast<const Index*>(index));
        *p_out = reinterpret_cast<FaissGpuIndex*>(gpu_index);
    } CATCH_AND_HANDLE
}

int faiss_index_cpu_to_gpu_multiple_with_options(
    FaissGpuResources** resources_vec, size_t resources_vec_size,
    int* devices, size_t devices_size,
    const FaissIndex* index, const FaissGpuMultipleClonerOptions* options,
    FaissGpuIndex** p_out)
{
    try {
        std::vector<GpuResources*> res(resources_vec_size);
        for (auto i = 0u; i < resources_vec_size; ++i) {
            res[i] = reinterpret_cast<GpuResources*>(resources_vec[i]);
        }

        std::vector<int> dev(devices, devices + devices_size);

        auto gpu_index = faiss::gpu::index_cpu_to_gpu_multiple(
            res, dev, reinterpret_cast<const Index*>(index),
            reinterpret_cast<const GpuMultipleClonerOptions*>(options));
        *p_out = reinterpret_cast<FaissGpuIndex*>(gpu_index);
    } CATCH_AND_HANDLE
}
