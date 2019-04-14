/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "gpu/StandardGpuResources_c.h"
#include "gpu/StandardGpuResources.h"
#include "macros_impl.h"

using faiss::gpu::StandardGpuResources;

DEFINE_DESTRUCTOR(StandardGpuResources)

int faiss_StandardGpuResources_new(FaissStandardGpuResources** p_res) {
    try {
        auto p = new StandardGpuResources();
        *p_res = reinterpret_cast<FaissStandardGpuResources*>(p);
    } CATCH_AND_HANDLE
}

int faiss_StandardGpuResources_noTempMemory(FaissStandardGpuResources* res) {
    try {
        reinterpret_cast<StandardGpuResources*>(res)->noTempMemory();
    } CATCH_AND_HANDLE
}

int faiss_StandardGpuResources_setTempMemory(FaissStandardGpuResources* res, size_t size) {
    try {
        reinterpret_cast<StandardGpuResources*>(res)->setTempMemory(size);
    } CATCH_AND_HANDLE
}

int faiss_StandardGpuResources_setTempMemoryFraction(FaissStandardGpuResources* res, float fraction) {
    try {
        reinterpret_cast<StandardGpuResources*>(res)->setTempMemoryFraction(fraction);
    } CATCH_AND_HANDLE
}

int faiss_StandardGpuResources_setPinnedMemory(FaissStandardGpuResources* res, size_t size) {
    try {
        reinterpret_cast<StandardGpuResources*>(res)->setPinnedMemory(size);
    } CATCH_AND_HANDLE
}

int faiss_StandardGpuResources_setDefaultStream(FaissStandardGpuResources* res, int device, cudaStream_t stream) {
    try {
        reinterpret_cast<StandardGpuResources*>(res)->setDefaultStream(device, stream);
    } CATCH_AND_HANDLE
}

int faiss_StandardGpuResources_setDefaultNullStreamAllDevices(FaissStandardGpuResources* res) {
    try {
        reinterpret_cast<StandardGpuResources*>(res)->setDefaultNullStreamAllDevices();
    } CATCH_AND_HANDLE
}
