/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#ifndef GPU_MACROS_IMPL_H
#define GPU_MACROS_IMPL_H
#include "../macros_impl.h"

#undef DEFINE_GETTER
#define DEFINE_GETTER(clazz, ty, name)                               \
    ty faiss_ ## clazz ## _ ## name (const Faiss ## clazz *obj) {    \
        return static_cast< ty >(                                    \
            reinterpret_cast< const faiss::gpu::clazz *>(obj)-> name \
        );                                                           \
    }

#undef DEFINE_SETTER
#define DEFINE_SETTER(clazz, ty, name)                                    \
    void faiss_ ## clazz ## _set_ ## name (Faiss ## clazz *obj, ty val) { \
        reinterpret_cast< faiss::gpu::clazz *>(obj)-> name = val;              \
    }

#undef DEFINE_SETTER_STATIC
#define DEFINE_SETTER_STATIC(clazz, ty_to, ty_from, name)                      \
    void faiss_ ## clazz ## _set_ ## name (Faiss ## clazz *obj, ty_from val) { \
        reinterpret_cast< faiss::gpu::clazz *>(obj)-> name =                   \
            static_cast< ty_to >(val);                                         \
    }

#undef DEFINE_DESTRUCTOR
#define DEFINE_DESTRUCTOR(clazz)                           \
    void faiss_ ## clazz ## _free (Faiss ## clazz *obj) {  \
        delete reinterpret_cast<faiss::gpu::clazz *>(obj); \
    }

#endif
