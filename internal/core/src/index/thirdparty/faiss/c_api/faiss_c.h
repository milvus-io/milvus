/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

/// Macros and typedefs for C wrapper API declarations

#ifndef FAISS_C_H
#define FAISS_C_H

#include <stdint.h>

typedef int64_t faiss_idx_t;    ///< all indices are this type
typedef faiss_idx_t idx_t;
typedef float faiss_component_t;    ///< all vector components are this type
typedef float faiss_distance_t;    ///< all distances between vectors are this type

/// Declare an opaque type for a class type `clazz`.
#define FAISS_DECLARE_CLASS(clazz) \
    typedef struct Faiss ## clazz ## _H Faiss ## clazz;

/// Declare an opaque type for a class type `clazz`, while
/// actually aliasing it to an existing parent class type `parent`.
#define FAISS_DECLARE_CLASS_INHERITED(clazz, parent) \
    typedef struct Faiss ## parent ## _H Faiss ## clazz;

/// Declare a dynamic downcast operation from a base `FaissIndex*` pointer
/// type to a more specific index type. The function returns the same pointer
/// if the downcast is valid, and `NULL` otherwise.
#define FAISS_DECLARE_INDEX_DOWNCAST(clazz) \
    Faiss ## clazz * faiss_ ## clazz ## _cast (FaissIndex*);

/// Declare a getter for the field `name` in class `clazz`,
/// of return type `ty`
#define FAISS_DECLARE_GETTER(clazz, ty, name) \
    ty faiss_ ## clazz ## _ ## name (const Faiss ## clazz *);

/// Declare a setter for the field `name` in class `clazz`,
/// in which the user provides a value of type `ty`
#define FAISS_DECLARE_SETTER(clazz, ty, name) \
    void faiss_ ## clazz ## _set_ ## name (Faiss ## clazz *, ty);

/// Declare a getter and setter for the field `name` in class `clazz`.
#define FAISS_DECLARE_GETTER_SETTER(clazz, ty, name) \
    FAISS_DECLARE_GETTER(clazz, ty, name) \
    FAISS_DECLARE_SETTER(clazz, ty, name)
    
/// Declare a destructor function which frees an object of
/// type `clazz`.
#define FAISS_DECLARE_DESTRUCTOR(clazz) \
    void faiss_ ## clazz ## _free (Faiss ## clazz *obj);

#endif
