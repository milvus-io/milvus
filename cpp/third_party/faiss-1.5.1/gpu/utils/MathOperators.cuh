/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "Float16.cuh"

//
// Templated wrappers to express math for different scalar and vector
// types, so kernels can have the same written form but can operate
// over half and float, and on vector types transparently
//

namespace faiss { namespace gpu {

template <typename T>
struct Math {
  typedef T ScalarType;

  static inline __device__ T add(T a, T b) {
    return a + b;
  }

  static inline __device__ T sub(T a, T b) {
    return a - b;
  }

  static inline __device__ T mul(T a, T b) {
    return a * b;
  }

  static inline __device__ T neg(T v) {
    return -v;
  }

  /// For a vector type, this is a horizontal add, returning sum(v_i)
  static inline __device__ T reduceAdd(T v) {
    return v;
  }

  static inline __device__ bool lt(T a, T b) {
    return a < b;
  }

  static inline __device__ bool gt(T a, T b) {
    return a > b;
  }

  static inline __device__ bool eq(T a, T b) {
    return a == b;
  }

  static inline __device__ T zero() {
    return (T) 0;
  }
};

template <>
struct Math<float2> {
  typedef float ScalarType;

  static inline __device__ float2 add(float2 a, float2 b) {
    float2 v;
    v.x = a.x + b.x;
    v.y = a.y + b.y;
    return v;
  }

  static inline __device__ float2 sub(float2 a, float2 b) {
    float2 v;
    v.x = a.x - b.x;
    v.y = a.y - b.y;
    return v;
  }

  static inline __device__ float2 add(float2 a, float b) {
    float2 v;
    v.x = a.x + b;
    v.y = a.y + b;
    return v;
  }

  static inline __device__ float2 sub(float2 a, float b) {
    float2 v;
    v.x = a.x - b;
    v.y = a.y - b;
    return v;
  }

  static inline __device__ float2 mul(float2 a, float2 b) {
    float2 v;
    v.x = a.x * b.x;
    v.y = a.y * b.y;
    return v;
  }

  static inline __device__ float2 mul(float2 a, float b) {
    float2 v;
    v.x = a.x * b;
    v.y = a.y * b;
    return v;
  }

  static inline __device__ float2 neg(float2 v) {
    v.x = -v.x;
    v.y = -v.y;
    return v;
  }

  /// For a vector type, this is a horizontal add, returning sum(v_i)
  static inline __device__ float reduceAdd(float2 v) {
    return v.x + v.y;
  }

  // not implemented for vector types
  // static inline __device__ bool lt(float2 a, float2 b);
  // static inline __device__ bool gt(float2 a, float2 b);
  // static inline __device__ bool eq(float2 a, float2 b);

  static inline __device__ float2 zero() {
    float2 v;
    v.x = 0.0f;
    v.y = 0.0f;
    return v;
  }
};

template <>
struct Math<float4> {
  typedef float ScalarType;

  static inline __device__ float4 add(float4 a, float4 b) {
    float4 v;
    v.x = a.x + b.x;
    v.y = a.y + b.y;
    v.z = a.z + b.z;
    v.w = a.w + b.w;
    return v;
  }

  static inline __device__ float4 sub(float4 a, float4 b) {
    float4 v;
    v.x = a.x - b.x;
    v.y = a.y - b.y;
    v.z = a.z - b.z;
    v.w = a.w - b.w;
    return v;
  }

  static inline __device__ float4 add(float4 a, float b) {
    float4 v;
    v.x = a.x + b;
    v.y = a.y + b;
    v.z = a.z + b;
    v.w = a.w + b;
    return v;
  }

  static inline __device__ float4 sub(float4 a, float b) {
    float4 v;
    v.x = a.x - b;
    v.y = a.y - b;
    v.z = a.z - b;
    v.w = a.w - b;
    return v;
  }

  static inline __device__ float4 mul(float4 a, float4 b) {
    float4 v;
    v.x = a.x * b.x;
    v.y = a.y * b.y;
    v.z = a.z * b.z;
    v.w = a.w * b.w;
    return v;
  }

  static inline __device__ float4 mul(float4 a, float b) {
    float4 v;
    v.x = a.x * b;
    v.y = a.y * b;
    v.z = a.z * b;
    v.w = a.w * b;
    return v;
  }

  static inline __device__ float4 neg(float4 v) {
    v.x = -v.x;
    v.y = -v.y;
    v.z = -v.z;
    v.w = -v.w;
    return v;
  }

  /// For a vector type, this is a horizontal add, returning sum(v_i)
  static inline __device__ float reduceAdd(float4 v) {
    return v.x + v.y + v.z + v.w;
  }

  // not implemented for vector types
  // static inline __device__ bool lt(float4 a, float4 b);
  // static inline __device__ bool gt(float4 a, float4 b);
  // static inline __device__ bool eq(float4 a, float4 b);

  static inline __device__ float4 zero() {
    float4 v;
    v.x = 0.0f;
    v.y = 0.0f;
    v.z = 0.0f;
    v.w = 0.0f;
    return v;
  }
};

#ifdef FAISS_USE_FLOAT16

template <>
struct Math<half> {
  typedef half ScalarType;

  static inline __device__ half add(half a, half b) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hadd(a, b);
#else
    return __float2half(__half2float(a) + __half2float(b));
#endif
  }

  static inline __device__ half sub(half a, half b) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hsub(a, b);
#else
    return __float2half(__half2float(a) - __half2float(b));
#endif
  }

  static inline __device__ half mul(half a, half b) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hmul(a, b);
#else
    return __float2half(__half2float(a) * __half2float(b));
#endif
  }

  static inline __device__ half neg(half v) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hneg(v);
#else
    return __float2half(-__half2float(v));
#endif
  }

  static inline __device__ half reduceAdd(half v) {
    return v;
  }

  static inline __device__ bool lt(half a, half b) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hlt(a, b);
#else
    return __half2float(a) < __half2float(b);
#endif
  }

  static inline __device__ bool gt(half a, half b) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hgt(a, b);
#else
    return __half2float(a) > __half2float(b);
#endif
  }

  static inline __device__ bool eq(half a, half b) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __heq(a, b);
#else
    return __half2float(a) == __half2float(b);
#endif
  }

  static inline __device__ half zero() {
#if CUDA_VERSION >= 9000
    return 0;
#else
    half h;
    h.x = 0;
    return h;
#endif
  }
};

template <>
struct Math<half2> {
  typedef half ScalarType;

  static inline __device__ half2 add(half2 a, half2 b) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hadd2(a, b);
#else
  float2 af = __half22float2(a);
  float2 bf = __half22float2(b);

  af.x += bf.x;
  af.y += bf.y;

  return __float22half2_rn(af);
#endif
  }

  static inline __device__ half2 sub(half2 a, half2 b) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hsub2(a, b);
#else
  float2 af = __half22float2(a);
  float2 bf = __half22float2(b);

  af.x -= bf.x;
  af.y -= bf.y;

  return __float22half2_rn(af);
#endif
  }

  static inline __device__ half2 add(half2 a, half b) {
#ifdef FAISS_USE_FULL_FLOAT16
    half2 b2 = __half2half2(b);
    return __hadd2(a, b2);
#else
  float2 af = __half22float2(a);
  float bf = __half2float(b);

  af.x += bf;
  af.y += bf;

  return __float22half2_rn(af);
#endif
  }

  static inline __device__ half2 sub(half2 a, half b) {
#ifdef FAISS_USE_FULL_FLOAT16
    half2 b2 = __half2half2(b);
    return __hsub2(a, b2);
#else
  float2 af = __half22float2(a);
  float bf = __half2float(b);

  af.x -= bf;
  af.y -= bf;

  return __float22half2_rn(af);
#endif
  }

  static inline __device__ half2 mul(half2 a, half2 b) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hmul2(a, b);
#else
  float2 af = __half22float2(a);
  float2 bf = __half22float2(b);

  af.x *= bf.x;
  af.y *= bf.y;

  return __float22half2_rn(af);
#endif
  }

  static inline __device__ half2 mul(half2 a, half b) {
#ifdef FAISS_USE_FULL_FLOAT16
    half2 b2 = __half2half2(b);
    return __hmul2(a, b2);
#else
  float2 af = __half22float2(a);
  float bf = __half2float(b);

  af.x *= bf;
  af.y *= bf;

  return __float22half2_rn(af);
#endif
  }

  static inline __device__ half2 neg(half2 v) {
#ifdef FAISS_USE_FULL_FLOAT16
    return __hneg2(v);
#else
  float2 vf = __half22float2(v);
  vf.x = -vf.x;
  vf.y = -vf.y;

  return __float22half2_rn(vf);
#endif
  }

  static inline __device__ half reduceAdd(half2 v) {
#ifdef FAISS_USE_FULL_FLOAT16
  half hv = __high2half(v);
  half lv = __low2half(v);

  return __hadd(hv, lv);
#else
  float2 vf = __half22float2(v);
  vf.x += vf.y;

  return __float2half(vf.x);
#endif
  }

  // not implemented for vector types
  // static inline __device__ bool lt(half2 a, half2 b);
  // static inline __device__ bool gt(half2 a, half2 b);
  // static inline __device__ bool eq(half2 a, half2 b);

  static inline __device__ half2 zero() {
    return __half2half2(Math<half>::zero());
  }
};

template <>
struct Math<Half4> {
  typedef half ScalarType;

  static inline __device__ Half4 add(Half4 a, Half4 b) {
    Half4 h;
    h.a = Math<half2>::add(a.a, b.a);
    h.b = Math<half2>::add(a.b, b.b);
    return h;
  }

  static inline __device__ Half4 sub(Half4 a, Half4 b) {
    Half4 h;
    h.a = Math<half2>::sub(a.a, b.a);
    h.b = Math<half2>::sub(a.b, b.b);
    return h;
  }

  static inline __device__ Half4 add(Half4 a, half b) {
    Half4 h;
    h.a = Math<half2>::add(a.a, b);
    h.b = Math<half2>::add(a.b, b);
    return h;
  }

  static inline __device__ Half4 sub(Half4 a, half b) {
    Half4 h;
    h.a = Math<half2>::sub(a.a, b);
    h.b = Math<half2>::sub(a.b, b);
    return h;
  }

  static inline __device__ Half4 mul(Half4 a, Half4 b) {
    Half4 h;
    h.a = Math<half2>::mul(a.a, b.a);
    h.b = Math<half2>::mul(a.b, b.b);
    return h;
  }

  static inline __device__ Half4 mul(Half4 a, half b) {
    Half4 h;
    h.a = Math<half2>::mul(a.a, b);
    h.b = Math<half2>::mul(a.b, b);
    return h;
  }

  static inline __device__ Half4 neg(Half4 v) {
    Half4 h;
    h.a = Math<half2>::neg(v.a);
    h.b = Math<half2>::neg(v.b);
    return h;
  }

  static inline __device__ half reduceAdd(Half4 v) {
    half hx = Math<half2>::reduceAdd(v.a);
    half hy = Math<half2>::reduceAdd(v.b);
    return Math<half>::add(hx, hy);
  }

  // not implemented for vector types
  // static inline __device__ bool lt(Half4 a, Half4 b);
  // static inline __device__ bool gt(Half4 a, Half4 b);
  // static inline __device__ bool eq(Half4 a, Half4 b);

  static inline __device__ Half4 zero() {
    Half4 h;
    h.a = Math<half2>::zero();
    h.b = Math<half2>::zero();
    return h;
  }
};

template <>
struct Math<Half8> {
  typedef half ScalarType;

  static inline __device__ Half8 add(Half8 a, Half8 b) {
    Half8 h;
    h.a = Math<Half4>::add(a.a, b.a);
    h.b = Math<Half4>::add(a.b, b.b);
    return h;
  }

  static inline __device__ Half8 sub(Half8 a, Half8 b) {
    Half8 h;
    h.a = Math<Half4>::sub(a.a, b.a);
    h.b = Math<Half4>::sub(a.b, b.b);
    return h;
  }

  static inline __device__ Half8 add(Half8 a, half b) {
    Half8 h;
    h.a = Math<Half4>::add(a.a, b);
    h.b = Math<Half4>::add(a.b, b);
    return h;
  }

  static inline __device__ Half8 sub(Half8 a, half b) {
    Half8 h;
    h.a = Math<Half4>::sub(a.a, b);
    h.b = Math<Half4>::sub(a.b, b);
    return h;
  }

  static inline __device__ Half8 mul(Half8 a, Half8 b) {
    Half8 h;
    h.a = Math<Half4>::mul(a.a, b.a);
    h.b = Math<Half4>::mul(a.b, b.b);
    return h;
  }

  static inline __device__ Half8 mul(Half8 a, half b) {
    Half8 h;
    h.a = Math<Half4>::mul(a.a, b);
    h.b = Math<Half4>::mul(a.b, b);
    return h;
  }

  static inline __device__ Half8 neg(Half8 v) {
    Half8 h;
    h.a = Math<Half4>::neg(v.a);
    h.b = Math<Half4>::neg(v.b);
    return h;
  }

  static inline __device__ half reduceAdd(Half8 v) {
    half hx = Math<Half4>::reduceAdd(v.a);
    half hy = Math<Half4>::reduceAdd(v.b);
    return Math<half>::add(hx, hy);
  }

  // not implemented for vector types
  // static inline __device__ bool lt(Half8 a, Half8 b);
  // static inline __device__ bool gt(Half8 a, Half8 b);
  // static inline __device__ bool eq(Half8 a, Half8 b);

  static inline __device__ Half8 zero() {
    Half8 h;
    h.a = Math<Half4>::zero();
    h.b = Math<Half4>::zero();
    return h;
  }
};

#endif // FAISS_USE_FLOAT16

} } // namespace
