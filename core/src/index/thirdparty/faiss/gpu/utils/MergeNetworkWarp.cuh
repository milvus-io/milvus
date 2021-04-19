/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/MergeNetworkUtils.cuh>
#include <faiss/gpu/utils/PtxUtils.cuh>
#include <faiss/gpu/utils/StaticUtils.h>
#include <faiss/gpu/utils/WarpShuffles.cuh>

namespace faiss { namespace gpu {

//
// This file contains functions to:
//
// -perform bitonic merges on pairs of sorted lists, held in
// registers. Each list contains N * kWarpSize (multiple of 32)
// elements for some N.
// The bitonic merge is implemented for arbitrary sizes;
// sorted list A of size N1 * kWarpSize registers
// sorted list B of size N2 * kWarpSize registers =>
// sorted list C if size (N1 + N2) * kWarpSize registers. N1 and N2
// are >= 1 and don't have to be powers of 2.
//
// -perform bitonic sorts on a set of N * kWarpSize key/value pairs
// held in registers, by using the above bitonic merge as a
// primitive.
// N can be an arbitrary N >= 1; i.e., the bitonic sort here supports
// odd sizes and doesn't require the input to be a power of 2.
//
// The sort or merge network is completely statically instantiated via
// template specialization / expansion and constexpr, and it uses warp
// shuffles to exchange values between warp lanes.
//
// A note about comparsions:
//
// For a sorting network of keys only, we only need one
// comparison (a < b). However, what we really need to know is
// if one lane chooses to exchange a value, then the
// corresponding lane should also do the exchange.
// Thus, if one just uses the negation !(x < y) in the higher
// lane, this will also include the case where (x == y). Thus, one
// lane in fact performs an exchange and the other doesn't, but
// because the only value being exchanged is equivalent, nothing has
// changed.
// So, you can get away with just one comparison and its negation.
//
// If we're sorting keys and values, where equivalent keys can
// exist, then this is a problem, since we want to treat (x, v1)
// as not equivalent to (x, v2).
//
// To remedy this, you can either compare with a lexicographic
// ordering (a.k < b.k || (a.k == b.k && a.v < b.v)), which since
// we're predicating all of the choices results in 3 comparisons
// being executed, or we can invert the selection so that there is no
// middle choice of equality; the other lane will likewise
// check that (b.k > a.k) (the higher lane has the values
// swapped). Then, the first lane swaps if and only if the
// second lane swaps; if both lanes have equivalent keys, no
// swap will be performed. This results in only two comparisons
// being executed.
//
// If you don't consider values as well, then this does not produce a
// consistent ordering among (k, v) pairs with equivalent keys but
// different values; for us, we don't really care about ordering or
// stability here.
//
// I have tried both re-arranging the order in the higher lane to get
// away with one comparison or adding the value to the check; both
// result in greater register consumption or lower speed than just
// perfoming both < and > comparisons with the variables, so I just
// stick with this.

// This function merges kWarpSize / 2L lists in parallel using warp
// shuffles.
// It works on at most size-16 lists, as we need 32 threads for this
// shuffle merge.
//
// If IsBitonic is false, the first stage is reversed, so we don't
// need to sort directionally. It's still technically a bitonic sort.
template <typename K, typename V, int L,
          bool Dir, typename Comp, bool IsBitonic>
inline __device__ void warpBitonicMergeLE16(K& k, V& v) {
  static_assert(utils::isPowerOf2(L), "L must be a power-of-2");
  static_assert(L <= kWarpSize / 2, "merge list size must be <= 16");

  int laneId = getLaneId();

  if (!IsBitonic) {
    // Reverse the first comparison stage.
    // For example, merging a list of size 8 has the exchanges:
    // 0 <-> 15, 1 <-> 14, ...
    K otherK = shfl_xor(k, 2 * L - 1);
    V otherV = shfl_xor(v, 2 * L - 1);

    // Whether we are the lesser thread in the exchange
    bool small = !(laneId & L);

    if (Dir) {
      // See the comment above how performing both of these
      // comparisons in the warp seems to win out over the
      // alternatives in practice
      bool s = small ? Comp::gt(k, otherK) : Comp::lt(k, otherK);
      assign(s, k, otherK);
      assign(s, v, otherV);

    } else {
      bool s = small ? Comp::lt(k, otherK) : Comp::gt(k, otherK);
      assign(s, k, otherK);
      assign(s, v, otherV);
    }
  }

#pragma unroll
  for (int stride = IsBitonic ? L : L / 2; stride > 0; stride /= 2) {
    K otherK = shfl_xor(k, stride);
    V otherV = shfl_xor(v, stride);

    // Whether we are the lesser thread in the exchange
    bool small = !(laneId & stride);

    if (Dir) {
      bool s = small ? Comp::gt(k, otherK) : Comp::lt(k, otherK);
      assign(s, k, otherK);
      assign(s, v, otherV);

    } else {
      bool s = small ? Comp::lt(k, otherK) : Comp::gt(k, otherK);
      assign(s, k, otherK);
      assign(s, v, otherV);
    }
  }
}

// Template for performing a bitonic merge of an arbitrary set of
// registers
template <typename K, typename V, int N,
          bool Dir, typename Comp, bool Low, bool Pow2>
struct BitonicMergeStep {
};

//
// Power-of-2 merge specialization
//

// All merges eventually call this
template <typename K, typename V, bool Dir, typename Comp, bool Low>
struct BitonicMergeStep<K, V, 1, Dir, Comp, Low, true> {
  static inline __device__ void merge(K k[1], V v[1]) {
    // Use warp shuffles
    warpBitonicMergeLE16<K, V, 16, Dir, Comp, true>(k[0], v[0]);
  }
};

template <typename K, typename V, int N, bool Dir, typename Comp, bool Low>
struct BitonicMergeStep<K, V, N, Dir, Comp, Low, true> {
  static inline __device__ void merge(K k[N], V v[N]) {
    static_assert(utils::isPowerOf2(N), "must be power of 2");
    static_assert(N > 1, "must be N > 1");

#pragma unroll
    for (int i = 0; i < N / 2; ++i) {
      K& ka = k[i];
      V& va = v[i];

      K& kb = k[i + N / 2];
      V& vb = v[i + N / 2];

      bool s = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
      swap(s, ka, kb);
      swap(s, va, vb);
    }

    {
      K newK[N / 2];
      V newV[N / 2];

#pragma unroll
      for (int i = 0; i < N / 2; ++i) {
        newK[i] = k[i];
        newV[i] = v[i];
      }

      BitonicMergeStep<K, V, N / 2, Dir, Comp, true, true>::merge(newK, newV);

#pragma unroll
      for (int i = 0; i < N / 2; ++i) {
        k[i] = newK[i];
        v[i] = newV[i];
      }
    }

    {
      K newK[N / 2];
      V newV[N / 2];

#pragma unroll
      for (int i = 0; i < N / 2; ++i) {
        newK[i] = k[i + N / 2];
        newV[i] = v[i + N / 2];
      }

      BitonicMergeStep<K, V, N / 2, Dir, Comp, false, true>::merge(newK, newV);

#pragma unroll
      for (int i = 0; i < N / 2; ++i) {
        k[i + N / 2] = newK[i];
        v[i + N / 2] = newV[i];
      }
    }
  }
};

//
// Non-power-of-2 merge specialization
//

// Low recursion
template <typename K, typename V, int N, bool Dir, typename Comp>
struct BitonicMergeStep<K, V, N, Dir, Comp, true, false> {
  static inline __device__ void merge(K k[N], V v[N]) {
    static_assert(!utils::isPowerOf2(N), "must be non-power-of-2");
    static_assert(N >= 3, "must be N >= 3");

    constexpr int kNextHighestPowerOf2 = utils::nextHighestPowerOf2(N);

#pragma unroll
    for (int i = 0; i < N - kNextHighestPowerOf2 / 2; ++i) {
      K& ka = k[i];
      V& va = v[i];

      K& kb = k[i + kNextHighestPowerOf2 / 2];
      V& vb = v[i + kNextHighestPowerOf2 / 2];

      bool s = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
      swap(s, ka, kb);
      swap(s, va, vb);
    }

    constexpr int kLowSize = N - kNextHighestPowerOf2 / 2;
    constexpr int kHighSize = kNextHighestPowerOf2 / 2;
    {
      K newK[kLowSize];
      V newV[kLowSize];

#pragma unroll
      for (int i = 0; i < kLowSize; ++i) {
        newK[i] = k[i];
        newV[i] = v[i];
      }

      constexpr bool kLowIsPowerOf2 =
        utils::isPowerOf2(N - kNextHighestPowerOf2 / 2);
      // FIXME: compiler doesn't like this expression? compiler bug?
//      constexpr bool kLowIsPowerOf2 = utils::isPowerOf2(kLowSize);
      BitonicMergeStep<K, V, kLowSize, Dir, Comp,
                       true, // low
                       kLowIsPowerOf2>::merge(newK, newV);

#pragma unroll
      for (int i = 0; i < kLowSize; ++i) {
        k[i] = newK[i];
        v[i] = newV[i];
      }
    }

    {
      K newK[kHighSize];
      V newV[kHighSize];

#pragma unroll
      for (int i = 0; i < kHighSize; ++i) {
        newK[i] = k[i + kLowSize];
        newV[i] = v[i + kLowSize];
      }

      constexpr bool kHighIsPowerOf2 =
        utils::isPowerOf2(kNextHighestPowerOf2 / 2);
      // FIXME: compiler doesn't like this expression? compiler bug?
//      constexpr bool kHighIsPowerOf2 = utils::isPowerOf2(kHighSize);
      BitonicMergeStep<K, V, kHighSize, Dir, Comp,
                       false, // high
                       kHighIsPowerOf2>::merge(newK, newV);

#pragma unroll
      for (int i = 0; i < kHighSize; ++i) {
        k[i + kLowSize] = newK[i];
        v[i + kLowSize] = newV[i];
      }
    }
  }
};

// High recursion
template <typename K, typename V, int N, bool Dir, typename Comp>
struct BitonicMergeStep<K, V, N, Dir, Comp, false, false> {
  static inline __device__ void merge(K k[N], V v[N]) {
    static_assert(!utils::isPowerOf2(N), "must be non-power-of-2");
    static_assert(N >= 3, "must be N >= 3");

    constexpr int kNextHighestPowerOf2 = utils::nextHighestPowerOf2(N);

#pragma unroll
    for (int i = 0; i < N - kNextHighestPowerOf2 / 2; ++i) {
      K& ka = k[i];
      V& va = v[i];

      K& kb = k[i + kNextHighestPowerOf2 / 2];
      V& vb = v[i + kNextHighestPowerOf2 / 2];

      bool s = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
      swap(s, ka, kb);
      swap(s, va, vb);
    }

    constexpr int kLowSize = kNextHighestPowerOf2 / 2;
    constexpr int kHighSize = N - kNextHighestPowerOf2 / 2;
    {
      K newK[kLowSize];
      V newV[kLowSize];

#pragma unroll
      for (int i = 0; i < kLowSize; ++i) {
        newK[i] = k[i];
        newV[i] = v[i];
      }

      constexpr bool kLowIsPowerOf2 =
        utils::isPowerOf2(kNextHighestPowerOf2 / 2);
      // FIXME: compiler doesn't like this expression? compiler bug?
//      constexpr bool kLowIsPowerOf2 = utils::isPowerOf2(kLowSize);
      BitonicMergeStep<K, V, kLowSize, Dir, Comp,
                       true, // low
                       kLowIsPowerOf2>::merge(newK, newV);

#pragma unroll
      for (int i = 0; i < kLowSize; ++i) {
        k[i] = newK[i];
        v[i] = newV[i];
      }
    }

    {
      K newK[kHighSize];
      V newV[kHighSize];

#pragma unroll
      for (int i = 0; i < kHighSize; ++i) {
        newK[i] = k[i + kLowSize];
        newV[i] = v[i + kLowSize];
      }

      constexpr bool kHighIsPowerOf2 =
        utils::isPowerOf2(N - kNextHighestPowerOf2 / 2);
      // FIXME: compiler doesn't like this expression? compiler bug?
//      constexpr bool kHighIsPowerOf2 = utils::isPowerOf2(kHighSize);
      BitonicMergeStep<K, V, kHighSize, Dir, Comp,
                       false, // high
                       kHighIsPowerOf2>::merge(newK, newV);

#pragma unroll
      for (int i = 0; i < kHighSize; ++i) {
        k[i + kLowSize] = newK[i];
        v[i + kLowSize] = newV[i];
      }
    }
  }
};

/// Merges two sets of registers across the warp of any size;
/// i.e., merges a sorted k/v list of size kWarpSize * N1 with a
/// sorted k/v list of size kWarpSize * N2, where N1 and N2 are any
/// value >= 1
template <typename K,
          typename V,
          int N1,
          int N2,
          bool Dir,
          typename Comp,
          bool FullMerge = true>
inline __device__ void warpMergeAnyRegisters(K k1[N1], V v1[N1],
                                             K k2[N2], V v2[N2]) {
  constexpr int kSmallestN = N1 < N2 ? N1 : N2;

#pragma unroll
  for (int i = 0; i < kSmallestN; ++i) {
    K& ka = k1[N1 - 1 - i];
    V& va = v1[N1 - 1 - i];

    K& kb = k2[i];
    V& vb = v2[i];

    K otherKa;
    V otherVa;

    if (FullMerge) {
      // We need the other values
      otherKa = shfl_xor(ka, kWarpSize - 1);
      otherVa = shfl_xor(va, kWarpSize - 1);
    }

    K otherKb = shfl_xor(kb, kWarpSize - 1);
    V otherVb = shfl_xor(vb, kWarpSize - 1);

    // ka is always first in the list, so we needn't use our lane
    // in this comparison
    bool swapa = Dir ? Comp::gt(ka, otherKb) : Comp::lt(ka, otherKb);
    assign(swapa, ka, otherKb);
    assign(swapa, va, otherVb);

    // kb is always second in the list, so we needn't use our lane
    // in this comparison
    if (FullMerge) {
      bool swapb = Dir ? Comp::lt(kb, otherKa) : Comp::gt(kb, otherKa);
      assign(swapb, kb, otherKa);
      assign(swapb, vb, otherVa);

    } else {
      // We don't care about updating elements in the second list
    }
  }

  BitonicMergeStep<K, V, N1, Dir, Comp,
                   true, utils::isPowerOf2(N1)>::merge(k1, v1);
  if (FullMerge) {
    // Only if we care about N2 do we need to bother merging it fully
    BitonicMergeStep<K, V, N2, Dir, Comp,
                     false, utils::isPowerOf2(N2)>::merge(k2, v2);
  }
}

// Recursive template that uses the above bitonic merge to perform a
// bitonic sort
template <typename K, typename V, int N, bool Dir, typename Comp>
struct BitonicSortStep {
  static inline __device__ void sort(K k[N], V v[N]) {
    static_assert(N > 1, "did not hit specialized case");

    // Sort recursively
    constexpr int kSizeA = N / 2;
    constexpr int kSizeB = N - kSizeA;

    K aK[kSizeA];
    V aV[kSizeA];

#pragma unroll
    for (int i = 0; i < kSizeA; ++i) {
      aK[i] = k[i];
      aV[i] = v[i];
    }

    BitonicSortStep<K, V, kSizeA, Dir, Comp>::sort(aK, aV);

    K bK[kSizeB];
    V bV[kSizeB];

#pragma unroll
    for (int i = 0; i < kSizeB; ++i) {
      bK[i] = k[i + kSizeA];
      bV[i] = v[i + kSizeA];
    }

    BitonicSortStep<K, V, kSizeB, Dir, Comp>::sort(bK, bV);

    // Merge halves
    warpMergeAnyRegisters<K, V, kSizeA, kSizeB, Dir, Comp>(aK, aV, bK, bV);

#pragma unroll
    for (int i = 0; i < kSizeA; ++i) {
      k[i] = aK[i];
      v[i] = aV[i];
    }

#pragma unroll
    for (int i = 0; i < kSizeB; ++i) {
      k[i + kSizeA] = bK[i];
      v[i + kSizeA] = bV[i];
    }
  }
};

// Single warp (N == 1) sorting specialization
template <typename K, typename V, bool Dir, typename Comp>
struct BitonicSortStep<K, V, 1, Dir, Comp> {
  static inline __device__ void sort(K k[1], V v[1]) {
    // Update this code if this changes
    // should go from 1 -> kWarpSize in multiples of 2
    static_assert(kWarpSize == 32, "unexpected warp size");

    warpBitonicMergeLE16<K, V, 1, Dir, Comp, false>(k[0], v[0]);
    warpBitonicMergeLE16<K, V, 2, Dir, Comp, false>(k[0], v[0]);
    warpBitonicMergeLE16<K, V, 4, Dir, Comp, false>(k[0], v[0]);
    warpBitonicMergeLE16<K, V, 8, Dir, Comp, false>(k[0], v[0]);
    warpBitonicMergeLE16<K, V, 16, Dir, Comp, false>(k[0], v[0]);
  }
};

/// Sort a list of kWarpSize * N elements in registers, where N is an
/// arbitrary >= 1
template <typename K, typename V, int N, bool Dir, typename Comp>
inline __device__ void warpSortAnyRegisters(K k[N], V v[N]) {
  BitonicSortStep<K, V, N, Dir, Comp>::sort(k, v);
}

} } // namespace
