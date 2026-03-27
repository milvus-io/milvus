#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace knowhere {

class KnowhereConfig {
 public:
    enum SimdType {
        AUTO = 0,
        AVX512,
        AVX2,
        SSE4_2,
        GENERIC,
    };

    enum ClusteringType {
        K_MEANS = 0,
        K_MEANS_PLUS_PLUS,
    };

    static void
    ShowVersion() {
    }

    static std::string
    SetSimdType(const SimdType simd_type) {
        current_simd_type_ = simd_type;
        switch (simd_type) {
            case AUTO:
                return "AUTO";
            case AVX512:
                return "AVX512";
            case AVX2:
                return "AVX2";
            case SSE4_2:
                return "SSE4_2";
            case GENERIC:
            default:
                return "GENERIC";
        }
    }

    static void
    SetBlasThreshold(const int64_t use_blas_threshold) {
        blas_threshold_ = use_blas_threshold;
    }

    static int64_t
    GetBlasThreshold() {
        return blas_threshold_;
    }

    static void
    SetEarlyStopThreshold(const double early_stop_threshold) {
        early_stop_threshold_ = early_stop_threshold;
    }

    static double
    GetEarlyStopThreshold() {
        return early_stop_threshold_;
    }

    static void
    SetClusteringType(const ClusteringType clustering_type) {
        clustering_type_ = clustering_type;
    }

    static bool
    SetAioContextPool(size_t) {
        return true;
    }

    static void
    EnablePatchForComputeFP32AsBF16() {
    }

    static void
    SetBuildThreadPoolSize(uint32_t num_threads) {
        build_thread_pool_size_ = num_threads;
    }

    static void
    SetSearchThreadPoolSize(uint32_t num_threads) {
        search_thread_pool_size_ = num_threads;
    }

    static void
    SetFetchThreadPoolSize(uint32_t num_threads) {
        fetch_thread_pool_size_ = num_threads;
    }

    static void
    InitGPUResource(int64_t, int64_t = 2) {
    }

    static void
    FreeGPUResource() {
    }

    static void
    SetRaftMemPool() {
    }

    static void
    SetRaftMemPool(size_t, size_t) {
    }

 private:
    static inline int64_t blas_threshold_ = 0;
    static inline double early_stop_threshold_ = 0.0;
    static inline SimdType current_simd_type_ = AUTO;
    static inline ClusteringType clustering_type_ = K_MEANS;
    static inline uint32_t build_thread_pool_size_ = 0;
    static inline uint32_t search_thread_pool_size_ = 0;
    static inline uint32_t fetch_thread_pool_size_ = 0;
};

}  // namespace knowhere
