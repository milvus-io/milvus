#pragma once
#ifndef NO_MANUAL_VECTORIZATION
#ifdef __SSE__
#define USE_SSE
#ifdef __AVX__
#define USE_AVX
#endif
#endif
#endif

#if defined(USE_AVX) || defined(USE_SSE)
#ifdef _MSC_VER
#include <intrin.h>
#include <stdexcept>
#else
#include <x86intrin.h>
#endif

#if defined(__GNUC__)
#define PORTABLE_ALIGN32 __attribute__((aligned(32)))
#else
#define PORTABLE_ALIGN32 __declspec(align(32))
#endif
#endif

#include <fstream>
#include <queue>
#include <vector>

#include <string.h>
#include <faiss/utils/BitsetView.h>

namespace hnswlib {
    typedef int64_t labeltype;

    template <typename T>
    class pairGreater {
    public:
        bool operator()(const T& p1, const T& p2) {
            return p1.first > p2.first;
        }
    };

    template<typename T>
    static void writeBinaryPOD(std::ostream &out, const T &podRef) {
        out.write((char *) &podRef, sizeof(T));
    }

    template<typename T>
    static void readBinaryPOD(std::istream &in, T &podRef) {
        in.read((char *) &podRef, sizeof(T));
    }

    template<typename T, typename W>
    static void writeBinaryPOD(W &out, const T &podRef) {
        out.write((char *) &podRef, sizeof(T));
    }

    template<typename T, typename R>
    static void readBinaryPOD(R &in, T &podRef) {
        in.read((char *) &podRef, sizeof(T));
    }

    template<typename MTYPE>
    using DISTFUNC = MTYPE(*)(const void *, const void *, const void *);


    template<typename MTYPE>
    class SpaceInterface {
    public:
        //virtual void search(void *);
        virtual size_t get_data_size() = 0;

        virtual DISTFUNC<MTYPE> get_dist_func() = 0;

        virtual void *get_dist_func_param() = 0;

        SpaceInterface() =default;

        virtual ~SpaceInterface() =default;
    };

    class StatisticsInfo {
    public:
        StatisticsInfo(): target_level(1) {}
        int target_level;
        std::vector<unsigned int> accessed_points;
    };

    template<typename dist_t>
    class AlgorithmInterface {
    public:
        virtual void addPoint(const void *datapoint, labeltype label)=0;

        virtual std::priority_queue<std::pair<dist_t, labeltype >>
        searchKnn(const void *, size_t, const faiss::BitsetView bitset, hnswlib::StatisticsInfo &stats) const = 0;

        virtual void saveIndex(const std::string &location)=0;
        virtual ~AlgorithmInterface(){
        }
    };
}

#include "space_l2.h"
#include "space_ip.h"
#include "bruteforce.h"
#include "hnswalg.h"

