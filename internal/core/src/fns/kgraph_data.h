// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef INTERNAL_CORE_SRC_FNS_KGRAPH_DATA_H_
#define INTERNAL_CORE_SRC_FNS_KGRAPH_DATA_H_

#include <cmath>
#ifndef __APPLE__
#include <malloc.h>
#endif
#include <memory>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <string>
#include <fstream>
#include <stdexcept>
#include <boost/assert.hpp>
#include <iostream>
#include <stdexcept>

#ifdef __GNUC__
#ifdef __AVX__
#define KGRAPH_MATRIX_ALIGN 32
#else
#ifdef __SSE2__
#define KGRAPH_MATRIX_ALIGN 16
#else
#define KGRAPH_MATRIX_ALIGN 4
#endif
#endif
#endif

namespace milvus {

constexpr float EPS = 1e-6;

struct Neighbor {
    uint32_t id;
    float dist;
    bool flag;  // whether this entry is a newly found one
    Neighbor() {
    }
    Neighbor(unsigned i, float d, bool f = true) : id(i), dist(d), flag(f) {
    }
    bool
    operator<(const Neighbor& other) const {
        if (fabs(this->dist - other.dist) < EPS)
            return this->id < other.id;
        return this->dist < other.dist;
    }
    bool
    operator==(const Neighbor& other) const {
        return this->id == other.id && (fabs(this->dist - other.dist) < EPS);
    }
    bool
    operator>=(const Neighbor& other) const {
        return !(*this < other);
    }
    bool
    operator<=(const Neighbor& other) const {
        return (*this == other) || (*this < other);
    }
    bool
    operator>(const Neighbor& other) const {
        return !(*this <= other);
    }
    bool
    operator!=(const Neighbor& other) const {
        return !(*this == other);
    }
};

using std::vector;

/// namespace for various distance metrics.
namespace metric {
/// L2 square distance.
struct l2sqr {
    template <typename T>
    /// L2 square distance.
    static float
    apply(T const* t1, T const* t2, unsigned dim) {
        float r = 0;
        for (unsigned i = 0; i < dim; ++i) {
            float v = float(t1[i]) - float(t2[i]);
            v *= v;
            r += v;
        }
        return r;
    }

    /// inner product.
    template <typename T>
    static float
    dot(T const* t1, T const* t2, unsigned dim) {
        float r = 0;
        for (unsigned i = 0; i < dim; ++i) {
            r += float(t1[i]) * float(t2[i]);
        }
        return r;
    }

    /// L2 norm.
    template <typename T>
    static float
    norm2(T const* t1, unsigned dim) {
        float r = 0;
        for (unsigned i = 0; i < dim; ++i) {
            float v = float(t1[i]);
            v *= v;
            r += v;
        }
        return r;
    }
};

struct l2 {
    template <typename T>
    static float
    apply(T const* t1, T const* t2, unsigned dim) {
        return sqrt(l2sqr::apply<T>(t1, t2, dim));
    }
};
}  // namespace metric

/// Matrix data.
template <typename T, unsigned A = KGRAPH_MATRIX_ALIGN>
class Matrix {
    unsigned col;
    unsigned row;
    size_t stride;
    char* data;

    void
    reset(unsigned r, unsigned c) {
        row = r;
        col = c;
        stride = (sizeof(T) * c + A - 1) / A * A;
        if (data)
            free(data);
#ifndef __APPLE__
        data = (char*)memalign(
            A, row * stride);  // SSE instruction needs data to be aligned
#else
        data = (char*)malloc(row * stride);
#endif
        if (!data)
            throw std::runtime_error("memalign");
    }

 public:
    Matrix() : col(0), row(0), stride(0), data(0) {
    }
    Matrix(unsigned r, unsigned c) : data(0) {
        reset(r, c);
    }
    ~Matrix() {
        if (data)
            free(data);
    }
    unsigned
    size() const {
        return row;
    }
    unsigned
    dim() const {
        return col;
    }
    size_t
    step() const {
        return stride;
    }
    void
    resize(unsigned r, unsigned c) {
        reset(r, c);
    }
    T const*
    operator[](unsigned i) const {
        return reinterpret_cast<T const*>(&data[stride * i]);
    }
    T*
    operator[](unsigned i) {
        return reinterpret_cast<T*>(&data[stride * i]);
    }
    void
    zero() {
        memset(data, 0, row * stride);
    }

    void
    normalize2() {
#pragma omp parallel for
        for (unsigned i = 0; i < row; ++i) {
            T* p = operator[](i);
            double sum = metric::l2sqr::norm2(p, col);
            sum = std::sqrt(sum);
            for (unsigned j = 0; j < col; ++j) {
                p[j] /= sum;
            }
        }
    }

    void
    load(const std::string& path,
         unsigned dim,
         unsigned skip = 0,
         unsigned gap = 0) {
        std::ifstream is(path.c_str(), std::ios::binary);
        if (!is)
            return;
        is.seekg(0, std::ios::end);
        size_t size = is.tellg();
        size -= skip;
        is.seekg(0, std::ios::beg);
        is.read((char*)&dim, sizeof(unsigned int));
        unsigned line = sizeof(T) * dim + gap;
        unsigned N = size / line;
        reset(N, dim);
        zero();
        is.seekg(skip, std::ios::beg);
        for (unsigned i = 0; i < N; ++i) {
            is.seekg(gap, std::ios::cur);
            is.read(&data[stride * i], sizeof(T) * dim);
        }
    }

    void
    load(const T* base_vector, size_t data_size, size_t data_dim) {
        reset(data_size, data_dim);
        zero();
        for (size_t i = 0; i < data_size; ++i) {
            memcpy(&data[stride * i],
                   base_vector + i * data_dim,
                   sizeof(T) * data_dim);
        }
    }
};

namespace kgraph {

class IndexOracle {
 public:
    /// Returns the size of the dataset.
    virtual unsigned
    size() const = 0;
    /// Computes similarity
    /**
         * 0 <= i, j < size() are the index of two objects in the dataset.
         * This method return the distance between objects i and j.
         */
    virtual float
    operator()(unsigned i, unsigned j) const = 0;
};
/// Search oracle
/** The search oracle is the user-supplied plugin that computes
     * the distance between the query and a arbitrary object in the dataset.
     * It is used for online k-NN search.
     */
class SearchOracle {
 public:
    /// Returns the size of the dataset.
    virtual unsigned
    size() const = 0;
    /// Computes similarity
    /**
         * 0 <= i < size() are the index of an objects in the dataset.
         * This method return the distance between the query and object i.
         */
    virtual float
    operator()(unsigned i) const = 0;
    /// Search with brutal force.
    /**
         * Search results are guaranteed to be ranked in ascending order of distance.
         *
         * @param K Return at most K nearest neighbors.
         * @param epsilon Only returns nearest neighbors within distance epsilon.
         * @param ids Pointer to the memory where neighbor IDs are returned.
         * @param dists Pointer to the memory where distance values are returned, can be nullptr.
         */
    unsigned
    search(unsigned K,
           float epsilon,
           unsigned* ids,
           float* dists = nullptr) const;
};
}  // namespace kgraph

/// Matrix proxy to interface with 3rd party libraries (FLANN, OpenCV, NumPy).
template <typename DATA_TYPE, unsigned A = KGRAPH_MATRIX_ALIGN>
class MatrixProxy {
    unsigned rows;
    unsigned cols;  // # elements, not bytes, in a row,
    size_t stride;  // # bytes in a row, >= cols * sizeof(element)
    uint8_t const* data;

 public:
    MatrixProxy(Matrix<DATA_TYPE> const& m)
        : rows(m.size()),
          cols(m.dim()),
          stride(m.step()),
          data(reinterpret_cast<uint8_t const*>(m[0])) {
    }

#ifndef __AVX__
#ifdef FLANN_DATASET_H_
    /// Construct from FLANN matrix.
    MatrixProxy(flann::Matrix<DATA_TYPE> const& m)
        : rows(m.rows), cols(m.cols), stride(m.stride), data(m.data) {
        if (stride % A)
            throw invalid_argument("bad alignment");
    }
#endif
#ifdef CV_MAJOR_VERSION
    /// Construct from OpenCV matrix.
    MatrixProxy(cv::Mat const& m)
        : rows(m.rows), cols(m.cols), stride(m.step), data(m.data) {
        if (stride % A)
            throw invalid_argument("bad alignment");
    }
#endif
#ifdef NPY_NDARRAYOBJECT_H
    /// Construct from NumPy matrix.
    MatrixProxy(PyArrayObject* obj) {
        if (!obj || (obj->nd != 2))
            throw invalid_argument("bad array shape");
        rows = obj->dimensions[0];
        cols = obj->dimensions[1];
        stride = obj->strides[0];
        data = reinterpret_cast<uint8_t const*>(obj->data);
        if (obj->descr->elsize != sizeof(DATA_TYPE))
            throw invalid_argument("bad data type size");
        if (stride % A)
            throw invalid_argument("bad alignment");
        if (!(stride >= cols * sizeof(DATA_TYPE)))
            throw invalid_argument("bad stride");
    }
#endif
#endif
    unsigned
    size() const {
        return rows;
    }
    unsigned
    dim() const {
        return cols;
    }
    DATA_TYPE const*
    operator[](unsigned i) const {
        return reinterpret_cast<DATA_TYPE const*>(data + stride * i);
    }
    DATA_TYPE*
    operator[](unsigned i) {
        return const_cast<DATA_TYPE*>(
            reinterpret_cast<DATA_TYPE const*>(data + stride * i));
    }
};

template <typename DATA_TYPE, typename DIST_TYPE>
class MatrixOracle : public kgraph::IndexOracle {
    MatrixProxy<DATA_TYPE> proxy;

 public:
    class SearchOracle : public kgraph::SearchOracle {
        MatrixProxy<DATA_TYPE> proxy;
        DATA_TYPE const* query;

     public:
        SearchOracle(MatrixProxy<DATA_TYPE> const& p, DATA_TYPE const* q)
            : proxy(p), query(q) {
        }
        virtual unsigned
        size() const {
            return proxy.size();
        }
        virtual float
        operator()(unsigned i) const {
            return DIST_TYPE::apply(proxy[i], query, proxy.dim());
        }
    };
    template <typename MATRIX_TYPE>
    MatrixOracle(MATRIX_TYPE const& m) : proxy(m) {
    }
    virtual unsigned
    size() const {
        return proxy.size();
    }
    virtual float
    operator()(unsigned i, unsigned j) const {
        return DIST_TYPE::apply(proxy[i], proxy[j], proxy.dim());
    }
    SearchOracle
    query(DATA_TYPE const* query) const {
        return SearchOracle(proxy, query);
    }
};

}  // namespace milvus

#endif  // INTERNAL_CORE_SRC_FNS_KGRAPH_DATA_H_