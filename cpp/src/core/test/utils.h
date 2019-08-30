////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vector>
#include <cstdlib>
#include <cstdio>
#include <fstream>

#include "knowhere/adapter/structure.h"

class DataGen  {
 protected:
    void Init_with_default();

    void Generate(const int &dim, const int &nb, const int &nq);

    zilliz::knowhere::DatasetPtr GenQuery(const int&nq);

 protected:
    int nb = 10000;
    int nq = 10;
    int dim = 64;
    int k = 10;
    std::vector<float> xb;
    std::vector<float> xq;
    std::vector<int64_t> ids;
    zilliz::knowhere::DatasetPtr base_dataset = nullptr;
    zilliz::knowhere::DatasetPtr query_dataset = nullptr;
};


extern void GenAll(const int64_t dim,
                   const int64_t &nb,
                   std::vector<float> &xb,
                   std::vector<int64_t> &ids,
                   const int64_t &nq,
                   std::vector<float> &xq);

extern void GenAll(const int64_t &dim,
                   const int64_t &nb,
                   float *xb,
                   int64_t *ids,
                   const int64_t &nq,
                   float *xq);

extern void GenBase(const int64_t &dim,
                    const int64_t &nb,
                    float *xb,
                    int64_t *ids);

zilliz::knowhere::DatasetPtr
generate_dataset(int64_t nb, int64_t dim, float *xb, long *ids);

zilliz::knowhere::DatasetPtr
generate_query_dataset(int64_t nb, int64_t dim, float *xb);

struct FileIOWriter {
    std::fstream fs;
    std::string name;

    FileIOWriter(const std::string &fname);
    ~FileIOWriter();
    size_t operator()(void *ptr, size_t size);
};

struct FileIOReader {
    std::fstream fs;
    std::string name;

    FileIOReader(const std::string &fname);
    ~FileIOReader();
    size_t operator()(void *ptr, size_t size);
};

