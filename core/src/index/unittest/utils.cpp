// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "unittest/utils.h"
#include "knowhere/adapter/VectorAdapter.h"

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>

INITIALIZE_EASYLOGGINGPP

void
InitLog() {
    el::Configurations defaultConf;
    defaultConf.setToDefault();
    defaultConf.set(el::Level::Debug, el::ConfigurationType::Format, "[%thread-%datetime-%level]: %msg (%fbase:%line)");
    el::Loggers::reconfigureLogger("default", defaultConf);
}

void
DataGen::Init_with_default() {
    Generate(dim, nb, nq);
}

void
DataGen::Generate(const int& dim, const int& nb, const int& nq) {
    this->nb = nb;
    this->nq = nq;
    this->dim = dim;

    GenAll(dim, nb, xb, ids, nq, xq);
    assert(xb.size() == (size_t)dim * nb);
    assert(xq.size() == (size_t)dim * nq);

    base_dataset = generate_dataset(nb, dim, xb.data(), ids.data());
    query_dataset = generate_query_dataset(nq, dim, xq.data());
}

knowhere::DatasetPtr
DataGen::GenQuery(const int& nq) {
    xq.resize(nq * dim);
    for (int i = 0; i < nq * dim; ++i) {
        xq[i] = xb[i];
    }
    return generate_query_dataset(nq, dim, xq.data());
}

void
GenAll(const int64_t dim, const int64_t& nb, std::vector<float>& xb, std::vector<int64_t>& ids, const int64_t& nq,
       std::vector<float>& xq) {
    xb.resize(nb * dim);
    xq.resize(nq * dim);
    ids.resize(nb);
    GenAll(dim, nb, xb.data(), ids.data(), nq, xq.data());
}

void
GenAll(const int64_t& dim, const int64_t& nb, float* xb, int64_t* ids, const int64_t& nq, float* xq) {
    GenBase(dim, nb, xb, ids);
    for (int64_t i = 0; i < nq * dim; ++i) {
        xq[i] = xb[i];
    }
}

void
GenBase(const int64_t& dim, const int64_t& nb, float* xb, int64_t* ids) {
    for (auto i = 0; i < nb; ++i) {
        for (auto j = 0; j < dim; ++j) {
            // p_data[i * d + j] = float(base + i);
            xb[i * dim + j] = drand48();
        }
        xb[dim * i] += i / 1000.;
        ids[i] = i;
    }
}

FileIOReader::FileIOReader(const std::string& fname) {
    name = fname;
    fs = std::fstream(name, std::ios::in | std::ios::binary);
}

FileIOReader::~FileIOReader() {
    fs.close();
}

size_t
FileIOReader::operator()(void* ptr, size_t size) {
    fs.read(reinterpret_cast<char*>(ptr), size);
    return size;
}

FileIOWriter::FileIOWriter(const std::string& fname) {
    name = fname;
    fs = std::fstream(name, std::ios::out | std::ios::binary);
}

FileIOWriter::~FileIOWriter() {
    fs.close();
}

size_t
FileIOWriter::operator()(void* ptr, size_t size) {
    fs.write(reinterpret_cast<char*>(ptr), size);
    return size;
}

knowhere::DatasetPtr
generate_dataset(int64_t nb, int64_t dim, const float* xb, const int64_t* ids) {
    auto ret_ds = std::make_shared<knowhere::Dataset>();
    ret_ds->Set(knowhere::meta::ROWS, nb);
    ret_ds->Set(knowhere::meta::DIM, dim);
    ret_ds->Set(knowhere::meta::TENSOR, xb);
    ret_ds->Set(knowhere::meta::IDS, ids);
    return ret_ds;
}

knowhere::DatasetPtr
generate_query_dataset(int64_t nb, int64_t dim, const float* xb) {
    auto ret_ds = std::make_shared<knowhere::Dataset>();
    ret_ds->Set(knowhere::meta::ROWS, nb);
    ret_ds->Set(knowhere::meta::DIM, dim);
    ret_ds->Set(knowhere::meta::TENSOR, xb);
    return ret_ds;
}

void
AssertAnns(const knowhere::DatasetPtr& result, const int& nq, const int& k) {
    auto ids = result->Get<int64_t*>(knowhere::meta::IDS);
    for (auto i = 0; i < nq; i++) {
        EXPECT_EQ(i, *((int64_t*)(ids) + i * k));
        //        EXPECT_EQ(i, *(ids->data()->GetValues<int64_t>(1, i * k)));
    }
}

void
PrintResult(const knowhere::DatasetPtr& result, const int& nq, const int& k) {
    auto ids = result->Get<int64_t*>(knowhere::meta::IDS);
    auto dist = result->Get<float*>(knowhere::meta::DISTANCE);

    std::stringstream ss_id;
    std::stringstream ss_dist;
    for (auto i = 0; i < nq; i++) {
        for (auto j = 0; j < k; ++j) {
            // ss_id << *(ids->data()->GetValues<int64_t>(1, i * k + j)) << " ";
            // ss_dist << *(dists->data()->GetValues<float>(1, i * k + j)) << " ";
            ss_id << *((int64_t*)(ids) + i * k + j) << " ";
            ss_dist << *((float*)(dist) + i * k + j) << " ";
        }
        ss_id << std::endl;
        ss_dist << std::endl;
    }
    std::cout << "id\n" << ss_id.str() << std::endl;
    std::cout << "dist\n" << ss_dist.str() << std::endl;
}

void
Load_nns_graph(std::vector<std::vector<int64_t>>& final_graph, const char* filename) {
    std::vector<std::vector<unsigned>> knng;

    std::ifstream in(filename, std::ios::binary);
    unsigned k;
    in.read((char*)&k, sizeof(unsigned));
    in.seekg(0, std::ios::end);
    std::ios::pos_type ss = in.tellg();
    size_t fsize = (size_t)ss;
    size_t num = (size_t)(fsize / (k + 1) / 4);
    in.seekg(0, std::ios::beg);

    knng.resize(num);
    knng.reserve(num);
    int64_t kk = (k + 3) / 4 * 4;
    for (size_t i = 0; i < num; i++) {
        in.seekg(4, std::ios::cur);
        knng[i].resize(k);
        knng[i].reserve(kk);
        in.read((char*)knng[i].data(), k * sizeof(unsigned));
    }
    in.close();

    final_graph.resize(knng.size());
    for (int i = 0; i < knng.size(); ++i) {
        final_graph[i].resize(knng[i].size());
        for (int j = 0; j < knng[i].size(); ++j) {
            final_graph[i][j] = knng[i][j];
        }
    }
}

float*
fvecs_read(const char* fname, size_t* d_out, size_t* n_out) {
    FILE* f = fopen(fname, "r");
    if (!f) {
        fprintf(stderr, "could not open %s\n", fname);
        perror("");
        abort();
    }
    int d;
    fread(&d, 1, sizeof(int), f);
    assert((d > 0 && d < 1000000) || !"unreasonable dimension");
    fseek(f, 0, SEEK_SET);
    struct stat st;
    fstat(fileno(f), &st);
    size_t sz = st.st_size;
    assert(sz % ((d + 1) * 4) == 0 || !"weird file size");
    size_t n = sz / ((d + 1) * 4);

    *d_out = d;
    *n_out = n;
    float* x = new float[n * (d + 1)];
    size_t nr = fread(x, sizeof(float), n * (d + 1), f);
    assert(nr == n * (d + 1) || !"could not read whole file");

    // shift array to remove row headers
    for (size_t i = 0; i < n; i++) memmove(x + i * d, x + 1 + i * (d + 1), d * sizeof(*x));

    fclose(f);
    return x;
}

int*  // not very clean, but works as long as sizeof(int) == sizeof(float)
ivecs_read(const char* fname, size_t* d_out, size_t* n_out) {
    return (int*)fvecs_read(fname, d_out, n_out);
}
