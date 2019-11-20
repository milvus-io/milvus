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
generate_dataset(int64_t nb, int64_t dim, float* xb, int64_t* ids) {
    std::vector<int64_t> shape{nb, dim};
    auto tensor = knowhere::ConstructFloatTensor((uint8_t*)xb, nb * dim * sizeof(float), shape);
    std::vector<knowhere::TensorPtr> tensors{tensor};
    std::vector<knowhere::FieldPtr> tensor_fields{knowhere::ConstructFloatField("data")};
    auto tensor_schema = std::make_shared<knowhere::Schema>(tensor_fields);

    auto id_array = knowhere::ConstructInt64Array((uint8_t*)ids, nb * sizeof(int64_t));
    std::vector<knowhere::ArrayPtr> arrays{id_array};
    std::vector<knowhere::FieldPtr> array_fields{knowhere::ConstructInt64Field("id")};
    auto array_schema = std::make_shared<knowhere::Schema>(tensor_fields);

    auto dataset =
        std::make_shared<knowhere::Dataset>(std::move(arrays), array_schema, std::move(tensors), tensor_schema);
    return dataset;
}

knowhere::DatasetPtr
generate_query_dataset(int64_t nb, int64_t dim, float* xb) {
    std::vector<int64_t> shape{nb, dim};
    auto tensor = knowhere::ConstructFloatTensor((uint8_t*)xb, nb * dim * sizeof(float), shape);
    std::vector<knowhere::TensorPtr> tensors{tensor};
    std::vector<knowhere::FieldPtr> tensor_fields{knowhere::ConstructFloatField("data")};
    auto tensor_schema = std::make_shared<knowhere::Schema>(tensor_fields);

    auto dataset = std::make_shared<knowhere::Dataset>(std::move(tensors), tensor_schema);
    return dataset;
}

void
AssertAnns(const knowhere::DatasetPtr& result, const int& nq, const int& k) {
    auto ids = result->ids();
    for (auto i = 0; i < nq; i++) {
       EXPECT_EQ(i, *((int64_t*)(ids) + i * k));
        //        EXPECT_EQ(i, *(ids->data()->GetValues<int64_t>(1, i * k)));
    }
}

void
PrintResult(const knowhere::DatasetPtr& result, const int& nq, const int& k) {
    auto ids = result->ids();
    auto dists = result->dist();

    std::stringstream ss_id;
    std::stringstream ss_dist;
    for (auto i = 0; i < nq; i++) {
        for (auto j = 0; j < k; ++j) {
            //ss_id << *(ids->data()->GetValues<int64_t>(1, i * k + j)) << " ";
            //ss_dist << *(dists->data()->GetValues<float>(1, i * k + j)) << " ";
            ss_id << *((int64_t*)(ids) + i * k + j) << " ";
            ss_dist << *((float*)(dists) + i * k + j) << " ";
        }
        ss_id << std::endl;
        ss_dist << std::endl;
    }
    std::cout << "id\n" << ss_id.str() << std::endl;
    std::cout << "dist\n" << ss_dist.str() << std::endl;
}
