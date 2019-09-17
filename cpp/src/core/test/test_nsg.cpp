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


#include <fstream>
#include <iostream>
#include <utils.h>
#include "index.h"

//#include <gperftools/profiler.h>

using namespace zilliz::knowhere;

void load_data(std::string &filename, float *&data, unsigned &num,
               unsigned &dim) {  // load data with sift10K pattern
    std::ifstream in(filename, std::ios::binary);
    if (!in.is_open()) {
        std::cout << "open file error" << std::endl;
        exit(-1);
    }
    in.read((char *) &dim, 4);
    in.seekg(0, std::ios::end);
    std::ios::pos_type ss = in.tellg();
    size_t fsize = (size_t) ss;
    num = (unsigned) (fsize / (dim + 1) / 4);
    data = new float[(size_t) num * (size_t) dim];

    in.seekg(0, std::ios::beg);
    for (size_t i = 0; i < num; i++) {
        in.seekg(4, std::ios::cur);
        in.read((char *) (data + i * dim), dim * 4);
    }
    in.close();
}

void test_distance() {
    std::vector<float> xb{1, 2, 3, 4};
    std::vector<float> xq{2, 2, 3, 4};
    float r = calculate(xb.data(), xq.data(), 4);
    std::cout << r << std::endl;
}

int main() {
    test_distance();

    BuildParams params;
    params.search_length = 100;
    params.candidate_pool_size = 100;
    params.out_degree = 50;

    float *data = nullptr;
    long *ids = nullptr;
    unsigned ntotal, dim;
    std::string filename = "/home/zilliz/opt/workspace/wook/efanna_graph/tests/siftsmall/siftsmall_base.fvecs";
    //std::string filename = "/home/zilliz/opt/workspace/wook/efanna_graph/tests/sift/sift_base.fvecs";

    load_data(filename, data, ntotal, dim);
    assert(data);
    //float x = calculate(data + dim * 0, data + dim * 62, dim);
    //std::cout << x << std::endl;

    NsgIndex index(dim, ntotal);

    auto s = std::chrono::high_resolution_clock::now();
    index.Build_with_ids(ntotal, data, ids, params);
    auto e = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = e - s;
    std::cout << "indexing time: " << diff.count() << "\n";


    int k = 10;
    int nq = 1000;
    SearchParams s_params;
    s_params.search_length = 50;
    auto dist = new float[nq*k];
    auto ids_b = new long[nq*k];
    s = std::chrono::high_resolution_clock::now();
    //ProfilerStart("xx.prof");
    index.Search(data, nq, dim, k, dist, ids_b, s_params);
    //ProfilerStop();
    e = std::chrono::high_resolution_clock::now();
    diff = e - s;
    std::cout << "search time: " << diff.count() << "\n";

    for (int i = 0; i < k; ++i) {
        std::cout << "id " << ids_b[i] << std::endl;
        //std::cout << "dist " << dist[i] << std::endl;
    }

    delete[] dist;
    delete[] ids_b;

    return 0;
}


