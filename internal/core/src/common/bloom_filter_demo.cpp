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

#include <array>
#include <cstring>
#include <cstdio>
#include <fstream>
#include <string_view>
#include <vector>

#include "common/EasyAssert.h"
#include "simdjson/padded_string.h"
#include "simdjson/ondemand.h"

#include "common/Json.h"
#include "common/bloom_filter.h"
#include "common/bloom_filter_block.h"

// Read bf files which marshall from go storage PrimaryKeyStats from bf_path
// And read test data from pk_path
int main(int argc, char* argv[]){
    auto bf_path = argv[1], pk_path = argv[2];
    auto file = std::ifstream(bf_path, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening file: " << bf_path << "\n";
        return 1;
    }

    std::cout << "bf_path: "<< bf_path << "\n";
    std::cout << "pk_path: "<< pk_path << "\n";

    // Seek to end to determine file size
    file.seekg(0, std::ios::end);
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);

    // Allocate buffer and read file
    std::vector<char> buffer(size);
    if (file.read(buffer.data(), size)) {
        std::cout << "File read successfully! Size: " << size << " bytes" << "\n";
    } else {
        std::cerr << "Error reading file!" << "\n";
    }
    
    auto bf = milvus::unmarshal_bloom_filter(buffer.data(), buffer.size(), "");

    // load test data
    FILE* pk_file = fopen(pk_path, "r"); // Open file for reading
    if (!pk_file) {
        perror("Error opening file");
        return 1;
    }

    int64_t pk;
    while (std::fscanf(pk_file, "%ld\n", &pk) != EOF) { // Read pks from file
        printf("Read pk: %ld\n", pk);
        auto res = bf->test_int64(pk);
        Assert(res);
    }

    return 0;
}