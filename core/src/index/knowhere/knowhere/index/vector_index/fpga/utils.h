// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#ifndef UTILIS_H
#define UTILIS_H

#include <iostream>

#include <algorithm>
#include <numeric>
#include <set>
#include <vector>

size_t
GetFileSize(const char* filename);
void
SplitFile(const char* filename, size_t file_size, char* buffer0, char* buffer1, char* buffer2, char* buffer3);
void
SplitMemory(char* buffer, size_t size, char* buffer0, char* buffer1, char* buffer2, char* buffer3);

template <typename T>
void
ShowVecValues(std::vector<T>& vec, int col) {
    auto iter = vec.begin();
    int index = 0;
    while (iter != vec.end()) {
        std::cout << *iter << " ";
        iter++;
        index++;

        if (index == col) {
            std::cout << "\n";
            index = 0;
        }
    }
    std::cout << std::endl;
}

double
Elapsed();
#endif
