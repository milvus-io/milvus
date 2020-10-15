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

#include <sys/time.h>
#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>

#include "knowhere/index/vector_index/fpga/utils.h"

size_t
GetFileSize(const char* filename) {
    size_t file_size;

    std::ifstream fin(filename, std::ifstream::in | std::ifstream::binary);
    if (fin.is_open()) {
        fin.seekg(0, std::ios::end);
        file_size = fin.tellg();
        fin.close();
    } else {
        std::printf("Error, failed to open %s\n", filename);
    }

    return file_size;
}

void
SplitFile(const char* filename, size_t file_size, char* buffer0, char* buffer1, char* buffer2, char* buffer3) {
    FILE* fp;
    char temp[64];

    fp = fopen(filename, "rb");
    assert(fp);

    int flag = 0;
    for (size_t i = 0; i < file_size / 64; i++) {
        memset(temp, 0x0, 64);
        fread(temp, 64, 1, fp);
        if (flag % 4 == 0) {
            memcpy(buffer0 + i / 4 * 64, temp, 64);
        } else if (flag % 4 == 1) {
            memcpy(buffer1 + i / 4 * 64, temp, 64);
        } else if (flag % 4 == 2) {
            memcpy(buffer2 + i / 4 * 64, temp, 64);
        } else if (flag % 4 == 3) {
            memcpy(buffer3 + i / 4 * 64, temp, 64);
        }

        flag++;
    }

    fclose(fp);
}

void
SplitMemory(char* buffer, size_t size, char* buffer0, char* buffer1, char* buffer2, char* buffer3) {
    int flag = 0;
    for (size_t i = 0; i < size / 64; i++) {
        if (flag % 4 == 0) {
            memcpy(buffer0 + i / 4 * 64, buffer + i * 64, 64);
        } else if (flag % 4 == 1) {
            memcpy(buffer1 + i / 4 * 64, buffer + i * 64, 64);
        } else if (flag % 4 == 2) {
            memcpy(buffer2 + i / 4 * 64, buffer + i * 64, 64);
        } else if (flag % 4 == 3) {
            memcpy(buffer3 + i / 4 * 64, buffer + i * 64, 64);
        }

        flag++;
    }
}

double
Elapsed() {
    struct timeval tv;

    gettimeofday(&tv, NULL);

    return tv.tv_sec + tv.tv_usec * 1e-6;
}
