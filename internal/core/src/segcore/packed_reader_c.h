// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <arrow/c/abi.h>

typedef void* CPackedReader;
typedef void* CArrowArray;
typedef void* CArrowSchema;

/**
 * @brief Open a packed reader to read needed columns in the specified path.
 *
 * @param path The root path of the packed files to read.
 * @param schema The original schema of data.
 * @param buffer_size The max buffer size of the packed reader.
 * @param c_packed_reader The output pointer of the packed reader.
 */
int
NewPackedReader(const char* path,
                struct ArrowSchema* schema,
                const int64_t buffer_size,
                CPackedReader* c_packed_reader);

/**
 * @brief Read the next record batch from the packed reader.
 *        By default, the maximum return batch is 1024 rows.
 *
 * @param c_packed_reader The packed reader to read.
 * @param out_array The output pointer of the arrow array.
 * @param out_schema The output pointer of the arrow schema.
 */
int
ReadNext(CPackedReader c_packed_reader,
         CArrowArray* out_array,
         CArrowSchema* out_schema);

/**
 * @brief Close the packed reader and release the resources.
 *
 * @param c_packed_reader The packed reader to close.
 */
int
CloseReader(CPackedReader c_packed_reader);

#ifdef __cplusplus
}
#endif