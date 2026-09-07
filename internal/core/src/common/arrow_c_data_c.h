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

#pragma once

#include <stddef.h>
#include <stdint.h>

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

#ifndef MILVUS_GO_ARROW_C_DATA_HELPERS
#define MILVUS_GO_ARROW_C_DATA_HELPERS

static inline void
MilvusGoArrowSchemaRelease(struct ArrowSchema* schema) {
    if (schema != NULL && schema->release != NULL) {
        schema->release(schema);
    }
}

static inline int
MilvusGoArrowArrayIsReleased(const struct ArrowArray* array) {
    return array == NULL || array->release == NULL;
}

static inline void
MilvusGoArrowArrayRelease(struct ArrowArray* array) {
    if (!MilvusGoArrowArrayIsReleased(array)) {
        array->release(array);
    }
}

#endif  // MILVUS_GO_ARROW_C_DATA_HELPERS
