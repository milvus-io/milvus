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

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "segcore/collection_c.h"
#include "common/status_c.h"

typedef void* CIndex;

// TODO: how could we pass map between go and c++ more efficiently?
// Solution: using protobuf instead of json, this way significantly increase programming efficiency

CStatus
CreateIndex(const char* serialized_type_params, const char* serialized_index_params, CIndex* res_index);

void
DeleteIndex(CIndex index);

CStatus
BuildFloatVecIndexWithoutIds(CIndex index, int64_t float_value_num, const float* vectors);

CStatus
BuildBinaryVecIndexWithoutIds(CIndex index, int64_t data_size, const uint8_t* vectors);

CStatus
SerializeToSlicedBuffer(CIndex index, int32_t* buffer_size, char** res_buffer);

void
LoadFromSlicedBuffer(CIndex index, const char* serialized_sliced_blob_buffer, int32_t size);

#ifdef __cplusplus
};
#endif
