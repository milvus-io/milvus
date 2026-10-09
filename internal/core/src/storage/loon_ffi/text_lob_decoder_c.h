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

#include <stdint.h>

#include "common/common_type_c.h"
#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowArray;

// Decodes physical Binary TEXT references into a separate logical String
// array. Neither the input references nor their LOB files are modified.
typedef void* CTextLOBDecoder;

CStatus
NewTextLOBDecoder(int64_t field_id,
                  const char* lob_base_path,
                  CStorageConfig storage_config,
                  CTextLOBDecoder* out_decoder);
CStatus
DecodeTextLOB(CTextLOBDecoder decoder,
              struct ArrowArray* references,
              struct ArrowArray* out_strings);
CStatus
CloseTextLOBDecoder(CTextLOBDecoder decoder);

#ifdef __cplusplus
}
#endif
