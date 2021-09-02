//  Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
//  with the License. You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the License
//  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
//  or implied. See the License for the specific language governing permissions and limitations under the License.
//

#ifdef MILVUS_APU_VERSION
#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <db/Utils.h>
#include <segment/DeletedDocs.h>
#include <segment/SegmentReader.h>

void
getDeletedDocs(std::string location, std::vector<milvus::segment::offset_t>& delete_docs);

int
getOffsetByValue(std::shared_ptr<std::vector<milvus::knowhere::IDType>> inp_vec,
                 milvus::engine::IDNumbers value_to_look, std::vector<uint32_t>& out_offsets);

#endif  //  MILVUS_APU_VERSION
