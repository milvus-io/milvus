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

#include "SegmentInterface.h"

// class SegmentSealed : public SegmentInternalInterface {
//  public:
//     const Schema& get_schema() = 0;
//     int64_t get_num_chunk() = 0;
//
//     explicit SegmentSealed(SchemaPtr schema);
//     void set_size();
//     void load_data(FieldId field_id, void* blob, int64_t blob_size);
//
//
//  private:
//     SchemaPtr schema_;
// }
