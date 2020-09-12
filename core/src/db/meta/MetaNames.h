// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

namespace milvus::engine::meta {

extern const char* F_MAPPINGS;
extern const char* F_STATE;
extern const char* F_LSN;
extern const char* F_CREATED_ON;
extern const char* F_UPDATED_ON;
extern const char* F_ID;
extern const char* F_COLLECTON_ID;
extern const char* F_SCHEMA_ID;
extern const char* F_NUM;
extern const char* F_FTYPE;
extern const char* F_FETYPE;
extern const char* F_FIELD_ID;
extern const char* F_FIELD_ELEMENT_ID;
extern const char* F_PARTITION_ID;
extern const char* F_SEGMENT_ID;
extern const char* F_NAME;
extern const char* F_PARAMS;
extern const char* F_SIZE;
extern const char* F_ROW_COUNT;
extern const char* F_TYPE_NAME;

////////////////////////////////////////////////////////
// Table names
extern const char* TABLE_COLLECTION;
extern const char* TABLE_COLLECTION_COMMIT;
extern const char* TABLE_PARTITION;
extern const char* TABLE_PARTITION_COMMIT;
extern const char* TABLE_SEGMENT;
extern const char* TABLE_SEGMENT_COMMIT;
extern const char* TABLE_SEGMENT_FILE;
extern const char* TABLE_SCHEMA_COMMIT;
extern const char* TABLE_FIELD;
extern const char* TABLE_FIELD_COMMIT;
extern const char* TABLE_FIELD_ELEMENT;

}  // namespace milvus::engine::meta
