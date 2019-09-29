// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "knowhere/common/Dataset.h"

namespace knowhere {

extern ArrayPtr
ConstructInt64ArraySmart(uint8_t* data, int64_t size);

extern ArrayPtr
ConstructFloatArraySmart(uint8_t* data, int64_t size);

extern TensorPtr
ConstructFloatTensorSmart(uint8_t* data, int64_t size, std::vector<int64_t> shape);

extern ArrayPtr
ConstructInt64Array(uint8_t* data, int64_t size);

extern ArrayPtr
ConstructFloatArray(uint8_t* data, int64_t size);

extern TensorPtr
ConstructFloatTensor(uint8_t* data, int64_t size, std::vector<int64_t> shape);

extern FieldPtr
ConstructInt64Field(const std::string& name);

extern FieldPtr
ConstructFloatField(const std::string& name);

}  // namespace knowhere
