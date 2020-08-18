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
#include <iostream>


#include "storage/IOReader.h"
#include "storage/IOWriter.h"
#include "storage/Operation.h"

namespace milvus {
namespace storage {

struct FSHandler {
    IOReaderPtr reader_ptr_ = nullptr;
    IOWriterPtr writer_ptr_ = nullptr;
    OperationPtr operation_ptr_ = nullptr;

    FSHandler(IOReaderPtr& reader_ptr, IOWriterPtr& writer_ptr, OperationPtr& operation_ptr)
        : reader_ptr_(reader_ptr), writer_ptr_(writer_ptr), operation_ptr_(operation_ptr) {
    }
};

using FSHandlerPtr = std::shared_ptr<FSHandler>;

}  // namespace storage
}  // namespace milvus
