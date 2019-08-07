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

// Coarse public API while the library is in development

#ifndef ARROW_API_H
#define ARROW_API_H

#include "arrow/array.h"                    // IYWU pragma: export
#include "arrow/buffer.h"                   // IYWU pragma: export
#include "arrow/builder.h"                  // IYWU pragma: export
#include "arrow/compare.h"                  // IYWU pragma: export
#include "arrow/extension_type.h"           // IYWU pragma: export
#include "arrow/memory_pool.h"              // IYWU pragma: export
#include "arrow/pretty_print.h"             // IYWU pragma: export
#include "arrow/record_batch.h"             // IYWU pragma: export
#include "arrow/status.h"                   // IYWU pragma: export
#include "arrow/table.h"                    // IYWU pragma: export
#include "arrow/table_builder.h"            // IYWU pragma: export
#include "arrow/tensor.h"                   // IYWU pragma: export
#include "arrow/type.h"                     // IYWU pragma: export
#include "arrow/util/config.h"              // IYWU pragma: export
#include "arrow/util/key_value_metadata.h"  // IWYU pragma: export
#include "arrow/visitor.h"                  // IYWU pragma: export

/// \brief Top-level namespace for Apache Arrow C++ API
namespace arrow {}

#endif  // ARROW_API_H
