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

#ifndef ARROW_COMPUTE_API_H
#define ARROW_COMPUTE_API_H

#include "arrow/compute/context.h"  // IWYU pragma: export
#include "arrow/compute/kernel.h"   // IWYU pragma: export

#include "arrow/compute/kernels/boolean.h"  // IWYU pragma: export
#include "arrow/compute/kernels/cast.h"     // IWYU pragma: export
#include "arrow/compute/kernels/compare.h"  // IWYU pragma: export
#include "arrow/compute/kernels/count.h"    // IWYU pragma: export
#include "arrow/compute/kernels/hash.h"     // IWYU pragma: export
#include "arrow/compute/kernels/mean.h"     // IWYU pragma: export
#include "arrow/compute/kernels/sum.h"      // IWYU pragma: export
#include "arrow/compute/kernels/take.h"     // IWYU pragma: export

#endif  // ARROW_COMPUTE_API_H
