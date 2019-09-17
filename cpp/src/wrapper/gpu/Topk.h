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

#include <cuda.h>
#include <cuda_runtime.h>


namespace zilliz {
namespace milvus {
namespace engine {
namespace gpu {

template<typename T>
void
TopK(T *input,
     int length,
     int k,
     T *output,
     int64_t *indices,
//     Ordering order_flag,
     cudaStream_t stream = nullptr);


#define INSTANTIATION_TOPK_2(T) \
    template void \
    TopK<T>(T *input, \
          int length, \
          int k, \
          T *output, \
          int64_t *indices, \
          cudaStream_t stream)
//          Ordering order_flag, \
//          cudaStream_t stream)

//extern INSTANTIATION_TOPK_2(int8_t);
//extern INSTANTIATION_TOPK_2(int16_t);
extern INSTANTIATION_TOPK_2(int32_t);
//extern INSTANTIATION_TOPK_2(int64_t);
extern INSTANTIATION_TOPK_2(float);
//extern INSTANTIATION_TOPK_2(double);
//extern INSTANTIATION_TOPK(TimeInterval);
//extern INSTANTIATION_TOPK(Float128);

}

// User Interface.
void TopK(float *input,
          int length,
          int k,
          float *output,
          int64_t *indices);


}
}
}
