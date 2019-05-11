////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cuda.h>
#include <cuda_runtime.h>


namespace zilliz {
namespace vecwise {
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
