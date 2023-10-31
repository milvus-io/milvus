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

#ifndef INTERNAL_CORE_SRC_FNS_BASICDISTANCE_H_
#define INTERNAL_CORE_SRC_FNS_BASICDISTANCE_H_

namespace milvus::basicDistance {

template <typename Dat_Type>
float
basicL2(const Dat_Type*, const Dat_Type*, const unsigned int);

template <typename Dat_Type>
float
basicL2(const Dat_Type* vec1, const Dat_Type* vec2, const unsigned int dim) {
    float res = 0.0;
    Dat_Type* v1 = (Dat_Type*)vec1;
    Dat_Type* v2 = (Dat_Type*)vec2;
    for (unsigned int i = 0; i < dim; ++i) {
        float diff = (float)(*v1 - *v2);
        res += (float)(diff * diff);
        v1++;
        v2++;
    }
    return res;
}

/// ---- inner product

template <typename Dat_Type>
float
basicInnerProduct(const Dat_Type* vec1,
                  const Dat_Type* vec2,
                  const unsigned int dim);

template <typename Dat_Type>
float
basicInnerProduct(const Dat_Type* vec1,
                  const Dat_Type* vec2,
                  const unsigned int dim) {
    float res = 0.0;
    Dat_Type* v1 = (Dat_Type*)vec1;
    Dat_Type* v2 = (Dat_Type*)vec2;
    for (unsigned int i = 0; i < dim; ++i) {
        float product = (float)(*v1 * *v2);
        res += product;
        v1++;
        v2++;
    }
    return 1.0 - res;
}

}  // namespace milvus::basicDistance

#endif  // INTERNAL_CORE_SRC_FNS_BASICDISTANCE_H_
