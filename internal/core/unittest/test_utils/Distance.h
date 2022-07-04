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

namespace {
float
L2(const float* point_a, const float* point_b, int dim) {
    float dis = 0;
    for (auto i = 0; i < dim; i++) {
        auto c_a = point_a[i];
        auto c_b = point_b[i];
        dis += pow(c_b - c_a, 2);
    }
    return dis;
}

float
IP(const float* point_a, const float* point_b, int dim) {
    float dis = 0;
    for (auto i = 0; i < dim; i++) {
        auto c_a = point_a[i];
        auto c_b = point_b[i];
        dis += c_a * c_b;
    }
    return dis;
}

}  // namespace
