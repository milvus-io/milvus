// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>

namespace milvus::index {

// Samples come from unlocked catch-up slices. Null-only work measures the
// logical mapping cost, but provides no evidence about vector Add throughput.
struct GrowingIndexCatchupCost {
    int64_t logical_rows = 0;
    double elapsed_ms = 0;
    int64_t payload_rows = 0;
    double payload_ms = 0;

    void
    Observe(int64_t logical, int64_t physical, double ms) {
        logical_rows += logical;
        elapsed_ms += ms;
        if (physical > 0) {
            payload_rows += physical;
            payload_ms += ms;
        }
    }

    double
    EstimateMs(int64_t logical, int64_t physical) const {
        // Each rate measures the whole slice. Both estimates must fit,
        // without counting the same elapsed time twice for non-nullable data.
        return std::max(Estimate(logical, logical_rows, elapsed_ms),
                        Estimate(physical, payload_rows, payload_ms));
    }

 private:
    static double
    Estimate(int64_t rows, int64_t sampled_rows, double ms) {
        if (rows == 0) {
            return 0;
        }
        return sampled_rows > 0 && ms > 0
                   ? rows * ms / sampled_rows
                   : std::numeric_limits<double>::infinity();
    }
};

}  // namespace milvus::index
