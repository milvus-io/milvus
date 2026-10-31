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

#pragma once

#include <memory>
#include <vector>

#include "SimpleNumericAggregate.h"

namespace milvus {
namespace exec {

// (count, mean, M2) accumulator for var_pop, var_samp, stddev_pop and
// stddev_samp. It emits only M2, the sum of squared deviations from the mean.
// The proxy plans sum and count beside it and merges partial states with
// Chan's formula.
template <typename TInput>
class M2Aggregate : public SimpleNumericAggregate<TInput, double, double> {
    using BaseAggregate = SimpleNumericAggregate<TInput, double, double>;

    struct Accumulator {
        int64_t count;
        double mean;
        double m2;
    };

 public:
    M2Aggregate() : BaseAggregate(DataType::DOUBLE) {
    }

    int32_t
    accumulatorFixedWidthSize() const override {
        return sizeof(Accumulator);
    }

    int32_t
    accumulatorAlignmentSize() const override {
        return alignof(Accumulator);
    }

    void
    extractValues(char** groups,
                  int32_t numGroups,
                  VectorPtr* result) override {
        BaseAggregate::doExtractValues(
            groups, numGroups, result, [&](char* group) {
                return Aggregate::value<Accumulator>(group)->m2;
            });
    }

    void
    addRawInput(char** groups,
                int numGroups,
                const std::vector<VectorPtr>& input) override {
        auto column = inputColumn(input);
        for (auto i = 0; i < column->size(); i++) {
            if (column->ValidAt(i)) {
                merge(groups[i],
                      1,
                      static_cast<double>(column->ValueAt<TInput>(i)),
                      0);
            }
        }
    }

    void
    addSingleGroupRawInput(char* group,
                           int64_t numRows,
                           const std::vector<VectorPtr>& input) override {
        // 2 passes over the batch avoid a division per row.
        auto column = inputColumn(input);
        int64_t n = 0;
        double sum = 0;
        for (auto i = 0; i < column->size(); i++) {
            if (column->ValidAt(i)) {
                sum += static_cast<double>(column->ValueAt<TInput>(i));
                n++;
            }
        }
        if (n == 0) {
            return;
        }
        auto mean = sum / static_cast<double>(n);
        double m2 = 0;
        for (auto i = 0; i < column->size(); i++) {
            if (column->ValidAt(i)) {
                auto d = static_cast<double>(column->ValueAt<TInput>(i)) - mean;
                m2 += d * d;
            }
        }
        merge(group, n, mean, m2);
    }

 protected:
    void
    initializeNewGroupsInternal(
        char** groups, folly::Range<const vector_size_t*> indices) override {
        Aggregate::setAllNulls(groups, indices);
        for (auto i : indices) {
            *Aggregate::value<Accumulator>(groups[i]) = {};
        }
    }

 private:
    static ColumnVectorPtr
    inputColumn(const std::vector<VectorPtr>& input) {
        auto column = std::dynamic_pointer_cast<ColumnVector>(input[0]);
        AssertInfo(column != nullptr,
                   "input column for m2 aggregation must be ColumnVector");
        return column;
    }

    // Chan's formula. A single value (n = 1, m2 = 0) is Welford's update.
    void
    merge(char* group, int64_t n, double mean, double m2) {
        Aggregate::clearNull(group);
        auto* acc = Aggregate::value<Accumulator>(group);
        auto count = acc->count + n;
        auto delta = mean - acc->mean;
        auto weight = static_cast<double>(n) / static_cast<double>(count);
        acc->m2 +=
            m2 + delta * delta * static_cast<double>(acc->count) * weight;
        acc->mean += delta * weight;
        acc->count = count;
    }
};

void
registerM2Aggregate();

}  // namespace exec
}  // namespace milvus
