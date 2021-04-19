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

#include <gtest/gtest.h>
#include <segcore/ConcurrentVector.h>
#include "common/Types.h"
#include "common/Span.h"

TEST(Common, Span) {
    using namespace milvus;
    using namespace milvus::segcore;

    Span<float> s1(nullptr, 100);
    Span<FloatVector> s2(nullptr, 10, 16 * sizeof(float));
    SpanBase b1 = s1;
    SpanBase b2 = s2;
    auto r1 = static_cast<Span<float>>(b1);
    auto r2 = static_cast<Span<FloatVector>>(b2);
    ASSERT_EQ(r1.row_count(), 100);
    ASSERT_EQ(r2.row_count(), 10);
    ASSERT_EQ(r2.element_sizeof(), 16 * sizeof(float));
}
