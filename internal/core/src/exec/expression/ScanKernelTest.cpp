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

#include <gtest/gtest.h>

#include <cstdint>

#include "exec/expression/ScanKernel.h"

using namespace milvus;
using namespace milvus::exec;

namespace {

struct PlainKernel {
    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<int64_t>& batch, TriStateOut out) const {
        for (size_t i = 0; i < batch.size; ++i) {
            if (batch.data[i] > 0) {
                out.SetTrue(i);
            }
        }
    }
};

struct SkippingKernel : PlainKernel {
    bool
    CanSkip(const SkipIndex&, FieldId, int64_t chunk_id) const {
        return chunk_id == 0;
    }
};

struct OverflowKernel : PlainKernel {
    bool
    AlwaysFalse() const {
        return true;
    }

    bool
    AlwaysTrue() const {
        return false;
    }
};

struct OffsetKernel : PlainKernel {
    static constexpr bool kNeedsSegmentOffsets = true;
};

struct ExistsLikeKernel : PlainKernel {
    static constexpr bool kNullRowsKnownFalse = true;
};

struct MissingRandomKernel {
    void
    Eval(const CandidateBatch<int64_t>&, TriStateOut) {
    }
};

}  // namespace

TEST(ScanKernelTest, ConceptsDescribeKernelCapabilities) {
    EXPECT_TRUE((ScanKernel<PlainKernel, int64_t>));
    EXPECT_FALSE((ScanKernel<MissingRandomKernel, int64_t>));

    EXPECT_FALSE((KernelCanSkip<PlainKernel>));
    EXPECT_TRUE((KernelCanSkip<SkippingKernel>));

    EXPECT_FALSE((KernelAlwaysFalse<PlainKernel>));
    EXPECT_TRUE((KernelAlwaysFalse<OverflowKernel>));
    EXPECT_FALSE((KernelAlwaysTrue<PlainKernel>));
    EXPECT_TRUE((KernelAlwaysTrue<OverflowKernel>));

    EXPECT_FALSE(kKernelNeedsSegmentOffsets<PlainKernel>);
    EXPECT_TRUE(kKernelNeedsSegmentOffsets<OffsetKernel>);
    EXPECT_TRUE(kKernelNeedsSegmentOffsets<const OffsetKernel&>);

    EXPECT_FALSE(kKernelNullRowsKnownFalse<PlainKernel>);
    EXPECT_TRUE(kKernelNullRowsKnownFalse<ExistsLikeKernel>);
}

TEST(ScanKernelTest, TriStateSettersEncodeThreeValues) {
    TargetBitmap match(3, false);
    TargetBitmap known(3, true);
    TriStateOut out{TargetBitmapView(match), TargetBitmapView(known)};

    out.SetTrue(0);
    out.SetFalse(1);
    out.SetUnknown(2);

    EXPECT_TRUE(match[0]);
    EXPECT_TRUE(known[0]);
    EXPECT_FALSE(match[1]);
    EXPECT_TRUE(known[1]);
    EXPECT_FALSE(match[2]);
    EXPECT_FALSE(known[2]);
}

TEST(ScanKernelTest, SliceWritesThroughToParent) {
    TargetBitmap match(8, false);
    TargetBitmap known(8, true);
    TriStateOut out{TargetBitmapView(match), TargetBitmapView(known)};

    auto tail = out.Slice(4, 4);
    tail.SetTrue(0);
    tail.SetUnknown(3);

    EXPECT_TRUE(match[4]);
    EXPECT_FALSE(known[7]);
    EXPECT_EQ(match.count(), 1);
    EXPECT_EQ(known.count(), 7);
}

TEST(ScanKernelTest, CandidateBatchWithoutMaskTreatsEveryRowAsCandidate) {
    int64_t values[3] = {1, 2, 3};
    CandidateBatch<int64_t> batch;
    batch.data = values;
    batch.size = 3;
    EXPECT_TRUE(batch.IsCandidate(0));
    EXPECT_TRUE(batch.IsCandidate(2));

    TargetBitmap mask(3, false);
    mask[1] = true;
    batch.candidates = TargetBitmapView(mask);
    EXPECT_FALSE(batch.IsCandidate(0));
    EXPECT_TRUE(batch.IsCandidate(1));
}
