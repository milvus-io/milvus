#include <gtest/gtest.h>
#include "test_utils/DataGen.h"
#include "knowhere/index/structured_index_simple/StructuredIndexSort.h"

TEST(Bitmap, Naive) {
    using namespace milvus;
    using namespace milvus::segcore;
    using namespace milvus::query;
    auto schema = std::make_shared<Schema>();
    schema->AddField("height", DataType::FLOAT);
    int N = 10000;
    auto raw_data = DataGen(schema, N);
    auto vec = raw_data.get_col<float>(0);
    auto sort_index = std::make_shared<knowhere::scalar::StructuredIndexSort<float>>();
    sort_index->Build(N, vec.data());
    {
        auto res = sort_index->Range(0, knowhere::scalar::OperatorType::LT);
        double count = res->count();
        ASSERT_NEAR(count / N, 0.5, 0.01);
    }
    {
        auto res = sort_index->Range(-1, false, 1, true);
        double count = res->count();
        ASSERT_NEAR(count / N, 0.682, 0.01);
    }
}