#include <gtest/gtest.h>
#include "knowhere/knowhere/common/Exception.h"
#include "knowhere/common/Dataset.h"
#include "utils.h"
#include "knowhere/common/Timer.h"

/*Some unittest for knowhere/common, mainly for improve code coverage.*/

TEST(COMMON_TEST, dataset_test) {
    knowhere::Dataset set;
    int64_t v1 = 111;

    set.Set("key1", v1);
    auto get_v1 = set.Get<int64_t>("key1");
    ASSERT_EQ(get_v1, v1);

    ASSERT_ANY_THROW(set.Get<int8_t>("key1"));
    ASSERT_ANY_THROW(set.Get<int64_t>("dummy"));
}

TEST(COMMON_TEST, knowhere_exception) {
    const std::string msg = "test";
    knowhere::KnowhereException ex(msg);
    ASSERT_EQ(ex.what(), msg);
}

TEST(COMMON_TEST, time_recoder) {
    InitLog();

    knowhere::TimeRecorder recoder("COMMTEST",0);
    sleep(1);
    double span = recoder.ElapseFromBegin("get time");
    ASSERT_GE(span,1.0);
}