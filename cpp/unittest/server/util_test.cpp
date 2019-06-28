////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>

#include "utils/CommonUtil.h"
#include "utils/Error.h"
#include "utils/StringHelpFunctions.h"
#include "utils/TimeRecorder.h"
#include "utils/BlockingQueue.h"

using namespace zilliz::milvus;

namespace {

using TimeUnit = server::TimeRecorder::TimeDisplayUnit;
double TestTimeRecorder(TimeUnit unit, int64_t log_level) {
    server::TimeRecorder rc("test rc", unit, log_level);
    rc.Record("begin");
    std::this_thread::sleep_for(std::chrono::microseconds(10));
    rc.Elapse("end");
    return rc.Span();
}

}

TEST(CommonTest, COMMON_TEST) {
    std::string path1 = "/tmp/milvus_test/";
    std::string path2 = path1 + "common_test_12345/";
    std::string path3 = path2 + "abcdef";
    server::ServerError err = server::CommonUtil::CreateDirectory(path3);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_TRUE(server::CommonUtil::IsDirectoryExit(path3));

    err = server::CommonUtil::DeleteDirectory(path1);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_FALSE(server::CommonUtil::IsDirectoryExit(path1));
}

TEST(UtilTest, STRINGFUNCTIONS_TEST) {
    std::string str = " test zilliz";
    server::StringHelpFunctions::TrimStringBlank(str);
    ASSERT_EQ(str, "test zilliz");

    str = "a,b,c";
    std::vector<std::string> result;
    server::StringHelpFunctions::SplitStringByDelimeter(str , ",", result);
    ASSERT_EQ(result.size(), 3UL);

    str = "55,\"aa,gg,yy\",b";
    result.clear();
    server::StringHelpFunctions::SplitStringByQuote(str , ",", "\"", result);
    ASSERT_EQ(result.size(), 3UL);
}

TEST(UtilTest, TIMERECORDER_TEST) {
    double span = TestTimeRecorder(TimeUnit::eTimeAutoUnit, 0);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeHourUnit, 1);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeMinuteUnit, 2);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeSecondUnit, 3);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeMilliSecUnit, 4);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeMicroSecUnit, -1);
    ASSERT_GT(span, 0.0);
}

TEST(UtilTest, BLOCKINGQUEUE_TEST) {
    server::BlockingQueue<std::string> bq;

    static const size_t count = 10;
    bq.SetCapacity(count);

    for(size_t i = 1; i <= count; i++) {
        std::string id = "No." + std::to_string(i);
        bq.Put(id);
    }

    ASSERT_EQ(bq.Size(), count);
    ASSERT_FALSE(bq.Empty());

    std::string str = bq.Front();
    ASSERT_EQ(str, "No.1");

    str = bq.Back();
    ASSERT_EQ(str, "No." + std::to_string(count));

    for(size_t i = 1; i <= count; i++) {
        std::string id = "No." + std::to_string(i);
        str = bq.Take();
        ASSERT_EQ(id, str);
    }
}

