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
#include "utils/LogUtil.h"

using namespace zilliz::milvus;

namespace {

static const std::string LOG_FILE_PATH = "./milvus/conf/log_config.conf";

using TimeUnit = server::TimeRecorder::TimeDisplayUnit;
double TestTimeRecorder(TimeUnit unit, int64_t log_level, int64_t sleep_ms) {
    server::TimeRecorder rc("test rc", unit, log_level);
    rc.Record("begin");
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    rc.Elapse("end");
    return rc.Span();
}

}

TEST(UtilTest, EXCEPTION_TEST) {
    std::string err_msg = "failed";
    server::ServerException ex(server::SERVER_UNEXPECTED_ERROR, err_msg);
    ASSERT_EQ(ex.error_code(), server::SERVER_UNEXPECTED_ERROR);
    std::string msg = ex.what();
    ASSERT_EQ(msg, err_msg);
}

TEST(UtilTest, COMMON_TEST) {
    unsigned long total_mem = 0, free_mem = 0;
    server::CommonUtil::GetSystemMemInfo(total_mem, free_mem);
    ASSERT_GT(total_mem, 0);
    ASSERT_GT(free_mem, 0);

    unsigned int thread_cnt = 0;
    server::CommonUtil::GetSystemAvailableThreads(thread_cnt);
    ASSERT_GT(thread_cnt, 0);

    std::string path1 = "/tmp/milvus_test/";
    std::string path2 = path1 + "common_test_12345/";
    std::string path3 = path2 + "abcdef";
    server::ServerError err = server::CommonUtil::CreateDirectory(path3);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    //test again
    err = server::CommonUtil::CreateDirectory(path3);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_TRUE(server::CommonUtil::IsDirectoryExist(path3));

    err = server::CommonUtil::DeleteDirectory(path1);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    //test again
    err = server::CommonUtil::DeleteDirectory(path1);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_FALSE(server::CommonUtil::IsDirectoryExist(path1));
    ASSERT_FALSE(server::CommonUtil::IsFileExist(path1));

    std::string exe_path = server::CommonUtil::GetExePath();
    ASSERT_FALSE(exe_path.empty());

    time_t tt;
    time( &tt );
    tm time_struct;
    memset(&time_struct, 0, sizeof(tm));
    server::CommonUtil::ConvertTime(tt, time_struct);
    ASSERT_GT(time_struct.tm_year, 0);
    ASSERT_GT(time_struct.tm_mon, 0);
    ASSERT_GT(time_struct.tm_mday, 0);
    server::CommonUtil::ConvertTime(time_struct, tt);
    ASSERT_GT(tt, 0);

    bool res = server::CommonUtil::TimeStrToTime("2019-03-23", tt, time_struct);
    ASSERT_EQ(time_struct.tm_year, 119);
    ASSERT_EQ(time_struct.tm_mon, 2);
    ASSERT_EQ(time_struct.tm_mday, 23);
    ASSERT_GT(tt, 0);
    ASSERT_TRUE(res);
}

TEST(UtilTest, STRINGFUNCTIONS_TEST) {
    std::string str = " test zilliz";
    server::StringHelpFunctions::TrimStringBlank(str);
    ASSERT_EQ(str, "test zilliz");

    str = "\"test zilliz\"";
    server::StringHelpFunctions::TrimStringQuote(str, "\"");
    ASSERT_EQ(str, "test zilliz");

    str = "a,b,c";
    std::vector<std::string> result;
    server::ServerError err = server::StringHelpFunctions::SplitStringByDelimeter(str , ",", result);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    ASSERT_EQ(result.size(), 3UL);

    result.clear();
    err = server::StringHelpFunctions::SplitStringByQuote(str , ",", "\"", result);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    ASSERT_EQ(result.size(), 3UL);

    result.clear();
    err = server::StringHelpFunctions::SplitStringByQuote(str , ",", "", result);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    ASSERT_EQ(result.size(), 3UL);

    str = "55,\"aa,gg,yy\",b";
    result.clear();
    err = server::StringHelpFunctions::SplitStringByQuote(str , ",", "\"", result);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    ASSERT_EQ(result.size(), 3UL);


}

TEST(UtilTest, TIMERECORDER_TEST) {
    double span = TestTimeRecorder(TimeUnit::eTimeAutoUnit, 0, 1001);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeAutoUnit, 0, 101);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeHourUnit, 1, 10);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeMinuteUnit, 2, 10);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeSecondUnit, 3, 10);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeMilliSecUnit, 4, 10);
    ASSERT_GT(span, 0.0);
    span = TestTimeRecorder(TimeUnit::eTimeMicroSecUnit, -1, 10);
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

    ASSERT_EQ(bq.Size(), 0);
}

TEST(UtilTest, LOG_TEST) {
    int32_t res = server::InitLog(LOG_FILE_PATH);
    ASSERT_EQ(res, 0);

    std::string fname = server::GetFileName(LOG_FILE_PATH);
    ASSERT_EQ(fname, "log_config.conf");
}
