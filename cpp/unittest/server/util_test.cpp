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
#include "utils/ValidationUtil.h"
#include "db/engine/ExecutionEngine.h"

using namespace zilliz::milvus;

namespace {

static const char* LOG_FILE_PATH = "./milvus/conf/log_config.conf";

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

    EXPECT_FALSE(el::Loggers::hasFlag(el::LoggingFlag::NewLineForContainer));
    EXPECT_FALSE(el::Loggers::hasFlag(el::LoggingFlag::LogDetailedCrashReason));

    std::string fname = server::GetFileName(LOG_FILE_PATH);
    ASSERT_EQ(fname, "log_config.conf");
}

TEST(UtilTest, VALIDATE_TABLENAME_TEST) {
    std::string table_name = "Normal123_";
    server:: ServerError res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, server::SERVER_SUCCESS);

    table_name = "12sds";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, server::SERVER_INVALID_TABLE_NAME);

    table_name = "";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, server::SERVER_INVALID_TABLE_NAME);

    table_name = "_asdasd";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, server::SERVER_SUCCESS);

    table_name = "!@#!@";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, server::SERVER_INVALID_TABLE_NAME);

    table_name = "_!@#!@";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, server::SERVER_INVALID_TABLE_NAME);

    table_name = "中文";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, server::SERVER_INVALID_TABLE_NAME);

    table_name = std::string(10000, 'a');
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, server::SERVER_INVALID_TABLE_NAME);
}

TEST(UtilTest, VALIDATE_DIMENSIONTEST) {
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(-1), server::SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(0), server::SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(16385), server::SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(16384), server::SERVER_SUCCESS);
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(1), server::SERVER_SUCCESS);
}

TEST(UtilTest, VALIDATE_INDEX_TEST) {
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexType((int)engine::EngineType::INVALID), server::SERVER_INVALID_INDEX_TYPE);
    for(int i = 1; i <= (int)engine::EngineType::MAX_VALUE; i++) {
        ASSERT_EQ(server::ValidationUtil::ValidateTableIndexType(i), server::SERVER_SUCCESS);
    }
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexType((int)engine::EngineType::MAX_VALUE + 1), server::SERVER_INVALID_INDEX_TYPE);

    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexNlist(0), server::SERVER_INVALID_INDEX_NLIST);
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexNlist(100), server::SERVER_SUCCESS);

    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexFileSize(0), server::SERVER_INVALID_INDEX_FILE_SIZE);
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexFileSize(100), server::SERVER_SUCCESS);

    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexMetricType(0), server::SERVER_INVALID_INDEX_METRIC_TYPE);
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexMetricType(1), server::SERVER_SUCCESS);
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexMetricType(2), server::SERVER_SUCCESS);
}

TEST(ValidationUtilTest, ValidateGpuTest) {
    ASSERT_EQ(server::ValidationUtil::ValidateGpuIndex(0), server::SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateGpuIndex(100), server::SERVER_SUCCESS);

    size_t memory = 0;
    ASSERT_EQ(server::ValidationUtil::GetGpuMemory(0, memory), server::SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::GetGpuMemory(100, memory), server::SERVER_SUCCESS);
}

TEST(UtilTest, TIMERECORDER_TEST) {
    for(int64_t log_level = 0; log_level <= 6; log_level++) {
        if(log_level == 5) {
            continue; //skip fatal
        }
        server::TimeRecorder rc("time", log_level);
        rc.RecordSection("end");
    }
}
