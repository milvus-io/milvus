////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>
#include <easylogging++.h>

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
    server::ServerException ex(SERVER_UNEXPECTED_ERROR, err_msg);
    ASSERT_EQ(ex.error_code(), SERVER_UNEXPECTED_ERROR);
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
    ErrorCode err = server::CommonUtil::CreateDirectory(path3);
    ASSERT_EQ(err, SERVER_SUCCESS);
    //test again
    err = server::CommonUtil::CreateDirectory(path3);
    ASSERT_EQ(err, SERVER_SUCCESS);

    ASSERT_TRUE(server::CommonUtil::IsDirectoryExist(path3));

    err = server::CommonUtil::DeleteDirectory(path1);
    ASSERT_EQ(err, SERVER_SUCCESS);
    //test again
    err = server::CommonUtil::DeleteDirectory(path1);
    ASSERT_EQ(err, SERVER_SUCCESS);

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
    ErrorCode err = server::StringHelpFunctions::SplitStringByDelimeter(str , ",", result);
    ASSERT_EQ(err, SERVER_SUCCESS);
    ASSERT_EQ(result.size(), 3UL);

    result.clear();
    err = server::StringHelpFunctions::SplitStringByQuote(str , ",", "\"", result);
    ASSERT_EQ(err, SERVER_SUCCESS);
    ASSERT_EQ(result.size(), 3UL);

    result.clear();
    err = server::StringHelpFunctions::SplitStringByQuote(str , ",", "", result);
    ASSERT_EQ(err, SERVER_SUCCESS);
    ASSERT_EQ(result.size(), 3UL);

    str = "55,\"aa,gg,yy\",b";
    result.clear();
    err = server::StringHelpFunctions::SplitStringByQuote(str , ",", "\"", result);
    ASSERT_EQ(err, SERVER_SUCCESS);
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

TEST(UtilTest, TIMERECORDER_TEST) {
    for(int64_t log_level = 0; log_level <= 6; log_level++) {
        if(log_level == 5) {
            continue; //skip fatal
        }
        server::TimeRecorder rc("time", log_level);
        rc.RecordSection("end");
    }
}

TEST(ValidationUtilTest, VALIDATE_TABLENAME_TEST) {
    std::string table_name = "Normal123_";
    ErrorCode res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_SUCCESS);

    table_name = "12sds";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "_asdasd";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_SUCCESS);

    table_name = "!@#!@";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "_!@#!@";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "中文";
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = std::string(10000, 'a');
    res = server::ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);
}

TEST(ValidationUtilTest, VALIDATE_DIMENSION_TEST) {
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(-1), SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(0), SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(16385), SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(16384), SERVER_SUCCESS);
    ASSERT_EQ(server::ValidationUtil::ValidateTableDimension(1), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, VALIDATE_INDEX_TEST) {
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexType((int)engine::EngineType::INVALID), SERVER_INVALID_INDEX_TYPE);
    for(int i = 1; i <= (int)engine::EngineType::MAX_VALUE; i++) {
        ASSERT_EQ(server::ValidationUtil::ValidateTableIndexType(i), SERVER_SUCCESS);
    }
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexType((int)engine::EngineType::MAX_VALUE + 1), SERVER_INVALID_INDEX_TYPE);

    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexNlist(0), SERVER_INVALID_INDEX_NLIST);
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexNlist(100), SERVER_SUCCESS);

    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexFileSize(0), SERVER_INVALID_INDEX_FILE_SIZE);
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexFileSize(100), SERVER_SUCCESS);

    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexMetricType(0), SERVER_INVALID_INDEX_METRIC_TYPE);
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexMetricType(1), SERVER_SUCCESS);
    ASSERT_EQ(server::ValidationUtil::ValidateTableIndexMetricType(2), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, VALIDATE_TOPK_TEST) {
    engine::meta::TableSchema schema;
    ASSERT_EQ(server::ValidationUtil::ValidateSearchTopk(10, schema), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateSearchTopk(65536, schema), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateSearchTopk(0, schema), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, VALIDATE_NPROBE_TEST) {
    engine::meta::TableSchema schema;
    schema.nlist_ = 100;
    ASSERT_EQ(server::ValidationUtil::ValidateSearchNprobe(10, schema), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateSearchNprobe(0, schema), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateSearchNprobe(101, schema), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, VALIDATE_GPU_TEST) {
    ASSERT_EQ(server::ValidationUtil::ValidateGpuIndex(0), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateGpuIndex(100), SERVER_SUCCESS);

    size_t memory = 0;
    ASSERT_EQ(server::ValidationUtil::GetGpuMemory(0, memory), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::GetGpuMemory(100, memory), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, VALIDATE_IPADDRESS_TEST) {
    ASSERT_EQ(server::ValidationUtil::ValidateIpAddress("127.0.0.1"), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateIpAddress("not ip"), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, VALIDATE_NUMBER_TEST) {
    ASSERT_EQ(server::ValidationUtil::ValidateStringIsNumber("1234"), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateStringIsNumber("not number"), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, VALIDATE_BOOL_TEST) {
    std::string str = "true";
    ASSERT_EQ(server::ValidationUtil::ValidateStringIsBool(str), SERVER_SUCCESS);
    str = "not bool";
    ASSERT_NE(server::ValidationUtil::ValidateStringIsBool(str), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, VALIDATE_DOUBLE_TEST) {
    double ret = 0.0;
    ASSERT_EQ(server::ValidationUtil::ValidateStringIsDouble("2.5", ret), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateStringIsDouble("not double", ret), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, VALIDATE_DBURI_TEST) {
    ASSERT_EQ(server::ValidationUtil::ValidateDbURI("sqlite://:@:/"), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateDbURI("xxx://:@:/"), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateDbURI("not uri"), SERVER_SUCCESS);
    ASSERT_EQ(server::ValidationUtil::ValidateDbURI("mysql://root:123456@127.0.0.1:3303/milvus"), SERVER_SUCCESS);
    ASSERT_NE(server::ValidationUtil::ValidateDbURI("mysql://root:123456@127.0.0.1:port/milvus"), SERVER_SUCCESS);
}
