////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>

#include "utils/ValidationUtil.h"
#include "utils/Error.h"
#include "db/ExecutionEngine.h"

#include <string>

using namespace zilliz::milvus;
using namespace zilliz::milvus::server;

TEST(ValidationUtilTest, TableNameTest) {
    std::string table_name = "Normal123_";
    ServerError res = ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_SUCCESS);

    table_name = "12sds";
    res = ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "";
    res = ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "_asdasd";
    res = ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_SUCCESS);

    table_name = "!@#!@";
    res = ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "中文";
    res = ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);


    table_name = std::string('a', 32768);
    res = ValidationUtil::ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);
}


TEST(ValidationUtilTest, TableDimensionTest) {
    ASSERT_EQ(ValidationUtil::ValidateTableDimension(-1), SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(ValidationUtil::ValidateTableDimension(0), SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(ValidationUtil::ValidateTableDimension(16385), SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(ValidationUtil::ValidateTableDimension(16384), SERVER_SUCCESS);
    ASSERT_EQ(ValidationUtil::ValidateTableDimension(1), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, TableIndexTypeTest) {
    ASSERT_EQ(ValidationUtil::ValidateTableIndexType((int)engine::EngineType::INVALID), SERVER_INVALID_INDEX_TYPE);
    for(int i = 1; i <= (int)engine::EngineType::MAX_VALUE; i++) {
        ASSERT_EQ(ValidationUtil::ValidateTableIndexType(i), SERVER_SUCCESS);
    }
    ASSERT_EQ(ValidationUtil::ValidateTableIndexType((int)engine::EngineType::MAX_VALUE + 1), SERVER_INVALID_INDEX_TYPE);
}
