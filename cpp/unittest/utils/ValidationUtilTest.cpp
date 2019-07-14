////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>

#include "utils/ValidationUtil.h"
#include "utils/Error.h"

#include <string>

using namespace zilliz::milvus::server;

TEST(ValidationUtilTest, TableNameTest) {
    std::string table_name = "Normal123_";
    ServerError res = ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_SUCCESS);

    table_name = "12sds";
    res = ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "";
    res = ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "_asdasd";
    res = ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_SUCCESS);

    table_name = "!@#!@";
    res = ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);

    table_name = "中文";
    res = ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);


    table_name = std::string('a', 32768);
    res = ValidateTableName(table_name);
    ASSERT_EQ(res, SERVER_INVALID_TABLE_NAME);
}


TEST(ValidationUtilTest, TableDimensionTest) {
    ASSERT_EQ(ValidateTableDimension(-1), SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(ValidateTableDimension(0), SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(ValidateTableDimension(16385), SERVER_INVALID_VECTOR_DIMENSION);
    ASSERT_EQ(ValidateTableDimension(16384), SERVER_SUCCESS);
    ASSERT_EQ(ValidateTableDimension(1), SERVER_SUCCESS);
}

TEST(ValidationUtilTest, TableIndexTypeTest) {
    ASSERT_EQ(ValidateTableIndexType(0), SERVER_INVALID_INDEX_TYPE);
    ASSERT_EQ(ValidateTableIndexType(1), SERVER_SUCCESS);
    ASSERT_EQ(ValidateTableIndexType(2), SERVER_SUCCESS);
    ASSERT_EQ(ValidateTableIndexType(3), SERVER_INVALID_INDEX_TYPE);
    ASSERT_EQ(ValidateTableIndexType(4), SERVER_INVALID_INDEX_TYPE);
}
