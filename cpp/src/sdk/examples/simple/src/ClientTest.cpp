/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ClientTest.h"
#include "MegaSearch.h"

#include <iostream>

namespace {
    static const std::string TABLE_NAME = "human face";
    static const int64_t TABLE_DIMENSION = 512;

    void PrintTableSchema(const megasearch::TableSchema& tb_schema) {
        std::cout << "===========================================" << std::endl;
        std::cout << "Table name: " << tb_schema.table_name << std::endl;
        std::cout << "Table vectors: " << tb_schema.vector_column_array.size() << std::endl;
        std::cout << "Table attributes: " << tb_schema.attribute_column_array.size() << std::endl;
        std::cout << "Table partitions: " << tb_schema.partition_column_name_array.size() << std::endl;
        std::cout << "===========================================" << std::endl;
    }
}

void
ClientTest::Test(const std::string& address, const std::string& port) {
    std::shared_ptr<megasearch::Connection> conn = megasearch::Connection::Create();
    megasearch::ConnectParam param = { address, port };
    conn->Connect(param);

    {//create table
        megasearch::TableSchema tb_schema;
        megasearch::VectorColumn col1;
        col1.name = "face";
        col1.dimension = TABLE_DIMENSION;
        col1.store_raw_vector = true;
        tb_schema.vector_column_array.emplace_back(col1);

        megasearch::Column col2;
        col2.name = "age";
        tb_schema.attribute_column_array.emplace_back(col2);

        tb_schema.table_name = TABLE_NAME;

        PrintTableSchema(tb_schema);
        megasearch::Status stat = conn->CreateTable(tb_schema);
        std::cout << "Create table result: " << stat.ToString() << std::endl;
    }

    {//describe table
        megasearch::TableSchema tb_schema;
        megasearch::Status stat = conn->DescribeTable(TABLE_NAME, tb_schema);
        std::cout << "Describe table result: " << stat.ToString() << std::endl;
        PrintTableSchema(tb_schema);
    }

    {//add vectors

    }

    {//search vectors

    }

    {//delete table
        megasearch::Status stat = conn->DeleteTable(TABLE_NAME);
        std::cout << "Delete table result: " << stat.ToString() << std::endl;
    }

    megasearch::Connection::Destroy(conn);
}