/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ClientTest.h"
#include "MegaSearch.h"

#include <iostream>
#include <time.h>
#include <unistd.h>

using namespace megasearch;

namespace {
    void PrintTableSchema(const megasearch::TableSchema& tb_schema) {
        std::cout << "===========================================" << std::endl;
        std::cout << "Table name: " << tb_schema.table_name << std::endl;
        std::cout << "Table vectors: " << tb_schema.vector_column_array.size() << std::endl;
        std::cout << "Table attributes: " << tb_schema.attribute_column_array.size() << std::endl;
        std::cout << "Table partitions: " << tb_schema.partition_column_name_array.size() << std::endl;
        std::cout << "===========================================" << std::endl;
    }

    std::string CurrentTime() {
        time_t tt;
        time( &tt );
        tt = tt + 8*3600;
        tm* t= gmtime( &tt );

        std::string str = std::to_string(t->tm_year + 1900) + "_" + std::to_string(t->tm_mon + 1)
                          + "_" + std::to_string(t->tm_mday) + "_" + std::to_string(t->tm_hour)
                          + "_" + std::to_string(t->tm_min) + "_" + std::to_string(t->tm_sec);

        return str;
    }

    std::string GetTableName() {
        static std::string s_id(CurrentTime());
        return s_id;
    }

    static const std::string TABLE_NAME = GetTableName();
    static const std::string VECTOR_COLUMN_NAME = "face_vector";
    static const int64_t TABLE_DIMENSION = 512;

    void BuildVectors(int64_t from, int64_t to,
                      std::vector<RowRecord>* vector_record_array,
                      std::vector<QueryRecord>* query_record_array) {
        if(to <= from){
            return;
        }

        if(vector_record_array) {
            vector_record_array->clear();
        }
        if(query_record_array) {
            query_record_array->clear();
        }

        for (int64_t k = from; k < to; k++) {

            std::vector<float> f_p;
            f_p.resize(TABLE_DIMENSION);
            for(int64_t i = 0; i < TABLE_DIMENSION; i++) {
                f_p[i] = (float)(i + k);
            }

            if(vector_record_array) {
                RowRecord record;
                record.vector_map.insert(std::make_pair(VECTOR_COLUMN_NAME, f_p));
                vector_record_array->emplace_back(record);
            }

            if(query_record_array) {
                QueryRecord record;
                record.vector_map.insert(std::make_pair(VECTOR_COLUMN_NAME, f_p));
                query_record_array->emplace_back(record);
            }
        }
    }
}

void
ClientTest::Test(const std::string& address, const std::string& port) {
    std::shared_ptr<Connection> conn = Connection::Create();
    ConnectParam param = { address, port };
    conn->Connect(param);

    {//create table
        TableSchema tb_schema;
        VectorColumn col1;
        col1.name = VECTOR_COLUMN_NAME;
        col1.dimension = TABLE_DIMENSION;
        col1.store_raw_vector = true;
        tb_schema.vector_column_array.emplace_back(col1);

        Column col2;
        col2.name = "age";
        tb_schema.attribute_column_array.emplace_back(col2);

        tb_schema.table_name = TABLE_NAME;

        PrintTableSchema(tb_schema);
        Status stat = conn->CreateTable(tb_schema);
        std::cout << "Create table result: " << stat.ToString() << std::endl;
    }

    {//describe table
        TableSchema tb_schema;
        Status stat = conn->DescribeTable(TABLE_NAME, tb_schema);
        std::cout << "Describe table result: " << stat.ToString() << std::endl;
        PrintTableSchema(tb_schema);
    }

    {//add vectors
        std::vector<RowRecord> record_array;
        BuildVectors(0, 10000, &record_array, nullptr);
        std::vector<int64_t> record_ids;
        std::cout << "Begin add vectors" << std::endl;
        Status stat = conn->AddVector(TABLE_NAME, record_array, record_ids);
        std::cout << "Add vector result: " << stat.ToString() << std::endl;
        std::cout << "Returned vector ids: " << record_ids.size() << std::endl;
    }

    {//search vectors
        sleep(10);
        std::vector<QueryRecord> record_array;
        BuildVectors(500, 510, nullptr, &record_array);

        std::vector<TopKQueryResult> topk_query_result_array;
        std::cout << "Begin search vectors" << std::endl;
        Status stat = conn->SearchVector(TABLE_NAME, record_array, topk_query_result_array, 10);
        std::cout << "Search vector result: " << stat.ToString() << std::endl;
        std::cout << "Returned result count: " << topk_query_result_array.size() << std::endl;
    }

//    {//delete table
//        Status stat = conn->DeleteTable(TABLE_NAME);
//        std::cout << "Delete table result: " << stat.ToString() << std::endl;
//    }

    Connection::Destroy(conn);
}