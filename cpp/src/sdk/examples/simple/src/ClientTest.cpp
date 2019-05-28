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

#define BLOCK_SPLITER std::cout << "===========================================" << std::endl;

    void PrintTableSchema(const megasearch::TableSchema& tb_schema) {
        BLOCK_SPLITER
        std::cout << "Table name: " << tb_schema.table_name << std::endl;
        std::cout << "Table vectors: " << tb_schema.vector_column_array.size() << std::endl;
        std::cout << "Table attributes: " << tb_schema.attribute_column_array.size() << std::endl;
        std::cout << "Table partitions: " << tb_schema.partition_column_name_array.size() << std::endl;
        BLOCK_SPLITER
    }

    void PrintRecordIdArray(const std::vector<int64_t>& record_ids) {
        BLOCK_SPLITER
        std::cout << "Returned id array count: " << record_ids.size() << std::endl;
#if 0
        for(auto id : record_ids) {
            std::cout << std::to_string(id) << std::endl;
        }
#endif
        BLOCK_SPLITER
    }

    void PrintSearchResult(const std::vector<TopKQueryResult>& topk_query_result_array) {
        BLOCK_SPLITER
        std::cout << "Returned result count: " << topk_query_result_array.size() << std::endl;

        int32_t index = 1;
        for(auto& result : topk_query_result_array) {
            std::cout << "No. " << std::to_string(index) << " vector top k search result:" << std::endl;
            for(auto& item : result.query_result_arrays) {
                std::cout << "\t" << std::to_string(item.id) << "\tscore:" << std::to_string(item.score);
                for(auto& attri : item.column_map) {
                    std::cout << "\t" << attri.first << ":" << attri.second;
                }
                std::cout << std::endl;
            }
        }

        BLOCK_SPLITER
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
    static const std::string AGE_COLUMN_NAME = "age";
    static const std::string CITY_COLUMN_NAME = "city";
    static const int64_t TABLE_DIMENSION = 512;

    TableSchema BuildTableSchema() {
        TableSchema tb_schema;
        VectorColumn col1;
        col1.name = VECTOR_COLUMN_NAME;
        col1.dimension = TABLE_DIMENSION;
        col1.store_raw_vector = true;
        tb_schema.vector_column_array.emplace_back(col1);

        Column col2 = {ColumnType::int8, AGE_COLUMN_NAME};
        tb_schema.attribute_column_array.emplace_back(col2);

        Column col3 = {ColumnType::int16, CITY_COLUMN_NAME};
        tb_schema.attribute_column_array.emplace_back(col3);

        tb_schema.table_name = TABLE_NAME;

        return tb_schema;
    }

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

        static const std::map<int64_t , std::string> CITY_MAP = {
                {0, "Beijing"},
                {1, "Shanhai"},
                {2, "Hangzhou"},
                {3, "Guangzhou"},
                {4, "Shenzheng"},
                {5, "Wuhan"},
                {6, "Chengdu"},
                {7, "Chongqin"},
                {8, "Tianjing"},
                {9, "Hongkong"},
        };

        for (int64_t k = from; k < to; k++) {

            std::vector<float> f_p;
            f_p.resize(TABLE_DIMENSION);
            for(int64_t i = 0; i < TABLE_DIMENSION; i++) {
                f_p[i] = (float)(i + k);
            }

            if(vector_record_array) {
                RowRecord record;
                record.vector_map.insert(std::make_pair(VECTOR_COLUMN_NAME, f_p));
                record.attribute_map[AGE_COLUMN_NAME] = std::to_string(k%100);
                record.attribute_map[CITY_COLUMN_NAME] = CITY_MAP.at(k%CITY_MAP.size());
                vector_record_array->emplace_back(record);
            }

            if(query_record_array) {
                QueryRecord record;
                record.vector_map.insert(std::make_pair(VECTOR_COLUMN_NAME, f_p));
                record.selected_column_array.push_back(AGE_COLUMN_NAME);
                record.selected_column_array.push_back(CITY_COLUMN_NAME);
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

    {
        std::cout << "ShowTables" << std::endl;
        std::vector<std::string> tables;
        Status stat = conn->ShowTables(tables);
        std::cout << "Function call status: " << stat.ToString() << std::endl;
        std::cout << "All tables: " << std::endl;
        for(auto& table : tables) {
            std::cout << "\t" << table << std::endl;
        }
    }

    {//create table
        TableSchema tb_schema = BuildTableSchema();
        PrintTableSchema(tb_schema);
        std::cout << "CreateTable" << std::endl;
        Status stat = conn->CreateTable(tb_schema);
        std::cout << "Function call status: " << stat.ToString() << std::endl;
    }

    {//describe table
        TableSchema tb_schema;
        std::cout << "DescribeTable" << std::endl;
        Status stat = conn->DescribeTable(TABLE_NAME, tb_schema);
        std::cout << "Function call status: " << stat.ToString() << std::endl;
        PrintTableSchema(tb_schema);
    }

    {//add vectors
        std::vector<RowRecord> record_array;
        BuildVectors(0, 10000, &record_array, nullptr);
        std::vector<int64_t> record_ids;
        std::cout << "AddVector" << std::endl;
        Status stat = conn->AddVector(TABLE_NAME, record_array, record_ids);
        std::cout << "Function call status: " << stat.ToString() << std::endl;
        PrintRecordIdArray(record_ids);
    }

    {//search vectors
        sleep(10);
        std::vector<QueryRecord> record_array;
        BuildVectors(500, 510, nullptr, &record_array);

        std::vector<TopKQueryResult> topk_query_result_array;
        std::cout << "SearchVector" << std::endl;
        Status stat = conn->SearchVector(TABLE_NAME, record_array, topk_query_result_array, 10);
        std::cout << "Function call status: " << stat.ToString() << std::endl;
        PrintSearchResult(topk_query_result_array);
    }

//    {//delete table
//        Status stat = conn->DeleteTable(TABLE_NAME);
//        std::cout << "Delete table result: " << stat.ToString() << std::endl;
//    }

    Connection::Destroy(conn);
}