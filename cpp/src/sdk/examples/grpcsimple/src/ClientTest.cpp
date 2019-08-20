/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ClientTest.h"
#include "MilvusApi.h"
#include "cache/CpuCacheMgr.h"

#include <iostream>
#include <time.h>
#include <chrono>
#include <thread>
#include <unistd.h>

using namespace milvus;

//#define SET_VECTOR_IDS;

namespace {
    std::string GetTableName();

    const std::string TABLE_NAME = GetTableName();
    constexpr int64_t TABLE_DIMENSION = 512;
    constexpr int64_t BATCH_ROW_COUNT = 100000;
    constexpr int64_t NQ = 10;
    constexpr int64_t TOP_K = 10;
    constexpr int64_t SEARCH_TARGET = 5000; //change this value, result is different
    constexpr int64_t ADD_VECTOR_LOOP = 1;
    constexpr int64_t SECONDS_EACH_HOUR = 3600;

#define BLOCK_SPLITER std::cout << "===========================================" << std::endl;

    void PrintTableSchema(const TableSchema& tb_schema) {
        BLOCK_SPLITER
        std::cout << "Table name: " << tb_schema.table_name << std::endl;
        std::cout << "Table index type: " << (int)tb_schema.index_type << std::endl;
        std::cout << "Table dimension: " << tb_schema.dimension << std::endl;
        std::cout << "Table store raw data: " << (tb_schema.store_raw_vector ? "true" : "false") << std::endl;
        BLOCK_SPLITER
    }

    void PrintSearchResult(const std::vector<std::pair<int64_t, RowRecord>>& search_record_array,
                           const std::vector<TopKQueryResult>& topk_query_result_array) {
        BLOCK_SPLITER
        std::cout << "Returned result count: " << topk_query_result_array.size() << std::endl;

        int32_t index = 0;
        for(auto& result : topk_query_result_array) {
            auto search_id = search_record_array[index].first;
            index++;
            std::cout << "No." << std::to_string(index) << " vector " << std::to_string(search_id)
                      << " top " << std::to_string(result.query_result_arrays.size())
                      << " search result:" << std::endl;
            for(auto& item : result.query_result_arrays) {
                std::cout << "\t" << std::to_string(item.id) << "\tdistance:" << std::to_string(item.distance);
                std::cout << std::endl;
            }
        }

        BLOCK_SPLITER
    }

    std::string CurrentTime() {
        time_t tt;
        time( &tt );
        tt = tt + 8*SECONDS_EACH_HOUR;
        tm* t= gmtime( &tt );

        std::string str = std::to_string(t->tm_year + 1900) + "_" + std::to_string(t->tm_mon + 1)
                          + "_" + std::to_string(t->tm_mday) + "_" + std::to_string(t->tm_hour)
                          + "_" + std::to_string(t->tm_min) + "_" + std::to_string(t->tm_sec);

        return str;
    }

    std::string CurrentTmDate(int64_t offset_day = 0) {
        time_t tt;
        time( &tt );
        tt = tt + 8*SECONDS_EACH_HOUR;
        tt = tt + 24*SECONDS_EACH_HOUR*offset_day;
        tm* t= gmtime( &tt );

        std::string str = std::to_string(t->tm_year + 1900) + "-" + std::to_string(t->tm_mon + 1)
                          + "-" + std::to_string(t->tm_mday);

        return str;
    }

    std::string GetTableName() {
        static std::string s_id(CurrentTime());
        return "tbl_" + s_id;
    }

    TableSchema BuildTableSchema() {
        TableSchema tb_schema;
        tb_schema.table_name = TABLE_NAME;
        tb_schema.index_type = IndexType::gpu_ivfflat;
        tb_schema.dimension = TABLE_DIMENSION;
        tb_schema.store_raw_vector = true;

        return tb_schema;
    }

    void BuildVectors(int64_t from, int64_t to,
                      std::vector<RowRecord>& vector_record_array) {
        if(to <= from){
            return;
        }

        vector_record_array.clear();
        for (int64_t k = from; k < to; k++) {
            RowRecord record;
            record.data.resize(TABLE_DIMENSION);
            for(int64_t i = 0; i < TABLE_DIMENSION; i++) {
                record.data[i] = (float)(k%(i+1));
            }

            vector_record_array.emplace_back(record);
        }
    }

    void Sleep(int seconds) {
        std::cout << "Waiting " << seconds << " seconds ..." << std::endl;
        sleep(seconds);
    }

    class TimeRecorder {
    public:
        explicit TimeRecorder(const std::string& title)
                : title_(title) {
            start_ = std::chrono::system_clock::now();
        }

        ~TimeRecorder() {
            std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
            long span = (std::chrono::duration_cast<std::chrono::milliseconds> (end - start_)).count();
            std::cout << title_ << " totally cost: " << span << " ms" << std::endl;
        }

    private:
        std::string title_;
        std::chrono::system_clock::time_point start_;
    };

    void CheckResult(const std::vector<std::pair<int64_t, RowRecord>>& search_record_array,
                     const std::vector<TopKQueryResult>& topk_query_result_array) {
        BLOCK_SPLITER
        int64_t index = 0;
        for(auto& result : topk_query_result_array) {
            auto result_id = result.query_result_arrays[0].id;
            auto search_id = search_record_array[index++].first;
            if(result_id != search_id) {
                std::cout << "The top 1 result is wrong: " << result_id
                          << " vs. " << search_id << std::endl;
            } else {
                std::cout << "Check result sucessfully" << std::endl;
            }
        }
        BLOCK_SPLITER
    }

    void DoSearch(std::shared_ptr<Connection> conn,
                  const std::vector<std::pair<int64_t, RowRecord>>& search_record_array,
                  const std::string& phase_name) {
        std::vector<Range> query_range_array;
        Range rg;
        rg.start_value = CurrentTmDate();
        rg.end_value = CurrentTmDate(1);
        query_range_array.emplace_back(rg);

        std::vector<RowRecord> record_array;
        for(auto& pair : search_record_array) {
            record_array.push_back(pair.second);
        }

        std::vector<TopKQueryResult> topk_query_result_array;
        {
            TimeRecorder rc(phase_name);
            Status stat = conn->Search(TABLE_NAME, record_array, query_range_array, TOP_K, 10, topk_query_result_array);
            std::cout << "SearchVector function call status: " << stat.ToString() << std::endl;
        }

        PrintSearchResult(search_record_array, topk_query_result_array);
        CheckResult(search_record_array, topk_query_result_array);
    }
}

void
ClientTest::Test(const std::string& address, const std::string& port) {
    std::shared_ptr<Connection> conn = Connection::Create();

    {//connect server
        ConnectParam param = {address, port};
        Status stat = conn->Connect(param);
        std::cout << "Connect function call status: " << stat.ToString() << std::endl;
    }

    {//server version
        std::string version = conn->ServerVersion();
        std::cout << "Server version: " << version << std::endl;
    }

    {//sdk version
        std::string version = conn->ClientVersion();
        std::cout << "SDK version: " << version << std::endl;
    }

    {
        std::vector<std::string> tables;
        Status stat = conn->ShowTables(tables);
        std::cout << "ShowTables function call status: " << stat.ToString() << std::endl;
        std::cout << "All tables: " << std::endl;
        for(auto& table : tables) {
            int64_t row_count = 0;
            conn->DropTable(table);
//            stat = conn->CountTable(table, row_count);
//            std::cout << "\t" << table << "(" << row_count << " rows)" << std::endl;
        }
    }

    {//create table
        TableSchema tb_schema = BuildTableSchema();
        Status stat = conn->CreateTable(tb_schema);
        std::cout << "CreateTable function call status: " << stat.ToString() << std::endl;
        PrintTableSchema(tb_schema);

        bool has_table = conn->HasTable(tb_schema.table_name);
        if(has_table) {
            std::cout << "Table is created" << std::endl;
        }
    }

    {//describe table
        TableSchema tb_schema;
        Status stat = conn->DescribeTable(TABLE_NAME, tb_schema);
        std::cout << "DescribeTable function call status: " << stat.ToString() << std::endl;
        PrintTableSchema(tb_schema);
    }

    std::vector<std::pair<int64_t, RowRecord>> search_record_array;
    {//insert vectors
        std::vector<int64_t> record_ids;
        for (int i = 0; i < ADD_VECTOR_LOOP; i++) {//add vectors
            std::vector<RowRecord> record_array;
            int64_t begin_index = i * BATCH_ROW_COUNT;
            BuildVectors(begin_index, begin_index + BATCH_ROW_COUNT, record_array);

#ifdef SET_VECTOR_IDS
            record_ids.resize(ADD_VECTOR_LOOP * BATCH_ROW_COUNT);
            for (auto j = begin_index; j <begin_index + BATCH_ROW_COUNT; j++) {
                record_ids[i * BATCH_ROW_COUNT + j] = i * BATCH_ROW_COUNT + j;
            }
#endif

            auto start = std::chrono::high_resolution_clock::now();

            Status stat = conn->Insert(TABLE_NAME, record_array, record_ids);
            auto finish = std::chrono::high_resolution_clock::now();
            std::cout << "InsertVector cost: " << std::chrono::duration_cast<std::chrono::duration<double>>(finish - start).count() << "s\n";


            std::cout << "InsertVector function call status: " << stat.ToString() << std::endl;
            std::cout << "Returned id array count: " << record_ids.size() << std::endl;

            if(search_record_array.size() < NQ) {
                search_record_array.push_back(
                        std::make_pair(record_ids[SEARCH_TARGET], record_array[SEARCH_TARGET]));
            }
        }
    }

    {//search vectors without index
        Sleep(2);
        DoSearch(conn, search_record_array, "Search without index");
    }

    {//wait unit build index finish
//        std::cout << "Wait until build all index done" << std::endl;
//        Status stat = conn->CreateIndex();
//        std::cout << "BuildIndex function call status: " << stat.ToString() << std::endl;
    }

    {//preload table
        Status stat = conn->PreloadTable(TABLE_NAME);
        std::cout << "PreloadTable function call status: " << stat.ToString() << std::endl;
    }

    {//search vectors after build index finish
        DoSearch(conn, search_record_array, "Search after build index finish");
    }

    {//delete table
        Status stat = conn->DropTable(TABLE_NAME);
        std::cout << "DeleteTable function call status: " << stat.ToString() << std::endl;
    }

    {//server status
        std::string status = conn->ServerStatus();
        std::cout << "Server status before disconnect: " << status << std::endl;
    }
    Connection::Destroy(conn);
//    conn->Disconnect();
    {//server status
        std::string status = conn->ServerStatus();
        std::cout << "Server status after disconnect: " << status << std::endl;
    }
}