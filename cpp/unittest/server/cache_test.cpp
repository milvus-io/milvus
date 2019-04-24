////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include "cache/CacheMgr.h"

using namespace zilliz::vecwise;

//namespace {
//cache::DataObjPtr MakeData(int64_t size) {
//    auto data_ptr = std::shared_ptr<char>(new char[size], std::default_delete<char[]>());
//    return std::make_shared<cache::DataObj>(data_ptr, size);
//}
//
//#define MAKE_1GB_DATA MakeData(1*1024*1024*1024)
//#define MAKE_100MB_DATA MakeData(100*1024*1024)
//#define MAKE_1MB_DATA MakeData(1*1024*1024)
//}
//
//TEST(CacheTest, CACHE_TEST) {
//    cache::CacheMgr cache_mgr = cache::CacheMgr::GetInstance();
//
//    for(int i = 0; i < 10; i++) {
//        cache_mgr.InsertItem(std::to_string(i), MAKE_100MB_DATA);
//    }
//
//    ASSERT_EQ(cache_mgr.ItemCount(), 10);
//
//    std::string key = "test_data";
//    cache_mgr.InsertItem(key, MAKE_100MB_DATA);
//    cache::DataObjPtr data = cache_mgr.GetItem(key);
//    ASSERT_TRUE(data != nullptr);
//    ASSERT_TRUE(cache_mgr.ItemExists(key));
//    ASSERT_EQ(data->size(), 100*1024*1024);
//
//    cache_mgr.EraseItem(key);
//    data = cache_mgr.GetItem(key);
//    ASSERT_TRUE(data == nullptr);
//}