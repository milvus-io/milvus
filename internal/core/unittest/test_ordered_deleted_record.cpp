// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include "segcore/OrderedDeletedRecord.h"
#include "common/Types.h"

using namespace milvus;
using namespace milvus::segcore;

template <typename ConcVec, typename Vec>
bool
equal(const ConcVec& v1, const Vec& v2) {
    auto size = v2.size();
    for (size_t i = 0; i < size; i++) {
        if (v1[i] != v2[i]) {
            return false;
        }
    }
    return true;
}

TEST(OrderedDeletedRecord, PushBeforeLoad) {
    OrderedDeletedRecord record;

    // push newer record.
    {
        std::vector<PkType> pks = {11, 12, 13};
        std::vector<Timestamp> timestamps = {110, 120, 130};
        record.push(pks, timestamps.data());
    }

    // load older record.
    {
        std::vector<PkType> pks = {1, 2, 3};
        std::vector<Timestamp> timestamps = {10, 20, 30};
        record.push(pks, timestamps.data());
    }

    // load duplicate record.
    {
        std::vector<PkType> pks = {1, 2, 3};
        std::vector<Timestamp> timestamps = {10, 20, 30};
        record.push(pks, timestamps.data());
    }

    ASSERT_EQ(record.get_deleted_count(), 6);

    record.commit();

    {
        auto& final_record = record.get_deleted_record();
        auto size = final_record.size();
        auto& timestamps = final_record.timestamps();
        auto& pks = final_record.pks();
        std::vector<Timestamp> ref_timestamps = {10, 20, 30, 110, 120, 130};
        std::vector<PkType> ref_pks = {1, 2, 3, 11, 12, 13};

        ASSERT_EQ(record.get_deleted_count(), 6);
        ASSERT_EQ(size, 6);
        ASSERT_TRUE(equal(timestamps, ref_timestamps));
        ASSERT_TRUE(equal(pks, ref_pks));
    }

    // push newer record.
    {
        std::vector<PkType> pks = {111, 112, 113};
        std::vector<Timestamp> timestamps = {1110, 1120, 1130};
        record.push(pks, timestamps.data());
    }

    {
        auto& final_record = record.get_deleted_record();
        auto size = final_record.size();
        auto& timestamps = final_record.timestamps();
        auto& pks = final_record.pks();
        std::vector<Timestamp> ref_timestamps = {
            10, 20, 30, 110, 120, 130, 1110, 1120, 1130};
        std::vector<PkType> ref_pks = {1, 2, 3, 11, 12, 13, 111, 112, 113};

        ASSERT_EQ(record.get_deleted_count(), 9);
        ASSERT_EQ(size, 9);
        ASSERT_TRUE(equal(timestamps, ref_timestamps));
        ASSERT_TRUE(equal(pks, ref_pks));
    }

    /* gtest can't support this well.
    // load after search.
    {
        std::vector<PkType> pks = {1, 2, 3};
        std::vector<Timestamp> timestamps = {10, 20, 30};
        ASSERT_DEATH({ record.load(pks, timestamps.data()); }, "");
    }

    // push older record.
    {
        std::vector<PkType> pks = {11, 12, 13};
        std::vector<Timestamp> timestamps = {110, 120, 130};
        ASSERT_DEATH({ record.push(pks, timestamps.data()); }, "");
    }
		*/
}
