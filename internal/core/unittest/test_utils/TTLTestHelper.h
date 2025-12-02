#pragma once

#include <gtest/gtest.h>
#include <vector>
#include <memory>
#include <numeric>
#include "common/Schema.h"
#include "common/Types.h"
#include "segcore/SegmentInterface.h"
#include "pb/segcore.pb.h"

namespace milvus::segcore {

class TTLTestHelper {
 public:
    static std::shared_ptr<Schema> 
    CreateTTLSchema(bool is_nullable) {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64, false);
        auto ttl_fid = schema->AddDebugField("ttl_field", DataType::INT64, is_nullable);
        schema->set_primary_field_id(pk_fid);
        schema->set_ttl_field_id(ttl_fid);
        return schema;
    }

    static void 
    PrepareTTLData(int64_t count, 
                   uint64_t base_ts, 
                   bool is_nullable,
                   std::vector<int64_t>& pks,
                   std::vector<Timestamp>& tss,
                   std::vector<int64_t>& ttls,
                   std::vector<bool>& valid_data) {
        pks.resize(count);
        tss.resize(count);
        ttls.resize(count);
        valid_data.assign(count, true);
        
        std::iota(pks.begin(), pks.end(), 0);
        uint64_t base_physical_us = (base_ts >> LOGICAL_BITS) * 1000;

        for (int i = 0; i < count; i++) {
            tss[i] = base_ts + i;
            
            // Logic for Null values (Only if is_nullable is true)
            if (is_nullable && (i % 3 == 0)) {
                ttls[i] = 0; 
                valid_data[i] = false;
            } 
            // Logic for Expired values (Even indices)
            else if (i % 2 == 0) {
                ttls[i] = static_cast<int64_t>(base_physical_us - 1000);
                valid_data[i] = true;
            } 
            // Logic for Normal values (Odd indices)
            else {
                ttls[i] = static_cast<int64_t>(base_physical_us + 1000);
                valid_data[i] = true;
            }
        }
    }

    static void 
    VerifyTTLMask(const BitsetView& bitset, int64_t count, bool is_nullable) {
        for (int i = 0; i < count; i++) {
            bool should_be_expired = false;
            
            if (is_nullable && (i % 3 == 0)) {
                // Null values never expire
                should_be_expired = false;
            } else if (i % 2 == 0) {
                // Expired if TTL is in the past
                should_be_expired = true;
            } else {
                // Normal data
                should_be_expired = false;
            }

            if (should_be_expired) {
                EXPECT_TRUE(bitset[i]) << "Row " << i << " (is_nullable=" << is_nullable << ") should be expired";
            } else {
                EXPECT_FALSE(bitset[i]) << "Row " << i << " (is_nullable=" << is_nullable << ") should NOT be expired";
            }
        }
    }
};

} // namespace milvus::segcore