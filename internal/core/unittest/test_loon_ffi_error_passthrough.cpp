// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_internal/ffi_error_code.h"
#include "storage/loon_ffi/loon_error_code.h"

// milvus_table_c.cpp exports this without a header of its own.
extern "C" LoonFFIResult
loon_milvus_table_append_source_manifests(LoonTransactionHandle transaction,
                                          char** source_manifest_paths,
                                          const int64_t* source_row_counts,
                                          size_t num_source_manifests,
                                          char** target_columns,
                                          size_t num_target_columns,
                                          const char* external_source,
                                          const LoonProperties* properties,
                                          int has_external_primary_key);

// The err_code a loon FFI call reports must survive milvus's OWN C-ABI export
// layer (milvus_table_c.cpp), whose tail returns a LoonFFIResult. That tail
// used to funnel every exception through RETURN_EXCEPTION, which is hardcoded
// to LOON_GOT_EXCEPTION(5): the classification attached deeper in the stack was
// discarded, and since loon_ffi_is_retryable_errcode(5) is false, a transient
// object-storage failure arrived on the Go side as a permanent one.
//
// This exercises the real exported entry point, not the mapper in isolation:
// a source manifest path that does not exist makes the nested loon call fail
// with LOON_FILE_NOT_FOUND(12), and the assertion is that 12 (not 5) is what
// comes back out.
TEST(LoonFFIErrorPassthrough, NestedErrCodeSurvivesTheExportBoundary) {
    std::string base_path = "/tmp/loon_ffi_passthrough_test";
    std::string missing_manifest =
        R"({"base_path":"/tmp/loon_ffi_passthrough_test/does_not_exist","ver":1})";
    std::string column = "pk";

    std::vector<char*> manifests{missing_manifest.data()};
    std::vector<int64_t> row_counts{1};
    std::vector<char*> columns{column.data()};
    LoonTransactionHandle transaction = 0;
    auto begin = loon_transaction_begin(base_path.c_str(),
                                        nullptr,
                                        0,
                                        LOON_TRANSACTION_RESOLVE_OVERWRITE,
                                        10,
                                        &transaction);
    ASSERT_NE(loon_ffi_is_success(&begin), 0);
    loon_ffi_free_result(&begin);

    auto result = loon_milvus_table_append_source_manifests(
        transaction,
        manifests.data(),
        row_counts.data(),
        manifests.size(),
        columns.data(),
        columns.size(),
        /*external_source=*/nullptr,
        /*properties=*/nullptr,
        /*has_external_primary_key=*/0);
    loon_transaction_destroy(transaction);

    ASSERT_EQ(loon_ffi_is_success(&result), 0)
        << "a missing source manifest must fail";
    EXPECT_NE(result.err_code, LOON_GOT_EXCEPTION)
        << "the export boundary collapsed the producer's err_code to "
           "LOON_GOT_EXCEPTION; Go then classifies every failure of this entry "
           "point as permanent";
    // The classification the Go side derives from the code must round-trip
    // through milvus's own mapper as well.
    EXPECT_NE(milvus::storage::LoonErrCodeToErrorCode(result.err_code),
              milvus::ErrorCode::UnexpectedError);
    loon_ffi_free_result(&result);
}
