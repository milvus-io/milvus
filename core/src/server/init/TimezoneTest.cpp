// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>

#include "Timezone.h"

namespace milvus::server {

class ValidTimezoneFixture : public ::testing::TestWithParam<std::string> {};

TEST_P(ValidTimezoneFixture, test_SetTimezone_returnvalue) {
    ASSERT_FALSE(Timezone::SetTimezone(GetParam()).ok());
}
INSTANTIATE_TEST_CASE_P(TimezoneInvalidTest, ValidTimezoneFixture, ::testing::Values("utc++8", "UTC++8", "utc+a", ""));

class InvalidTimezoneFixture : public ::testing::TestWithParam<std::string> {};
TEST_P(InvalidTimezoneFixture, test_SetTimezone_returnvalue) {
    ASSERT_TRUE(Timezone::SetTimezone(GetParam()).ok());
}
INSTANTIATE_TEST_CASE_P(TimezoneValidTest, InvalidTimezoneFixture, ::testing::Values("utc", "UTC+8"));

}  // namespace milvus::server
