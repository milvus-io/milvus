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
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include "exceptions/EasyAssert.h"

namespace cp = ::arrow::compute;

TEST(Arrow, Compute) {
    arrow::Int64Builder builder1;
    builder1.AppendValues({1, 2, 3, 4, 5});
    auto array1 = builder1.Finish().ValueOrDie();

    arrow::FloatBuilder builder2;
    builder2.AppendValues({1.1, 2.0, 3.3, 4.0, 5.5});
    auto array2 = builder2.Finish().ValueOrDie();

    auto array = cp::CallFunction("less_equal", {array1, array2}).ValueOrDie();
    array = cp::Or(arrow::Datum(false), array).ValueOrDie();

    Assert(array.type()->Equals(arrow::BooleanType()));
    Assert(array.is_array());
    auto bool_array = array.array_as<arrow::BooleanArray>();
    for (int i = 0; i < array.length(); i++) {
        Assert(bool_array->Value(i));
    }
}