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

#pragma once

// pure C don't support that we use schemapb.DataType directly.
// Note: the value of all enumerations must match the corresponding schemapb.DataType.
// TODO: what if there are increments in schemapb.DataType.
enum DataType {
    None = 0,
    Bool = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,

    Float = 10,
    Double = 11,

    String = 20,

    BinaryVector = 100,
    FloatVector = 101,
};

typedef void* CIndex;
typedef void* CIndexQueryResult;
