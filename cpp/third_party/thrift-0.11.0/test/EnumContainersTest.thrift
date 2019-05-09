/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

namespace java thrift.test.enumcontainers

enum GreekGodGoddess {
    ARES,
    APHRODITE,
    ZEUS,
    POSEIDON,
    HERA,
}

typedef GreekGodGoddess GreekGodGoddessType
typedef i32 Power

struct GodBean {
    1: optional map<GreekGodGoddessType, Power> power,
    2: optional set<GreekGodGoddessType> goddess,
    3: optional map<string, GreekGodGoddess> byAlias,
    4: optional set<string> images,
}

const map<GreekGodGoddessType, string> ATTRIBUTES =
{
    GreekGodGoddess.ZEUS: "lightning bolt",
    GreekGodGoddess.POSEIDON: "trident",
}

const set<GreekGodGoddessType> BEAUTY = [ GreekGodGoddess.APHRODITE, GreekGodGoddess.HERA ]
