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
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */

include "Base_One.thrift"
include "Base_Two.thrift"

const i32 WaterBoilingPoint = Base_One.BoilingPoint

const map<string, Base_One.Temperature> TemperatureNames = { "freezing": 0, "boiling": 100 }

const map<set<i32>, map<list<string>, string>> MyConstNestedMap = {
  [0, 1, 2, 3]: { ["foo"]: "bar" },
  [20]: { ["nut", "ton"] : "bar" },
  [30, 40]: { ["bouncy", "tinkly"]: "castle" }
}

const list<list<i32>> MyConstNestedList = [
  [0, 1, 2],
  [3, 4, 5],
  [6, 7, 8]
]

const set<set<i32>> MyConstNestedSet = [
  [0, 1, 2],
  [3, 4, 5],
  [6, 7, 8]
]

enum Pie {
  PUMPKIN,
  apple, // intentionally poorly cased
  STRAWBERRY_RHUBARB,
  Key_Lime, // intentionally poorly cased
  coconut_Cream, // intentionally poorly cased
  mississippi_mud, // intentionally poorly cased
}

struct Meal {
  1: Base_One.Noodle noodle
  2: Base_Two.Ramen ramen
}

union Dessert  {
  1: string port
  2: string iceWine
}

service MealService extends Base_Two.RamenService {
  Meal meal()
}

