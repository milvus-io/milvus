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

typedef i64 Temperature

typedef i8 Size

typedef string Location

const i32 BoilingPoint = 100

const list<Temperature> Temperatures = [10, 11, 22, 33]

// IMPORTANT: temps should end with ".0" because this tests
// that we don't have a problem with const float list generation
const list<double> CommonTemperatures = [300.0, 450.0]

const double MealsPerDay = 2.5;

const string DefaultRecipeName = "Soup-rise of the Day"
const binary DefaultRecipeBinary = "Soup-rise of the 01010101"

struct Noodle {
  1: string flourType
  2: Temperature cookTemp
}

struct Spaghetti {
  1: optional list<Noodle> noodles
}

const Noodle SpeltNoodle = { "flourType": "spelt", "cookTemp": 110 }

struct MeasuringSpoon {
  1: Size size
}

struct MeasuringCup {
  1: double millis
}

union MeasuringAids {
  1: MeasuringSpoon spoon
  2: MeasuringCup cup
}

struct CookingTemperatures {
  1: set<double> commonTemperatures
  2: list<double> usedTemperatures
  3: map<double, double> fahrenheitToCentigradeConversions
}

struct Recipe {
  1: string recipeName
  2: string cuisine
  3: i8 page
}

union CookingTools {
  1: set<MeasuringSpoon> measuringSpoons
  2: map<Size, Location> measuringCups,
  3: list<Recipe> recipes
}

