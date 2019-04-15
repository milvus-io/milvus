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

include "Midlayer.thrift"

enum Drink {
  WATER,
  WHISKEY,
  WINE,
  scotch, // intentionally poorly cased
  LATE_HARVEST_WINE,
  India_Pale_Ale, // intentionally poorly cased
  apple_cider, // intentially poorly cased
  belgian_Ale, // intentionally poorly cased
  Canadian_whisky, // intentionally poorly cased
}

const map<i8, Midlayer.Pie> RankedPies = {
  1: Midlayer.Pie.PUMPKIN,
  2: Midlayer.Pie.STRAWBERRY_RHUBARB,
  3: Midlayer.Pie.apple,
  4: Midlayer.Pie.mississippi_mud,
  5: Midlayer.Pie.coconut_Cream,
  6: Midlayer.Pie.Key_Lime,
}

struct FullMeal {
  1: required Midlayer.Meal meal
  2: required Midlayer.Dessert dessert
}

struct FullMealAndDrinks {
  1: required FullMeal fullMeal
  2: optional Drink drink
}

service FullMealService extends Midlayer.MealService {
  FullMeal fullMeal()
}

service FullMealAndDrinksService extends FullMealService {
  FullMealAndDrinks fullMealAndDrinks()

  Midlayer.Pie bestPie()
}

