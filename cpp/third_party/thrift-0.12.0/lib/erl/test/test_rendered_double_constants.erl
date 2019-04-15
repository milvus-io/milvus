%%
%% Licensed to the Apache Software Foundation (ASF) under one
%% or more contributor license agreements. See the NOTICE file
%% distributed with this work for additional information
%% regarding copyright ownership. The ASF licenses this file
%% to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance
%% with the License. You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%

-module(test_rendered_double_constants).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-include("gen-erl/double_constants_test_constants.hrl").

-define(EPSILON, 0.0000001).

rendered_double_constants_test() ->
  ?assert(abs(1.0 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_INT_CONSTANT_TEST) =< ?EPSILON),
  ?assert(abs(-100.0 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT_TEST) =< ?EPSILON),
  ?assert(abs(9223372036854775807.0 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT_TEST) =< ?EPSILON),
  ?assert(abs(-9223372036854775807.0 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT_TEST) =< ?EPSILON),
  ?assert(abs(3.14159265359 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS_TEST) =< ?EPSILON),
  ?assert(abs(1000000.1 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE_TEST) =< ?EPSILON),
  ?assert(abs(-1000000.1 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE_TEST) =< ?EPSILON),
  ?assert(abs(1.7e+308 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_LARGE_DOUBLE_TEST) =< ?EPSILON),
  ?assert(abs(9223372036854775816.43 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE_TEST) =< ?EPSILON),
  ?assert(abs(-1.7e+308 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_SMALL_DOUBLE_TEST) =< ?EPSILON),
  ?assert(abs(-9223372036854775816.43 - ?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE_TEST) =< ?EPSILON),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_INT_CONSTANT_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_LARGE_DOUBLE_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_SMALL_DOUBLE_TEST)),
  ?assert(is_float(?DOUBLE_CONSTANTS_TEST_DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE_TEST)).

rendered_double_list_test() ->
  ?assertEqual(12, length(?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)),
  ?assert(abs(1.0 - lists:nth(1, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(-100.0 - lists:nth(2, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(100.0 - lists:nth(3, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(9223372036854775807.0 - lists:nth(4, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(-9223372036854775807.0 - lists:nth(5, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(3.14159265359 - lists:nth(6, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(1000000.1 - lists:nth(7, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(-1000000.1 - lists:nth(8, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(1.7e+308 - lists:nth(9, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(-1.7e+308 - lists:nth(10, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(9223372036854775816.43 - lists:nth(11, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON),
  ?assert(abs(-9223372036854775816.43 - lists:nth(12, ?DOUBLE_CONSTANTS_TEST_DOUBLE_LIST_TEST)) =< ?EPSILON).

-endif. %% TEST