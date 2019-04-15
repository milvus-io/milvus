-module(test_omit).

-include("gen-erl/thrift_omit_with_types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

omit_struct1_test() ->
  %% In this test, the field that is deleted is a basic type (an i32).
  A = #test1{one = 1, three = 3},
  B = #test1{one = 1, two = 2, three = 3},
  {ok, Transport} = thrift_membuffer_transport:new(),
  {ok, P0} = thrift_binary_protocol:new(Transport),

  {P1, ok} = thrift_protocol:write(P0, {{struct, {thrift_omit_with_types, element(1, A)}}, A}),
  {P2, {ok, O0}} = thrift_protocol:read(P1, {struct, {thrift_omit_without_types, element(1, A)}}),
  ?assertEqual(element(1, A), element(1, O0)),
  ?assertEqual(element(2, A), element(2, O0)),
  ?assertEqual(element(4, A), element(3, O0)),

  {P3, ok} = thrift_protocol:write(P2, {{struct, {thrift_omit_with_types, element(1, B)}}, B}),
  {_P4, {ok, O1}} = thrift_protocol:read(P3, {struct, {thrift_omit_without_types, element(1, A)}}),
  ?assertEqual(element(1, A), element(1, O1)),
  ?assertEqual(element(2, A), element(2, O1)),
  ?assertEqual(element(4, A), element(3, O1)),

  ok.

omit_struct2_test() ->
  %% In this test, the field that is deleted is a struct.
  A = #test2{one = 1, two = #test2{one = 10, three = 30}, three = 3},
  B = #test2{one = 1, two = #test2{one = 10, two = #test2{one = 100}, three = 30}, three = 3},

  {ok, Transport} = thrift_membuffer_transport:new(),
  {ok, P0} = thrift_binary_protocol:new(Transport),

  {P1, ok} = thrift_protocol:write(P0, {{struct, {thrift_omit_with_types, element(1, A)}}, A}),
  {P2, {ok, O0}} = thrift_protocol:read(P1, {struct, {thrift_omit_without_types, element(1, A)}}),
  ?assertEqual(element(1, A), element(1, O0)),
  ?assertEqual(element(2, A), element(2, O0)),
  ?assertEqual(element(4, A), element(3, O0)),

  {P3, ok} = thrift_protocol:write(P2, {{struct, {thrift_omit_with_types, element(1, B)}}, B}),
  {_P4, {ok, O1}} = thrift_protocol:read(P3, {struct, {thrift_omit_without_types, element(1, A)}}),
  ?assertEqual(element(1, A), element(1, O1)),
  ?assertEqual(element(2, A), element(2, O1)),
  ?assertEqual(element(4, A), element(3, O1)),

  ok.

omit_list_test() ->
  %% In this test, the field that is deleted is a list.
  A = #test1{one = 1, two = 2, three = 3},
  B = #test3{one = 1, two = [ A ]},

  {ok, Transport} = thrift_membuffer_transport:new(),
  {ok, P0} = thrift_binary_protocol:new(Transport),

  {P1, ok} = thrift_protocol:write(P0, {{struct, {thrift_omit_with_types, element(1, B)}}, B}),
  {_P2, {ok, O0}} = thrift_protocol:read(P1, {struct, {thrift_omit_without_types, element(1, B)}}),
  ?assertEqual(element(2, B), element(2, O0)),

  ok.

omit_map_test() ->
  %% In this test, the field that is deleted is a map.
  A = #test1{one = 1, two = 2, three = 3},
  B = #test4{one = 1, two = dict:from_list([ {2, A} ])},

  {ok, Transport} = thrift_membuffer_transport:new(),
  {ok, P0} = thrift_binary_protocol:new(Transport),

  {P1, ok} = thrift_protocol:write(P0, {{struct, {thrift_omit_with_types, element(1, B)}}, B}),
  {_P2, {ok, O0}} = thrift_protocol:read(P1, {struct, {thrift_omit_without_types, element(1, B)}}),
  ?assertEqual(element(2, B), element(2, O0)),

  ok.

-endif. %% TEST
