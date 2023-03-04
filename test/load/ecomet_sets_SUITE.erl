-module(ecomet_sets_SUITE).

-include("ecomet.hrl").
-include("ecomet_test.hrl").

-define(INTERSECTION,{100000,10}).
-define(SETS,{100,10}).


%% API
-export([
  all/0,
  groups/0,
  init_per_testcase/2,
  end_per_testcase/2,
  init_per_group/2,
  end_per_group/2,
  init_per_suite/1,
  end_per_suite/1
]).

-export([
  gb_sets_bit_and/1, 
  ordsets_bit_and/1,
  bitmap_bit_and/1,
  gb_sets_bit_or/1,
  ordsets_bit_or/1,
  gb_sets_bit_subtract/1,
  ordsets_bit_subtract/1
]).


all()->
  [
    gb_sets_bit_and, 
    ordsets_bit_and,
    % bitmap_bit_and,
    gb_sets_bit_or,
    ordsets_bit_or,
    gb_sets_bit_subtract,
    ordsets_bit_subtract
  ].

groups()->[
].

init_per_suite(Config)->
  {IS1,IS2} = ?INTERSECTION,
  ITags1 = [erlang:phash2(make_ref(),4294836225)|| _<-lists:seq(1,IS1)],
  ITags2 = [erlang:phash2(make_ref(),4294836225)|| _<-lists:seq(1,IS2)],

  {S1,S2} = ?SETS,
  Tags1 = [erlang:phash2(make_ref(),4294836225)|| _<-lists:seq(1,S1)],
  Tags2 = [erlang:phash2(make_ref(),4294836225)|| _<-lists:seq(1,S2)],

  [
    {itags, {ITags1,ITags2}},
    {tags, {Tags1,Tags2}}
    |Config].

end_per_suite(_Config)->
  _Config.


init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

gb_sets_bit_and(Config) ->
    {Tags1,Tags2} = ?GET(itags,Config),
    Set1 = gb_sets:from_list(Tags1),
    Set2 = gb_sets:from_list(Tags2),
    {Time, Result} = timer:tc(gb_sets, intersection, [Set1, Set2]),
    ct:pal("Result: ~p~n", [gb_sets:size(Result)]),
    ct:pal("gb_sets intersection time taken: ~p microseconds~n", [Time]),
    ok.
    

ordsets_bit_and(Config) ->
    {Tags1,Tags2} = ?GET(itags,Config),
    Set1 = ordsets:from_list(Tags1),
    Set2 = ordsets:from_list(Tags2),
    {Time, Result} = timer:tc(ordsets, intersection, [Set1, Set2]),
    ct:pal("Result: ~p~n", [length(Result)]),
    ct:pal("ordsets intersection time taken: ~p microseconds~n", [Time]),
    ok.

bitmap_bit_and(Config) ->
    {Tags1,Tags2} = ?GET(itags,Config),
    Set1 = ecomet_bitmap:set_bit(<<>>,Tags1),
    Set2 = ecomet_bitmap:set_bit(<<>>,Tags2),
    {Time, Result} = timer:tc(ecomet_bitmap, bit_and, [Set1, Set2]),
    ct:pal("Result: ~p~n", [ecomet_bitmap:count(Result)]),
    ct:pal("ecomet_bitmap bit_and time taken: ~p microseconds~n", [Time]),
    ok.


gb_sets_bit_or(Config) ->
    {Tags1,Tags2} = ?GET(tags,Config),
    Set1 = gb_sets:from_list(Tags1),
    Set2 = gb_sets:from_list(Tags2),
    {Time, Result} = timer:tc(gb_sets, union, [Set1, Set2]),
    ct:pal("Result: ~p~n", [gb_sets:size(Result)]),
    ct:pal("gb_sets union time taken ~p microseconds~n", [Time]),
    ok.

ordsets_bit_or(Config) ->
    {Tags1,Tags2} = ?GET(tags,Config),
    Set1 = ordsets:from_list(Tags1),
    Set2 = ordsets:from_list(Tags2),
    {Time, Result} = timer:tc(ordsets, union, [Set1, Set2]),
    ct:pal("Result: ~p~n", [length(Result)]),
    ct:pal("ordsets union time taken: ~p microseconds~n", [Time]),
    ok.


gb_sets_bit_subtract(Config) ->
    {Tags1,Tags2} = ?GET(tags,Config),
    Set1 = gb_sets:from_list(Tags1),
    Set2 = gb_sets:from_list(Tags2),
    {Time, Result} = timer:tc(gb_sets, subtract, [Set1, Set2]),
    ct:pal("Result: ~p~n", [gb_sets:size(Result)]),
    ct:pal("gb_sets subtract time taken: ~p microseconds~n", [Time]),
    ok.

ordsets_bit_subtract(Config) ->
    {Tags1,Tags2} = ?GET(tags,Config),
    Set1 = ordsets:from_list(Tags1),
    Set2 = ordsets:from_list(Tags2),
    {Time, Result} = timer:tc(ordsets, subtract, [Set1, Set2]),
    ct:pal("Result: ~p~n", [length(Result)]),
    ct:pal("ordsets subtract time taken: ~p microseconds~n", [Time]),
    ok.