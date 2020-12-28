%%----------------------------------------------------------------
%% Copyright (c) 2020 Faceplate
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%----------------------------------------------------------------

-module(ecomet_query_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet_test.hrl").

%% API
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

%% Optimized search group
-export([
  get_rs/1,
  get_oid/1,
  get_count/1
]).

%% GET util
-export([
  get_alias/1,
  read_fun/1,
  read_map/1,
  is_aggregate/1,
  read_up/1,
  reorder_fields/1
]).

%% GET util grouping
-export([
  insert_to_group/1,
  sort_groups/1,
  merge_trees/1,
  grouped_resut/1,
  groups_page/1,
  compare_rows/1,
  sort_leafs/1
]).

%% SEARCH
-export([
  simple_no_sort/1,
  grouping_sorting/1,
  grouping_sorting_page/1,
  aggregate/1,
  query_get/1
]).

%% SET
-export([
  set/1
]).

%% parse
-export([
  parse_get/1,
  parse_set/1,
  parse_insert/1,
  parse_delete/1
]).

all()->
  [
    {group,optimized_seacrh},
    {group,get_util},
    {group,get_util_grouping},
    {group,search},
    set,
    {group, parse}
  ].

groups()->
  [
    {optimized_seacrh,
      [sequence],
      [
        get_rs,
        get_oid,
        get_count
      ]
    },
    {get_util,
      [sequence],
      [
        get_alias,
        read_fun,
        read_map,
        is_aggregate,
        read_up,
        reorder_fields
      ]
    },
    {get_util_grouping,
      [sequence],
      [
        insert_to_group,
        sort_groups,
        merge_trees,
        grouped_resut,
        groups_page,
        compare_rows,
        sort_leafs
      ]
    },
    {search,
      [sequence],
      [
        simple_no_sort,
        grouping_sorting,
        grouping_sorting_page,
        aggregate,
        query_get
      ]
    },
    {parse,
      [parallel],
      [
        parse_get,
        parse_set,
        parse_insert,
        parse_delete
      ]
    }
  ].

init_per_suite(Config)->
  ?BACKEND_INIT(),

  % Build patterns
  FolderOID={2,1000},
  PatternID1={3,1001},
  PatternID2={3,1002},
  PatternID3={3,1003},

  %--------Object map-------------------------------------------
  BaseObj = ecomet_pattern:get_fields({?PATTERN_PATTERN,?OBJECT_PATTERN}),
  %--------Pattern1-------------------------------------------
  Pattern1Map = BaseObj#{
    <<"field1">> => ecomet_field:build_description(#{ type=>string,storage=>ram }),
    <<"field2">> => ecomet_field:build_description(#{ type=>string,storage=>ramdisc }),
    <<"field3">> => ecomet_field:build_description(#{ type=>string,storage=>ramdisc }),
    <<"field4">> => ecomet_field:build_description(#{ type=>string,storage=>disc }),
    <<"string1">> => ecomet_field:build_description(#{ type=>string,index=>[simple,'3gram'],storage=>ram }),
    <<"string2">> => ecomet_field:build_description(#{ type=>string,index=>[simple],storage=>ramdisc }),
    <<"string3">> => ecomet_field:build_description(#{ type=>string,index=>[simple],storage=>disc }),
    <<"integer">> => ecomet_field:build_description(#{ type=>integer,index=>[simple],storage=>ram }),
    <<"float">> => ecomet_field:build_description(#{ type=>float,index=>[simple],storage=>ramdisc }),
    <<"atom">> => ecomet_field:build_description(#{ type=>atom,index=>[simple],storage=>disc }),
    <<"bool">> => ecomet_field:build_description(#{ type=>bool,index=>[simple],storage=>ram })
  },
  ecomet_schema:register_type(PatternID1, Pattern1Map),
  %--------Pattern2-------------------------------------------
  Pattern2Map = BaseObj#{
    <<"field1">> => ecomet_field:build_description(#{ type=>string,storage=>ram }),
    <<"field2">> => ecomet_field:build_description(#{ type=>string,storage=>disc }),
    <<"string1">> => ecomet_field:build_description(#{ type=>string,index=>[simple,'3gram'],storage=>ramdisc }),
    <<"string2">> => ecomet_field:build_description(#{ type=>string,index=>[simple],storage=>disc }),
    <<"string3">> => ecomet_field:build_description(#{ type=>string,index=>[simple],storage=>ram }),
    <<"integer">> => ecomet_field:build_description(#{ type=>integer,index=>[simple],storage=>ramdisc }),
    <<"float">> => ecomet_field:build_description(#{ type=>float,index=>[simple],storage=>disc }),
    <<"atom">> => ecomet_field:build_description(#{ type=>atom,index=>[simple],storage=>ram }),
    <<"bool">> => ecomet_field:build_description(#{ type=>bool,index=>[simple],storage=>ramdisc })
  },
  ecomet_schema:register_type(PatternID2, Pattern2Map),
  %--------Pattern3-------------------------------------------
  Pattern3Map = BaseObj#{
    <<"field1">> => ecomet_field:build_description(#{ type=>string,storage=>ram }),
    <<"field3">> => ecomet_field:build_description(#{ type=>string,storage=>disc })
  },
  ecomet_schema:register_type(PatternID3, Pattern3Map),

  %%-----------Build test database-------------------
  FolderOID1={2,1001},
  FolderOID2={2,1002},
  lists:foreach(
    fun(I)->
      if ((I rem 500)==0)->ct:pal("created ~p",[I*2]); true->ok end,
      IB=integer_to_binary(I),
      S1=integer_to_binary(I rem 100),
      String1= <<"value",S1/binary>>,
      S2= integer_to_binary(I div 100),
      String2= <<"value",S2/binary>>,
      S3=integer_to_binary(I div 500),
      String3= <<"value",S3/binary>>,
      Integer= I rem 100,
      Float= I/7,
      Atom=list_to_atom(binary_to_list(String3)),
      Bool=((I rem 2)==1),
      % Pattern1 object
      ecomet:create_object(#{
        <<".name">> => <<"test", IB/binary>>,
        <<".folder">> => FolderOID1,
        <<".pattern">> => PatternID1,
        <<".ts">>=>ecomet_lib:log_ts(),
        <<"string1">> => String1,
        <<"string2">> => String2,
        <<"string3">> => String3,
        <<"integer">> => Integer,
        <<"float">> => Float,
        <<"atom">> => Atom,
        <<"bool">> => Bool
      }),
      ecomet:create_object(#{
        <<".name">> => <<"test", IB/binary>>,
        <<".folder">> => FolderOID2,
        <<".pattern">> => PatternID2,
        <<".ts">>=>ecomet_lib:log_ts(),
        <<"string1">> => String1,
        <<"string2">> => String2,
        <<"string3">> => String3,
        <<"integer">> => Integer,
        <<"float">> => Float,
        <<"atom">> => Atom,
        <<"bool">> => Bool
      })
    end,
    lists:seq(1,1000)
  ),
  [
    {folder_id,FolderOID},
    {pattern_id1,PatternID1},
    {pattern_id2,PatternID2},
    {pattern_id3,PatternID3}
    |Config].

end_per_suite(Config)->
  ecomet_user:on_init_state(),
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),

  ecomet_query:delete([root],{'OR',[
    {<<".pattern">>,'=',PatternID1},
    {<<".pattern">>,'=',PatternID2}
  ]}),

  ?BACKEND_STOP(30000),
  ok.

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

%%================================================================================
%%    PLANNING
%%================================================================================
get_rs(Config)->
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),

  ecomet_user:on_init_state(),

  FunPatName=
    fun(OID,Acc)->
      Object = ecomet_object:construct(OID),
      {ok, Pattern}=ecomet_object:read_field(Object, <<".pattern">>),
      {ok, Name}=ecomet_object:read_field(Object, <<".name">>),
      [{Pattern, Name} | Acc]
    end,

  RS=ecomet_query:get([root],rs,{<<"string1">>,'=',<<"value1">>}),
  Result=ecomet_resultset:foldr(FunPatName, [], RS),
  20=length(Result),
  []=lists:subtract(Result,[
    {PatternID1,<<"test1">>},
    {PatternID1,<<"test101">>},
    {PatternID1,<<"test201">>},
    {PatternID1,<<"test301">>},
    {PatternID1,<<"test401">>},
    {PatternID1,<<"test501">>},
    {PatternID1,<<"test601">>},
    {PatternID1,<<"test701">>},
    {PatternID1,<<"test801">>},
    {PatternID1,<<"test901">>},
    {PatternID2,<<"test1">>},
    {PatternID2,<<"test101">>},
    {PatternID2,<<"test201">>},
    {PatternID2,<<"test301">>},
    {PatternID2,<<"test401">>},
    {PatternID2,<<"test501">>},
    {PatternID2,<<"test601">>},
    {PatternID2,<<"test701">>},
    {PatternID2,<<"test801">>},
    {PatternID2,<<"test901">>}
  ]).

get_oid(Config)->
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),

  ecomet_user:on_init_state(),

  FunPatName=
    fun(OID,Acc)->
      Object=ecomet_object:construct(OID),
      {ok,Pattern}=ecomet_object:read_field(Object,<<".pattern">>),
      {ok,Name}=ecomet_object:read_field(Object,<<".name">>),
      [{Pattern,Name}|Acc]
    end,

  %-------No paging---------------
  OIDList1=ecomet_query:get([root],[<<".oid">>],{<<"string1">>,'=',<<"value1">>}),
  20=length(OIDList1),
  Result1=lists:foldr(FunPatName,[],OIDList1),
  [
    {PatternID1,<<"test1">>},
    {PatternID1,<<"test101">>},
    {PatternID1,<<"test201">>},
    {PatternID1,<<"test301">>},
    {PatternID1,<<"test401">>},
    {PatternID1,<<"test501">>},
    {PatternID1,<<"test601">>},
    {PatternID1,<<"test701">>},
    {PatternID1,<<"test801">>},
    {PatternID1,<<"test901">>},
    {PatternID2,<<"test1">>},
    {PatternID2,<<"test101">>},
    {PatternID2,<<"test201">>},
    {PatternID2,<<"test301">>},
    {PatternID2,<<"test401">>},
    {PatternID2,<<"test501">>},
    {PatternID2,<<"test601">>},
    {PatternID2,<<"test701">>},
    {PatternID2,<<"test801">>},
    {PatternID2,<<"test901">>}
  ]=Result1,
  % Order DESC
  OIDList2=ecomet_query:get([root],[<<".oid">>],{<<"string1">>,'=',<<"value1">>},[{order,[{<<".oid">>,'DESC'}]}]),
  20=length(OIDList2),
  Result2=lists:foldr(FunPatName,[],OIDList2),
  [
    {PatternID1,<<"test1">>},
    {PatternID1,<<"test101">>},
    {PatternID1,<<"test201">>},
    {PatternID1,<<"test301">>},
    {PatternID1,<<"test401">>},
    {PatternID1,<<"test501">>},
    {PatternID1,<<"test601">>},
    {PatternID1,<<"test701">>},
    {PatternID1,<<"test801">>},
    {PatternID1,<<"test901">>},
    {PatternID2,<<"test1">>},
    {PatternID2,<<"test101">>},
    {PatternID2,<<"test201">>},
    {PatternID2,<<"test301">>},
    {PatternID2,<<"test401">>},
    {PatternID2,<<"test501">>},
    {PatternID2,<<"test601">>},
    {PatternID2,<<"test701">>},
    {PatternID2,<<"test801">>},
    {PatternID2,<<"test901">>}
  ]=lists:reverse(Result2),

  %-------Paging---------------
  {20,OIDList3}=ecomet_query:get([root],[<<".oid">>],{<<"string1">>,'=',<<"value1">>},[{page,{2,2}}]),
  2=length(OIDList3),
  Result3=lists:foldr(FunPatName,[],OIDList3),
  [
    {PatternID1,<<"test201">>},
    {PatternID1,<<"test301">>}
  ]=Result3,
  % Order DESC
  {20,OIDList4}=ecomet_query:get([root],[<<".oid">>],{<<"string1">>,'=',<<"value1">>},[{page,{2,2}},{order,[{<<".oid">>,'DESC'}]}]),
  2=length(OIDList4),
  Result4=lists:foldr(FunPatName,[],OIDList4),
  [
    {PatternID2,<<"test601">>},
    {PatternID2,<<"test701">>}
  ]=lists:reverse(Result4).


get_count(Config)->
  PatternID1=?config(pattern_id1,Config),

  ecomet_user:on_init_state(),

  20=ecomet_query:get([root],[count],{<<"string1">>,'=',<<"value1">>}),

  10=ecomet_query:get([root],[count],{'AND',[
    {<<".pattern">>,'=',PatternID1},
    {<<"string1">>,'=',<<"value1">>}
  ]}),

  6=ecomet_query:get([root],[count],{'AND',[
    {<<".pattern">>,'=',PatternID1},
    {<<"string1">>,'=',<<"value1">>},
    {'OR',[
      {<<"string2">>,'=',<<"value1">>},
      {<<"string3">>,'=',<<"value1">>}
    ]}
  ]}).

get_alias(_Config)->
  ecomet_user:on_init_state(),
  {<<"name1">>,<<"name1">>} =ecomet_query:get_alias(<<"name1">>,2),
  {<<"alias1">>,<<"name1">>} =ecomet_query:get_alias({<<"alias1">>,<<"name1">>},2),
  {<<"2">>,{sum,<<"name1">>}} =ecomet_query:get_alias({sum,<<"name1">>},2),
  {<<"alias1">>,{sum,<<"name1">>}} =ecomet_query:get_alias({<<"alias1">>,{sum,<<"name1">>}},2),
  F=fun(_)->ok end,
  {<<"alias1">>,F} =ecomet_query:get_alias({<<"alias1">>,F},2),
  {<<"2">>,F} =ecomet_query:get_alias(F,2).


-record(get,{value,args,aggregate}).
read_fun(_Config)->
  ecomet_user:on_init_state(),
  %-------count--------
  #get{value=F1,args=[],aggregate=Agr1}=ecomet_query:read_fun(count),
  1=F1(#{}),
  2=Agr1(2,none),
  5=Agr1(2,3),
  %------field---------
  #get{value=F2,args=[<<"name1">>],aggregate=undefined}=ecomet_query:read_fun(<<"name1">>),
  <<"value1">> =F2(#{<<"name1">>=><<"value1">>,<<"name2">>=>"value2"}),
  %-----error----------
  ?assertError({invalid_query_field,_},ecomet_query:read_fun({some_invalid_stuff})),
  %-----{Fun,Arguments}------------
  ConcatFun=
    fun([V1,V2])-><<V1/binary,V2/binary>> end,
  % case 1. fields
  #get{value=F3,args=[<<"field1">>,<<"field2">>],aggregate=undefined}=
    ecomet_query:read_fun({ConcatFun,[<<"field1">>,<<"field2">>]}),
  <<"value1value2">>=F3(#{<<"field1">>=><<"value1">>,<<"field2">>=><<"value2">>}),
  % case 2. embedded fun
  #get{value=F4,args=[<<"field1">>,<<"field2">>,<<"field3">>],aggregate=undefined}=
    ecomet_query:read_fun({ConcatFun,[{ConcatFun,[<<"field1">>,<<"field2">>]},<<"field3">>]}),
  <<"value1value2value3">>=F4(#{<<"field1">>=><<"value1">>,<<"field2">>=><<"value2">>,<<"field3">>=><<"value3">>}),
  %-------{Aggr,Field}--------------
  % SUM
  #get{value=F5,args=[<<"field1">>],aggregate=Aggr5}=
    ecomet_query:read_fun({sum,<<"field1">>}),
  5=F5(#{<<"field1">>=>5,<<"field2">>=>7}),
  none=Aggr5(none,none),
  10=Aggr5(none,10),
  5=Aggr5(5,none),
  7=Aggr5(5,2),
  % MIN
  #get{value=F6,args=[<<"field1">>],aggregate=Aggr6}=
    ecomet_query:read_fun({min,<<"field1">>}),
  5=F6(#{<<"field1">>=>5,<<"field2">>=>7}),
  5=Aggr6(5,7),
  3=Aggr6(5,3),
  % MAX
  #get{value=F7,args=[<<"field1">>],aggregate=Aggr7}=
    ecomet_query:read_fun({max,<<"field1">>}),
  5=F7(#{<<"field1">>=>5,<<"field2">>=>7}),
  7=Aggr7(5,7),
  5=Aggr7(5,3),
  %-------{Aggr,Field}--------------
  MulFun=fun([V1,V2])->V1*V2 end,
  #get{value=F8,args=[<<"field1">>,<<"field2">>],aggregate=Aggr8}=
    ecomet_query:read_fun({sum,{MulFun,[<<"field1">>,<<"field2">>]}}),
  35=F8(#{<<"field1">>=>5,<<"field2">>=>7}),
  40=Aggr8(35,5).

-record(field,{alias,value}).
read_map(_Config)->
  ecomet_user:on_init_state(),
  FunConcat=fun([V1,V2])-><<V1/binary,V2/binary>> end,
  Fields=[
    <<"field1">>,
    {<<"concat2">>,{FunConcat,[<<"field2">>,<<"field3">>]}},
    {FunConcat,[<<"field4">>,{FunConcat,[<<"field5">>,<<"field6">>]}]}
  ],
  FieldsMap=ecomet_query:read_map(Fields),

  #field{alias = <<"field1">>,value = Get1}=maps:get(1,FieldsMap),
  #get{args = [<<"field1">>],aggregate=undefined}=Get1,

  #field{alias = <<"concat2">>,value = Get2}=maps:get(2,FieldsMap),
  #get{args = [<<"field2">>,<<"field3">>],aggregate=undefined}=Get2,

  #field{alias = <<"3">>,value = Get3}=maps:get(3,FieldsMap),
  #get{args = [<<"field4">>,<<"field5">>,<<"field6">>],aggregate=undefined}=Get3.

is_aggregate(_Config)->
  ecomet_user:on_init_state(),
  FunAdd=fun([V1,V2])->V1+V2 end,
  Fields1=[
    <<"field1">>,
    {<<"sum2">>,{FunAdd,[<<"field2">>,<<"field3">>]}},
    {FunAdd,[<<"field4">>,{FunAdd,[<<"field5">>,<<"field6">>]}]}
  ],
  FieldsMap1=ecomet_query:read_map(Fields1),
  false=ecomet_query:is_aggregate(FieldsMap1),

  Fields2=[
    {sum,<<"field1">>},
    {<<"sum2">>,{max,{FunAdd,[<<"field2">>,<<"field3">>]}}},
    {min,{FunAdd,[<<"field4">>,{FunAdd,[<<"field5">>,<<"field6">>]}]}}
  ],
  FieldsMap2=ecomet_query:read_map(Fields2),
  true=ecomet_query:is_aggregate(FieldsMap2),

  Fields3=[
    {sum,<<"field1">>},
    {<<"sum2">>,{FunAdd,[<<"field2">>,<<"field3">>]}},
    {min,{FunAdd,[<<"field4">>,{FunAdd,[<<"field5">>,<<"field6">>]}]}}
  ],
  FieldsMap3=ecomet_query:read_map(Fields3),
  ?assertError(mix_aggegated_and_plain_fields,ecomet_query:is_aggregate(FieldsMap3)).

read_up(Config)->
  ecomet_user:on_init_state(),
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),

  OIDList=ecomet_query:get([root],[<<".oid">>],{<<"string1">>,'=',<<"value1">>}),
  20=length(OIDList),
  Fields=
    [
      {PatternID1,<<"test1">>},
      {PatternID1,<<"test101">>},
      {PatternID1,<<"test201">>},
      {PatternID1,<<"test301">>},
      {PatternID1,<<"test401">>},
      {PatternID1,<<"test501">>},
      {PatternID1,<<"test601">>},
      {PatternID1,<<"test701">>},
      {PatternID1,<<"test801">>},
      {PatternID1,<<"test901">>},
      {PatternID2,<<"test1">>},
      {PatternID2,<<"test101">>},
      {PatternID2,<<"test201">>},
      {PatternID2,<<"test301">>},
      {PatternID2,<<"test401">>},
      {PatternID2,<<"test501">>},
      {PatternID2,<<"test601">>},
      {PatternID2,<<"test701">>},
      {PatternID2,<<"test801">>},
      {PatternID2,<<"test901">>}
    ],

  % no lock
  ReadFun1=ecomet_query:read_up(none),
  lists:foreach(fun({OID,{Pattern,Name}})->
    #{
      <<".pattern">>:=Pattern,
      <<".name">>:=Name
    }=ReadFun1(OID,[<<".pattern">>,<<".name">>])
                end,lists:zip(OIDList,Fields)),

  % dirty
  ReadFun2=ecomet_query:read_up(dirty),
  lists:foreach(fun({OID,{Pattern,Name}})->
    #{
      <<".pattern">>:=Pattern,
      <<".name">>:=Name
    }=ReadFun2(OID,[<<".pattern">>,<<".name">>])
                end,lists:zip(OIDList,Fields)),

  %-----ReadLock-----------
  ReadFun3=ecomet_query:read_up(read),
  % No transaction
  ?assertError(no_transaction,lists:foreach(fun({OID,{Pattern,Name}})->
    #{
      <<".pattern">>:=Pattern,
      <<".name">>:=Name
    }=ReadFun3(OID,[<<".pattern">>,<<".name">>])
                                            end,lists:zip(OIDList,Fields))),
  % In transaction
  {ok,_}=ecomet_transaction:internal(fun()->
    lists:foreach(fun({OID,{Pattern,Name}})->
      #{
        <<".pattern">>:=Pattern,
        <<".name">>:=Name
      }=ReadFun3(OID,[<<".pattern">>,<<".name">>])
                  end,lists:zip(OIDList,Fields))
                                     end),

  %-----WriteLock-----------
  ReadFun4=ecomet_query:read_up(write),
  {ok,_}=ecomet_transaction:internal(fun()->
    lists:foreach(fun({OID,{Pattern,Name}})->
      #{
        <<".pattern">>:=Pattern,
        <<".name">>:=Name
      }=ReadFun4(OID,[<<".pattern">>,<<".name">>])
                  end,lists:zip(OIDList,Fields))
                                     end).

reorder_fields(_Config)->
  Template={none,none,none,none,none},
  % empty
  [none,none,none,none,none]=ecomet_query:reorder_fields([],[],Template),

  ?assertError(function_clause,ecomet_query:reorder_fields([1,2,3,4,5],[],{none,none,none,none,none})),
  ?assertError(function_clause,ecomet_query:reorder_fields([],[1,2,3,4,5],{none,none,none,none,none})),

  [v3,v2,v1,v5,v4]=ecomet_query:reorder_fields([v1,v2,v3,v4,v5],[3,2,1,5,4],{none,none,none,none,none}).


insert_to_group(_Config)->
  %----Level 1-------
  L1G1=ecomet_query:insert_to_group([<<"l1_g1">>],fun(GroupAcc)->[[<<"o1_v2">>,<<"o1_v3">>]|GroupAcc] end,[],#{}),
  L1G1=#{<<"l1_g1">>=>[
    [<<"o1_v2">>,<<"o1_v3">>]
  ]},
  % Add row
  L1G2=ecomet_query:insert_to_group([<<"l1_g1">>],fun(GroupAcc)->[[<<"o2_v2">>,<<"o2_v3">>]|GroupAcc] end,[],L1G1),
  L1G2=#{
    <<"l1_g1">>=>[
      [<<"o2_v2">>,<<"o2_v3">>],
      [<<"o1_v2">>,<<"o1_v3">>]
    ]
  },
  % Add group
  L1G3=ecomet_query:insert_to_group([<<"l1_g2">>],fun(GroupAcc)->[[<<"o3_v2">>,<<"o3_v3">>]|GroupAcc] end,[],L1G2),
  L1G3=#{
    <<"l1_g1">>=>[
      [<<"o2_v2">>,<<"o2_v3">>],
      [<<"o1_v2">>,<<"o1_v3">>]
    ],
    <<"l1_g2">>=>[
      [<<"o3_v2">>,<<"o3_v3">>]
    ]
  },
  %----Level 2-------
  L2G1=ecomet_query:insert_to_group([<<"l1_g1">>,<<"l2_g1">>],fun(GroupAcc)->[[<<"o1_v2">>,<<"o1_v3">>]|GroupAcc] end,[],#{}),
  L2G1=#{
    <<"l1_g1">>=>#{
      <<"l2_g1">>=>[
        [<<"o1_v2">>,<<"o1_v3">>]
      ]
    }
  },
  % Add row
  L2G2=ecomet_query:insert_to_group([<<"l1_g1">>,<<"l2_g1">>],fun(GroupAcc)->[[<<"o2_v2">>,<<"o2_v3">>]|GroupAcc] end,[],L2G1),
  L2G2=#{
    <<"l1_g1">>=>#{
      <<"l2_g1">>=>[
        [<<"o2_v2">>,<<"o2_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>]
      ]
    }
  },
  % Add level1
  L2G3=ecomet_query:insert_to_group([<<"l1_g2">>,<<"l2_g1">>],fun(GroupAcc)->[[<<"o3_v2">>,<<"o3_v3">>]|GroupAcc] end,[],L2G2),
  L2G3=#{
    <<"l1_g1">>=>#{
      <<"l2_g1">>=>[
        [<<"o2_v2">>,<<"o2_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>]
      ]
    },
    <<"l1_g2">>=>#{
      <<"l2_g1">>=>[
        [<<"o3_v2">>,<<"o3_v3">>]
      ]
    }
  },
  % Add level2
  L2G4=ecomet_query:insert_to_group([<<"l1_g2">>,<<"l2_g2">>],fun(GroupAcc)->[[<<"o4_v2">>,<<"o4_v3">>]|GroupAcc] end,[],L2G3),
  L2G4=#{
    <<"l1_g1">>=>#{
      <<"l2_g1">>=>[
        [<<"o2_v2">>,<<"o2_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>]
      ]
    },
    <<"l1_g2">>=>#{
      <<"l2_g1">>=>[
        [<<"o3_v2">>,<<"o3_v3">>]
      ],
      <<"l2_g2">>=>[
        [<<"o4_v2">>,<<"o4_v3">>]
      ]
    }
  }.

sort_groups(_Config)->
  Tree=#{
    <<"l1_g1">>=>#{
      <<"l2_g1">>=>[
        [<<"o2_v2">>,<<"o2_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>]
      ]
    },
    <<"l1_g2">>=>#{
      <<"l2_g1">>=>[
        [<<"o3_v2">>,<<"o3_v3">>]
      ],
      <<"l2_g2">>=>[
        [<<"o4_v2">>,<<"o4_v3">>]
      ]
    }
  },
  [
    {<<"l1_g1">>,[
      {<<"l2_g1">>,[
        [<<"o2_v2">>,<<"o2_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>]
      ]}
    ]},
    {<<"l1_g2">>,[
      {<<"l2_g1">>,[
        [<<"o3_v2">>,<<"o3_v3">>]
      ]},
      {<<"l2_g2">>,[
        [<<"o4_v2">>,<<"o4_v3">>]
      ]}
    ]}
  ]=ecomet_query:sort_groups(2,Tree).

merge_trees(_Config)->
  ConcatFun=fun(Group,Acc)->Group++Acc end,
  %-----Level 1------------
  % Empty
  []=ecomet_query:merge_trees(1,[],[],ConcatFun),
  L1T1=[{<<"l1_g1">>,[[<<"o1_v2">>,<<"o1_v3">>]]}],
  L1T1=ecomet_query:merge_trees(1,L1T1,[],ConcatFun),
  L1T1=ecomet_query:merge_trees(1,[],L1T1,ConcatFun),
  % Same group
  L1T2=[{<<"l1_g1">>,[[<<"o2_v2">>,<<"o2_v3">>]]}],
  L1Res1=ecomet_query:merge_trees(1,L1T1,L1T2,ConcatFun),
  [
    {<<"l1_g1">>,[
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o2_v2">>,<<"o2_v3">>]
    ]}
  ]=L1Res1,
  % Diff groups
  L1T3=[{<<"l1_g2">>,[[<<"o3_v2">>,<<"o3_v3">>]]}],
  L1Res2=ecomet_query:merge_trees(1,L1T3,L1T1,ConcatFun),
  [
    {<<"l1_g1">>,[
      [<<"o1_v2">>,<<"o1_v3">>]
    ]},
    {<<"l1_g2">>,[
      [<<"o3_v2">>,<<"o3_v3">>]
    ]}
  ]=L1Res2,
  % Mixed
  L1Res3=ecomet_query:merge_trees(1,L1Res1,L1Res2,ConcatFun),
  [
    {<<"l1_g1">>,[
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o2_v2">>,<<"o2_v3">>],
      [<<"o1_v2">>,<<"o1_v3">>]
    ]},
    {<<"l1_g2">>,[
      [<<"o3_v2">>,<<"o3_v3">>]
    ]}
  ]=L1Res3,
  % Change to tress order - changes only leafs order
  L1Res4=ecomet_query:merge_trees(1,L1Res2,L1Res1,ConcatFun),
  [
    {<<"l1_g1">>,[
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o2_v2">>,<<"o2_v3">>]
    ]},
    {<<"l1_g2">>,[
      [<<"o3_v2">>,<<"o3_v3">>]
    ]}
  ]=L1Res4,

  %-----Level 2------------
  % Empty
  []=ecomet_query:merge_trees(1,[],[],ConcatFun),
  L2T1=[
    {<<"l1_g1">>,[
      {<<"l2_g1">>,[
        [<<"o2_v2">>,<<"o2_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>]
      ]}
    ]},
    {<<"l1_g2">>,[
      {<<"l2_g1">>,[
        [<<"o3_v2">>,<<"o3_v3">>]
      ]},
      {<<"l2_g2">>,[
        [<<"o4_v2">>,<<"o4_v3">>]
      ]}
    ]}
  ],
  L2T1=ecomet_query:merge_trees(2,L2T1,[],ConcatFun),
  L2T1=ecomet_query:merge_trees(2,[],L2T1,ConcatFun),
  L2T2=[
    {<<"l1_g1">>,[
      {<<"l2_g1">>,[
        [<<"o5_v2">>,<<"o5_v3">>]
      ]},
      {<<"l2_g3">>,[
        [<<"o6_v2">>,<<"o6_v3">>]
      ]}
    ]},
    {<<"l1_g2">>,[
      {<<"l2_g1">>,[
        [<<"o7_v2">>,<<"o7_v3">>]
      ]},
      {<<"l2_g2">>,[
        [<<"o8_v2">>,<<"o8_v3">>]
      ]}
    ]},
    {<<"l1_g3">>,[
      {<<"l2_g1">>,[
        [<<"o9_v2">>,<<"o9_v3">>]
      ]},
      {<<"l2_g2">>,[
        [<<"o10_v2">>,<<"o10_v3">>]
      ]}
    ]}
  ],
  L2Res1=ecomet_query:merge_trees(2,L2T1,L2T2,ConcatFun),
  [
    {<<"l1_g1">>,[
      {<<"l2_g1">>,[
        [<<"o2_v2">>,<<"o2_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>],
        [<<"o5_v2">>,<<"o5_v3">>]
      ]},
      {<<"l2_g3">>,[
        [<<"o6_v2">>,<<"o6_v3">>]
      ]}
    ]},
    {<<"l1_g2">>,[
      {<<"l2_g1">>,[
        [<<"o3_v2">>,<<"o3_v3">>],
        [<<"o7_v2">>,<<"o7_v3">>]
      ]},
      {<<"l2_g2">>,[
        [<<"o4_v2">>,<<"o4_v3">>],
        [<<"o8_v2">>,<<"o8_v3">>]
      ]}
    ]},
    {<<"l1_g3">>,[
      {<<"l2_g1">>,[
        [<<"o9_v2">>,<<"o9_v3">>]
      ]},
      {<<"l2_g2">>,[
        [<<"o10_v2">>,<<"o10_v3">>]
      ]}
    ]}
  ]=L2Res1,

  %-----------Aggregate---------------
  Sum=fun(V,Acc)-> V+Acc end,
  Max=fun(V,Acc)-> if V>Acc->V; true->Acc end end,
  Min=fun(V,Acc)-> if V<Acc->V; true->Acc end end,
  Count=fun(V,Acc)-> V+Acc end,
  RowAggregate=[Sum,Max,Min,Count],
  AggregateFun=fun(Row,Acc)->[F(V,Vacc)||{V,Vacc,F}<-lists:zip3(Row,Acc,RowAggregate)] end,
  % Empty
  []=ecomet_query:merge_trees(2,[],[],AggregateFun),
  L2T1Aggr=[
    {<<"l1_g1">>,[
      {<<"l2_g1">>,[10,<<"o1">>,<<"o2">>,5]}
    ]},
    {<<"l1_g2">>,[
      {<<"l2_g1">>,[30,<<"o4">>,<<"o5">>,20]},
      {<<"l2_g2">>,[40,<<"o6">>,<<"o7">>,30]}
    ]}
  ],
  L2T1Aggr=ecomet_query:merge_trees(2,L2T1Aggr,[],AggregateFun),
  L2T1Aggr=ecomet_query:merge_trees(2,[],L2T1Aggr,AggregateFun),
  % Merge
  L2T2Aggr=[
    {<<"l1_g1">>,[
      {<<"l2_g1">>,[5,<<"o2">>,<<"o1">>,10]},
      {<<"l2_g3">>,[10,<<"v2">>,<<"v1">>,5]}
    ]},
    {<<"l1_g2">>,[
      {<<"l2_g1">>,[5,<<"o3">>,<<"o4">>,20]},
      {<<"l2_g2">>,[20,<<"o8">>,<<"o9">>,70]}
    ]},
    {<<"l1_g3">>,[
      {<<"l2_g1">>,[10,<<"o9">>,<<"o8">>,5]},
      {<<"l2_g2">>,[20,<<"v9">>,<<"v8">>,15]}
    ]}
  ],
  L2Res1Aggr=ecomet_query:merge_trees(2,L2T1Aggr,L2T2Aggr,AggregateFun),
  % Tree order is no sense
  L2Res1Aggr=ecomet_query:merge_trees(2,L2T2Aggr,L2T1Aggr,AggregateFun),
  [
    {<<"l1_g1">>,[
      {<<"l2_g1">>,[15,<<"o2">>,<<"o1">>,15]},   %  [10,<<"o1">>,<<"o2">>,5] , [5,<<"o2">>,<<"o1">>,10]
      {<<"l2_g3">>,[10,<<"v2">>,<<"v1">>,5]}    % [10,<<"v2">>,<<"v1">>,5]
    ]},
    {<<"l1_g2">>,[
      {<<"l2_g1">>,[35,<<"o4">>,<<"o4">>,40]}, % [30,<<"o4">>,<<"o5">>,20] , [5,<<"o3">>,<<"o4">>,20]
      {<<"l2_g2">>,[60,<<"o8">>,<<"o7">>,100]}  % [40,<<"o6">>,<<"o7">>,30] , [20,<<"o8">>,<<"o9">>,70]
    ]},
    {<<"l1_g3">>,[
      {<<"l2_g1">>,[10,<<"o9">>,<<"o8">>,5]},
      {<<"l2_g2">>,[20,<<"v9">>,<<"v8">>,15]}
    ]}
  ]=L2Res1Aggr.

grouped_resut(_Config)->
  ConcatFun=
    fun(Head,Rows)->
      [Head++Tail||Tail<-Rows]
    end,
  %------------Level 1------------------
  L1T1=
    [
      {<<"l1_g1">>,[
        [<<"o1_v2">>,<<"o1_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>],
        [<<"o2_v2">>,<<"o2_v3">>]
      ]},
      {<<"l1_g2">>,[
        [<<"o3_v2">>,<<"o3_v3">>]
      ]}
    ],
  % Group, Order = none
  [
    [<<"l1_g1">>,[
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o2_v2">>,<<"o2_v3">>]
    ]],
    [<<"l1_g2">>,[
      [<<"o3_v2">>,<<"o3_v3">>]
    ]]
  ]=ecomet_query:grouped_resut([{true,none}],L1T1,ConcatFun,[]),
  % Group, Order = 'ASC'
  [
    [<<"l1_g1">>,[
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o2_v2">>,<<"o2_v3">>]
    ]],
    [<<"l1_g2">>,[
      [<<"o3_v2">>,<<"o3_v3">>]
    ]]
  ]=ecomet_query:grouped_resut([{true,'ASC'}],L1T1,ConcatFun,[]),
  % Group, Order = 'DESC'
  [
    [<<"l1_g2">>,[
      [<<"o3_v2">>,<<"o3_v3">>]
    ]],
    [<<"l1_g1">>,[
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o2_v2">>,<<"o2_v3">>]
    ]]
  ]=ecomet_query:grouped_resut([{true,'DESC'}],L1T1,ConcatFun,[]),
  % Sorted, Order = 'ASC'
  [
    [<<"l1_g1">>,<<"o1_v2">>,<<"o1_v3">>],
    [<<"l1_g1">>,<<"o1_v2">>,<<"o1_v3">>],
    [<<"l1_g1">>,<<"o2_v2">>,<<"o2_v3">>],

    [<<"l1_g2">>,<<"o3_v2">>,<<"o3_v3">>]
  ]=ecomet_query:grouped_resut([{false,'ASC'}],L1T1,ConcatFun,[]),
  % Sorted, Order = 'DESC'
  [
    [<<"l1_g2">>,<<"o3_v2">>,<<"o3_v3">>],

    [<<"l1_g1">>,<<"o1_v2">>,<<"o1_v3">>],
    [<<"l1_g1">>,<<"o1_v2">>,<<"o1_v3">>],
    [<<"l1_g1">>,<<"o2_v2">>,<<"o2_v3">>]
  ]=ecomet_query:grouped_resut([{false,'DESC'}],L1T1,ConcatFun,[]),

  %------------Level 2------------------
  L2T1=
    [
      {<<"l1_g1">>,[
        {<<"l2_g1">>,[
          [<<"o2_v2">>,<<"o2_v3">>],
          [<<"o1_v2">>,<<"o1_v3">>],
          [<<"o5_v2">>,<<"o5_v3">>]
        ]},
        {<<"l2_g3">>,[
          [<<"o6_v2">>,<<"o6_v3">>]
        ]}
      ]},
      {<<"l1_g2">>,[
        {<<"l2_g1">>,[
          [<<"o3_v2">>,<<"o3_v3">>],
          [<<"o7_v2">>,<<"o7_v3">>]
        ]},
        {<<"l2_g2">>,[
          [<<"o4_v2">>,<<"o4_v3">>],
          [<<"o8_v2">>,<<"o8_v3">>]
        ]}
      ]},
      {<<"l1_g3">>,[
        {<<"l2_g1">>,[
          [<<"o9_v2">>,<<"o9_v3">>]
        ]},
        {<<"l2_g2">>,[
          [<<"o10_v2">>,<<"o10_v3">>]
        ]}
      ]}
    ],
  % Group/Group, ASC/DESC
  [
    [<<"l1_g1">>,[
      [<<"l2_g3">>,[
        [<<"o6_v2">>,<<"o6_v3">>]
      ]],
      [<<"l2_g1">>,[
        [<<"o2_v2">>,<<"o2_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>],
        [<<"o5_v2">>,<<"o5_v3">>]
      ]]
    ]],
    [<<"l1_g2">>,[
      [<<"l2_g2">>,[
        [<<"o4_v2">>,<<"o4_v3">>],
        [<<"o8_v2">>,<<"o8_v3">>]
      ]],
      [<<"l2_g1">>,[
        [<<"o3_v2">>,<<"o3_v3">>],
        [<<"o7_v2">>,<<"o7_v3">>]
      ]]
    ]],
    [<<"l1_g3">>,[
      [<<"l2_g2">>,[
        [<<"o10_v2">>,<<"o10_v3">>]
      ]],
      [<<"l2_g1">>,[
        [<<"o9_v2">>,<<"o9_v3">>]
      ]]
    ]]
  ]=ecomet_query:grouped_resut([{true,'ASC'},{true,'DESC'}],L2T1,ConcatFun,[]),
  % Group/Sorting, DESC/ASC
  [
    [<<"l1_g3">>,[
      [<<"l2_g1">>,<<"o9_v2">>,<<"o9_v3">>],
      [<<"l2_g2">>,<<"o10_v2">>,<<"o10_v3">>]
    ]],
    [<<"l1_g2">>,[
      [<<"l2_g1">>,<<"o3_v2">>,<<"o3_v3">>],
      [<<"l2_g1">>,<<"o7_v2">>,<<"o7_v3">>],
      [<<"l2_g2">>,<<"o4_v2">>,<<"o4_v3">>],
      [<<"l2_g2">>,<<"o8_v2">>,<<"o8_v3">>]
    ]],
    [<<"l1_g1">>,[
      [<<"l2_g1">>,<<"o2_v2">>,<<"o2_v3">>],
      [<<"l2_g1">>,<<"o1_v2">>,<<"o1_v3">>],
      [<<"l2_g1">>,<<"o5_v2">>,<<"o5_v3">>],
      [<<"l2_g3">>,<<"o6_v2">>,<<"o6_v3">>]
    ]]
  ]=ecomet_query:grouped_resut([{true,'DESC'},{false,'ASC'}],L2T1,ConcatFun,[]),
  % Sorting/Sorting, DESC/DESC
  [
    [<<"l1_g3">>,<<"l2_g2">>,<<"o10_v2">>,<<"o10_v3">>],
    [<<"l1_g3">>,<<"l2_g1">>,<<"o9_v2">>,<<"o9_v3">>],
    [<<"l1_g2">>,<<"l2_g2">>,<<"o4_v2">>,<<"o4_v3">>],
    [<<"l1_g2">>,<<"l2_g2">>,<<"o8_v2">>,<<"o8_v3">>],
    [<<"l1_g2">>,<<"l2_g1">>,<<"o3_v2">>,<<"o3_v3">>],
    [<<"l1_g2">>,<<"l2_g1">>,<<"o7_v2">>,<<"o7_v3">>],
    [<<"l1_g1">>,<<"l2_g3">>,<<"o6_v2">>,<<"o6_v3">>],
    [<<"l1_g1">>,<<"l2_g1">>,<<"o2_v2">>,<<"o2_v3">>],
    [<<"l1_g1">>,<<"l2_g1">>,<<"o1_v2">>,<<"o1_v3">>],
    [<<"l1_g1">>,<<"l2_g1">>,<<"o5_v2">>,<<"o5_v3">>]
  ]=ecomet_query:grouped_resut([{false,'DESC'},{false,'DESC'}],L2T1,ConcatFun,[]).

groups_page(_Config)->
  Tree=
    [
      {<<"l1_g1">>,[
        [<<"o1_v2">>,<<"o1_v3">>],
        [<<"o1_v2">>,<<"o1_v3">>],
        [<<"o2_v2">>,<<"o2_v3">>]
      ]},
      {<<"l1_g2">>,[
        [<<"o3_v2">>,<<"o3_v3">>]
      ]},
      {<<"l1_g3">>,[
        [<<"o4_v4">>,<<"o5_v5">>]
      ]},
      {<<"l1_g4">>,[
        [<<"o6_v6">>,<<"o7_v7">>]
      ]},
      {<<"l1_g5">>,[
        [<<"o8_v8">>,<<"o8_v8">>]
      ]}
    ],
  % EMPTY
  []=ecomet_query:groups_page(Tree,{4,2}),
  % Not full 'ASC'
  [
    {<<"l1_g5">>,[
      [<<"o8_v8">>,<<"o8_v8">>]
    ]}
  ]=ecomet_query:groups_page(Tree,{3,2}),
  % ASC
  [
    {<<"l1_g1">>,[
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o2_v2">>,<<"o2_v3">>]
    ]},
    {<<"l1_g2">>,[
      [<<"o3_v2">>,<<"o3_v3">>]
    ]}
  ]=ecomet_query:groups_page(Tree,{1,2}),
  % Page 2, items 2
  [
%%    {<<"l1_g1">>,[
%%      [<<"o1_v2">>,<<"o1_v3">>],
%%      [<<"o1_v2">>,<<"o1_v3">>],
%%      [<<"o2_v2">>,<<"o2_v3">>]
%%    ]},
%%    {<<"l1_g2">>,[
%%      [<<"o3_v2">>,<<"o3_v3">>]
%%    ]},
    {<<"l1_g3">>,[
      [<<"o4_v4">>,<<"o5_v5">>]
    ]},
    {<<"l1_g4">>,[
      [<<"o6_v6">>,<<"o7_v7">>]
    ]}
%%    {<<"l1_g5">>,[
%%      [<<"o8_v8">>,<<"o8_v8">>]
%%    ]}
  ]=ecomet_query:groups_page(Tree,{2,2}),
  % Page 1, items 3
  [
    {<<"l1_g1">>,[
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o1_v2">>,<<"o1_v3">>],
      [<<"o2_v2">>,<<"o2_v3">>]
    ]},
    {<<"l1_g2">>,[
      [<<"o3_v2">>,<<"o3_v3">>]
    ]},
    {<<"l1_g3">>,[
      [<<"o4_v4">>,<<"o5_v5">>]
    ]}
%%    {<<"l1_g4">>,[
%%      [<<"o6_v6">>,<<"o7_v7">>]
%%    ]},
%%    {<<"l1_g5">>,[
%%      [<<"o8_v8">>,<<"o8_v8">>]
%%    ]}
  ]=ecomet_query:groups_page(Tree,{1,3}).

compare_rows(_Config)->
  true=ecomet_query:compare_rows([none,none,none],[3,4,5],[4,3,5]),
  true=ecomet_query:compare_rows(['ASC',none,none],[3,4,5],[4,3,5]),
  false=ecomet_query:compare_rows(['DESC',none,none],[3,4,5],[4,3,5]),
  false=ecomet_query:compare_rows([none,'ASC',none],[3,4,5],[4,3,5]),

  true=ecomet_query:compare_rows([none,'DESC',none,'ASC'],[3,4,5,7],[2,4,5,8]).

sort_leafs(_Config)->
  L2T1=
    [
      {<<"l1_g1">>,[
        {<<"l2_g1">>,[<<"o2_v2">>,<<"o6_v6">>]},
        {<<"l2_g2">>,[<<"o4_v4">>,<<"o3_v3">>]},
        {<<"l2_g3">>,[<<"o1_v1">>,<<"o4_v4">>]}
      ]},
      {<<"l1_g2">>,[
        {<<"l2_g1">>,[<<"o3_v2">>,<<"o3_v3">>]},
        {<<"l2_g2">>,[<<"o8_v2">>,<<"o8_v3">>]},
        {<<"l2_g3">>,[<<"o4_v4">>,<<"o2_v2">>]}
      ]}
    ],
  CompareFun=
    fun({_,Row1},{_,Row2})->
      ecomet_query:compare_rows([none,'DESC'],Row1,Row2)
    end,
  [
    {<<"l1_g1">>,[
      [<<"l2_g1">>,<<"o2_v2">>,<<"o6_v6">>],
      [<<"l2_g3">>,<<"o1_v1">>,<<"o4_v4">>],
      [<<"l2_g2">>,<<"o4_v4">>,<<"o3_v3">>]
    ]},
    {<<"l1_g2">>,[
      [<<"l2_g2">>,<<"o8_v2">>,<<"o8_v3">>],
      [<<"l2_g1">>,<<"o3_v2">>,<<"o3_v3">>],
      [<<"l2_g3">>,<<"o4_v4">>,<<"o2_v2">>]
    ]}
  ]=ecomet_query:sort_leafs(1,L2T1,CompareFun).

%%----------------------------------------------------------------------
%%  SIMPLE SEARCH
%%----------------------------------------------------------------------
simple_no_sort(Config)->
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),
  ecomet_user:on_init_state(),
  ConcatFun=
    fun([V1,V2])-><<V1/binary,"_",V2/binary>> end,
  Fields=[
    <<".pattern">>,
    {<<"alias1">>,<<".name">>},
    {<<"alias2">>,{ConcatFun,[<<"string2">>,<<"string3">>]}}
  ],
  RS=ecomet_query:get([root],rs,{<<"string1">>,'=',<<"value1">>}),
  %----------No params--------------
  {Map1,Reduce1}=ecomet_query:compile_map_reduce(get,Fields,[]),
  LocalResult=Map1(RS),
  [
    [PatternID1,<<"test1">>,<<"value0_value0">>],
    [PatternID1,<<"test101">>,<<"value1_value0">>],
    [PatternID1,<<"test201">>,<<"value2_value0">>],
    [PatternID1,<<"test301">>,<<"value3_value0">>],
    [PatternID1,<<"test401">>,<<"value4_value0">>],
    [PatternID1,<<"test501">>,<<"value5_value1">>],
    [PatternID1,<<"test601">>,<<"value6_value1">>],
    [PatternID1,<<"test701">>,<<"value7_value1">>],
    [PatternID1,<<"test801">>,<<"value8_value1">>],
    [PatternID1,<<"test901">>,<<"value9_value1">>],
    [PatternID2,<<"test1">>,<<"value0_value0">>],
    [PatternID2,<<"test101">>,<<"value1_value0">>],
    [PatternID2,<<"test201">>,<<"value2_value0">>],
    [PatternID2,<<"test301">>,<<"value3_value0">>],
    [PatternID2,<<"test401">>,<<"value4_value0">>],
    [PatternID2,<<"test501">>,<<"value5_value1">>],
    [PatternID2,<<"test601">>,<<"value6_value1">>],
    [PatternID2,<<"test701">>,<<"value7_value1">>],
    [PatternID2,<<"test801">>,<<"value8_value1">>],
    [PatternID2,<<"test901">>,<<"value9_value1">>]
  ]=LocalResult,
  {[<<".pattern">>,<<"alias1">>,<<"alias2">>],LocalResult}=Reduce1([LocalResult]),
  %----------Sorting by oid--------------
  {Map2,Reduce2}=ecomet_query:compile_map_reduce(get,[<<".oid">>|Fields],[{order,[{<<".oid">>,'ASC'}]}]),
  LocalResult2=Map2(RS),
  LocalResult=[Tail||[_|Tail]<-LocalResult2],
  {[<<".oid">>,<<".pattern">>,<<"alias1">>,<<"alias2">>],LocalResult2}=Reduce2([LocalResult2]),

  {Map3,Reduce3}=ecomet_query:compile_map_reduce(get,[<<".oid">>|Fields],[{order,[{<<".oid">>,'DESC'}]}]),
  LocalResult3=Map3(RS),
  LocalResult3=lists:reverse(LocalResult2),
  {[<<".oid">>,<<".pattern">>,<<"alias1">>,<<"alias2">>],LocalResult3}=Reduce3([LocalResult3]),
  %-----------Paging----------------------
  {Map4,Reduce4}=ecomet_query:compile_map_reduce(get,Fields,[{page,{2,3}}]),
  RS=Map4(RS),
  {20,{[<<".pattern">>,<<"alias1">>,<<"alias2">>],[
    [PatternID1,<<"test301">>,<<"value3_value0">>],
    [PatternID1,<<"test401">>,<<"value4_value0">>],
    [PatternID1,<<"test501">>,<<"value5_value1">>]
  ]}}=Reduce4([RS]),

  {Map5,Reduce5}=ecomet_query:compile_map_reduce(get,[<<".oid">>|Fields],[{order,[{<<".oid">>,'DESC'}]},{page,{2,3}}]),
  RS=Map5(RS),
  {20,{[<<".oid">>,<<".pattern">>,<<"alias1">>,<<"alias2">>],[
    [_,PatternID2,<<"test601">>,<<"value6_value1">>],
    [_,PatternID2,<<"test501">>,<<"value5_value1">>],
    [_,PatternID2,<<"test401">>,<<"value4_value0">>]
  ]}}=Reduce5([RS]).

grouping_sorting(Config)->
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),
  ecomet_user:on_init_state(),
  ConcatFun=
    fun([V1,V2])-><<V1/binary,"_",V2/binary>> end,
  Fields=[
    <<".pattern">>,
    {<<"alias1">>,<<".name">>},
    {<<"alias2">>,{ConcatFun,[<<"string2">>,<<"string3">>]}}
  ],
  RS=ecomet_query:get([root],rs,{<<"string1">>,'=',<<"value1">>}),
  %----------Sorting--------------
  {Map1,Reduce1}=ecomet_query:compile_map_reduce(get,Fields,[{order,[{<<"alias2">>,'ASC'}]}]),
  LocalResult1=Map1(RS),
  [
    {<<"value0_value0">>,[
      [PatternID1,<<"test1">>],
      [PatternID2,<<"test1">>]
    ]},
    {<<"value1_value0">>,[
      [PatternID1,<<"test101">>],
      [PatternID2,<<"test101">>]
    ]},
    {<<"value2_value0">>,[
      [PatternID1,<<"test201">>],
      [PatternID2,<<"test201">>]
    ]},
    {<<"value3_value0">>,[
      [PatternID1,<<"test301">>],
      [PatternID2,<<"test301">>]
    ]},
    {<<"value4_value0">>,[
      [PatternID1,<<"test401">>],
      [PatternID2,<<"test401">>]
    ]},
    {<<"value5_value1">>,[
      [PatternID1,<<"test501">>],
      [PatternID2,<<"test501">>]
    ]},
    {<<"value6_value1">>,[
      [PatternID1,<<"test601">>],
      [PatternID2,<<"test601">>]
    ]},
    {<<"value7_value1">>,[
      [PatternID1,<<"test701">>],
      [PatternID2,<<"test701">>]
    ]},
    {<<"value8_value1">>,[
      [PatternID1,<<"test801">>],
      [PatternID2,<<"test801">>]
    ]},
    {<<"value9_value1">>,[
      [PatternID1,<<"test901">>],
      [PatternID2,<<"test901">>]
    ]}
  ]=LocalResult1,

  {[<<".pattern">>,<<"alias1">>,<<"alias2">>],[
    [PatternID1,<<"test1">>,<<"value0_value0">>],
    [PatternID2,<<"test1">>,<<"value0_value0">>],
    [PatternID1,<<"test101">>,<<"value1_value0">>],
    [PatternID2,<<"test101">>,<<"value1_value0">>],
    [PatternID1,<<"test201">>,<<"value2_value0">>],
    [PatternID2,<<"test201">>,<<"value2_value0">>],
    [PatternID1,<<"test301">>,<<"value3_value0">>],
    [PatternID2,<<"test301">>,<<"value3_value0">>],
    [PatternID1,<<"test401">>,<<"value4_value0">>],
    [PatternID2,<<"test401">>,<<"value4_value0">>],
    [PatternID1,<<"test501">>,<<"value5_value1">>],
    [PatternID2,<<"test501">>,<<"value5_value1">>],
    [PatternID1,<<"test601">>,<<"value6_value1">>],
    [PatternID2,<<"test601">>,<<"value6_value1">>],
    [PatternID1,<<"test701">>,<<"value7_value1">>],
    [PatternID2,<<"test701">>,<<"value7_value1">>],
    [PatternID1,<<"test801">>,<<"value8_value1">>],
    [PatternID2,<<"test801">>,<<"value8_value1">>],
    [PatternID1,<<"test901">>,<<"value9_value1">>],
    [PatternID2,<<"test901">>,<<"value9_value1">>]
  ]}=Reduce1([LocalResult1]),

  %----------Grouping--------------
  {Map2,Reduce2}=ecomet_query:compile_map_reduce(get,Fields,[{group,[<<"alias1">>]}]),
  LocalResult2=Map2(RS),
  [
    {<<"test1">>,[
      [PatternID1,<<"value0_value0">>],
      [PatternID2,<<"value0_value0">>]
    ]},
    {<<"test101">>,[
      [PatternID1,<<"value1_value0">>],
      [PatternID2,<<"value1_value0">>]
    ]},
    {<<"test201">>,[
      [PatternID1,<<"value2_value0">>],
      [PatternID2,<<"value2_value0">>]
    ]},
    {<<"test301">>,[
      [PatternID1,<<"value3_value0">>],
      [PatternID2,<<"value3_value0">>]
    ]},
    {<<"test401">>,[
      [PatternID1,<<"value4_value0">>],
      [PatternID2,<<"value4_value0">>]
    ]},
    {<<"test501">>,[
      [PatternID1,<<"value5_value1">>],
      [PatternID2,<<"value5_value1">>]
    ]},
    {<<"test601">>,[
      [PatternID1,<<"value6_value1">>],
      [PatternID2,<<"value6_value1">>]
    ]},
    {<<"test701">>,[
      [PatternID1,<<"value7_value1">>],
      [PatternID2,<<"value7_value1">>]
    ]},
    {<<"test801">>,[
      [PatternID1,<<"value8_value1">>],
      [PatternID2,<<"value8_value1">>]
    ]},
    {<<"test901">>,[
      [PatternID1,<<"value9_value1">>],
      [PatternID2,<<"value9_value1">>]
    ]}
  ]=LocalResult2,

  {[<<"alias1">>,<<".pattern">>,<<"alias2">>],[
    [<<"test1">>,[
      [PatternID1,<<"value0_value0">>],
      [PatternID2,<<"value0_value0">>]
    ]],
    [<<"test101">>,[
      [PatternID1,<<"value1_value0">>],
      [PatternID2,<<"value1_value0">>]
    ]],
    [<<"test201">>,[
      [PatternID1,<<"value2_value0">>],
      [PatternID2,<<"value2_value0">>]
    ]],
    [<<"test301">>,[
      [PatternID1,<<"value3_value0">>],
      [PatternID2,<<"value3_value0">>]
    ]],
    [<<"test401">>,[
      [PatternID1,<<"value4_value0">>],
      [PatternID2,<<"value4_value0">>]
    ]],
    [<<"test501">>,[
      [PatternID1,<<"value5_value1">>],
      [PatternID2,<<"value5_value1">>]
    ]],
    [<<"test601">>,[
      [PatternID1,<<"value6_value1">>],
      [PatternID2,<<"value6_value1">>]
    ]],
    [<<"test701">>,[
      [PatternID1,<<"value7_value1">>],
      [PatternID2,<<"value7_value1">>]
    ]],
    [<<"test801">>,[
      [PatternID1,<<"value8_value1">>],
      [PatternID2,<<"value8_value1">>]
    ]],
    [<<"test901">>,[
      [PatternID1,<<"value9_value1">>],
      [PatternID2,<<"value9_value1">>]
    ]]
  ]}=Reduce2([LocalResult2]),

  {Map3,Reduce3}=ecomet_query:compile_map_reduce(get,Fields,[{group,[<<"alias1">>]},{order,[{<<"alias1">>,'DESC'}]}]),
  LocalResult2=Map3(RS),  %% Local result always ascending order

  {[<<"alias1">>,<<".pattern">>,<<"alias2">>],[
    [<<"test901">>,[
      [PatternID1,<<"value9_value1">>],
      [PatternID2,<<"value9_value1">>]
    ]],
    [<<"test801">>,[
      [PatternID1,<<"value8_value1">>],
      [PatternID2,<<"value8_value1">>]
    ]],
    [<<"test701">>,[
      [PatternID1,<<"value7_value1">>],
      [PatternID2,<<"value7_value1">>]
    ]],
    [<<"test601">>,[
      [PatternID1,<<"value6_value1">>],
      [PatternID2,<<"value6_value1">>]
    ]],
    [<<"test501">>,[
      [PatternID1,<<"value5_value1">>],
      [PatternID2,<<"value5_value1">>]
    ]],
    [<<"test401">>,[
      [PatternID1,<<"value4_value0">>],
      [PatternID2,<<"value4_value0">>]
    ]],
    [<<"test301">>,[
      [PatternID1,<<"value3_value0">>],
      [PatternID2,<<"value3_value0">>]
    ]],
    [<<"test201">>,[
      [PatternID1,<<"value2_value0">>],
      [PatternID2,<<"value2_value0">>]
    ]],
    [<<"test101">>,[
      [PatternID1,<<"value1_value0">>],
      [PatternID2,<<"value1_value0">>]
    ]],
    [<<"test1">>,[
      [PatternID1,<<"value0_value0">>],
      [PatternID2,<<"value0_value0">>]
    ]]
  ]}=Reduce3([LocalResult2]),

  %----------Grouping and sorting--------------
  {Map4,Reduce4}=ecomet_query:compile_map_reduce(get,Fields,[{group,[<<"alias1">>]},{order,[{<<".pattern">>,'DESC'}]}]),
  LocalResult4=Map4(RS),
  [
    {<<"test1">>,[
      {PatternID1,[[<<"value0_value0">>]]},
      {PatternID2,[[<<"value0_value0">>]]}
    ]},
    {<<"test101">>,[
      {PatternID1,[[<<"value1_value0">>]]},
      {PatternID2,[[<<"value1_value0">>]]}
    ]},
    {<<"test201">>,[
      {PatternID1,[[<<"value2_value0">>]]},
      {PatternID2,[[<<"value2_value0">>]]}
    ]},
    {<<"test301">>,[
      {PatternID1,[[<<"value3_value0">>]]},
      {PatternID2,[[<<"value3_value0">>]]}
    ]},
    {<<"test401">>,[
      {PatternID1,[[<<"value4_value0">>]]},
      {PatternID2,[[<<"value4_value0">>]]}
    ]},
    {<<"test501">>,[
      {PatternID1,[[<<"value5_value1">>]]},
      {PatternID2,[[<<"value5_value1">>]]}
    ]},
    {<<"test601">>,[
      {PatternID1,[[<<"value6_value1">>]]},
      {PatternID2,[[<<"value6_value1">>]]}
    ]},
    {<<"test701">>,[
      {PatternID1,[[<<"value7_value1">>]]},
      {PatternID2,[[<<"value7_value1">>]]}
    ]},
    {<<"test801">>,[
      {PatternID1,[[<<"value8_value1">>]]},
      {PatternID2,[[<<"value8_value1">>]]}
    ]},
    {<<"test901">>,[
      {PatternID1,[[<<"value9_value1">>]]},
      {PatternID2,[[<<"value9_value1">>]]}
    ]}
  ]=LocalResult4,

  {[<<"alias1">>,<<".pattern">>,<<"alias2">>],[
    [<<"test1">>,[
      [PatternID2,<<"value0_value0">>],
      [PatternID1,<<"value0_value0">>]
    ]],
    [<<"test101">>,[
      [PatternID2,<<"value1_value0">>],
      [PatternID1,<<"value1_value0">>]
    ]],
    [<<"test201">>,[
      [PatternID2,<<"value2_value0">>],
      [PatternID1,<<"value2_value0">>]
    ]],
    [<<"test301">>,[
      [PatternID2,<<"value3_value0">>],
      [PatternID1,<<"value3_value0">>]
    ]],
    [<<"test401">>,[
      [PatternID2,<<"value4_value0">>],
      [PatternID1,<<"value4_value0">>]
    ]],
    [<<"test501">>,[
      [PatternID2,<<"value5_value1">>],
      [PatternID1,<<"value5_value1">>]
    ]],
    [<<"test601">>,[
      [PatternID2,<<"value6_value1">>],
      [PatternID1,<<"value6_value1">>]
    ]],
    [<<"test701">>,[
      [PatternID2,<<"value7_value1">>],
      [PatternID1,<<"value7_value1">>]
    ]],
    [<<"test801">>,[
      [PatternID2,<<"value8_value1">>],
      [PatternID1,<<"value8_value1">>]
    ]],
    [<<"test901">>,[
      [PatternID2,<<"value9_value1">>],
      [PatternID1,<<"value9_value1">>]
    ]]
  ]}=Reduce4([LocalResult4]),

  %%-----------2 Level grouping-------------------------------
  {Map5,Reduce5}=ecomet_query:compile_map_reduce(get,Fields,[{group,[<<"alias2">>,<<".pattern">>]},{order,[{<<".pattern">>,'DESC'}]}]),
  LocalResult5=Map5(RS),
  [
    {<<"value0_value0">>,[
      {PatternID1,[[<<"test1">>]]},
      {PatternID2,[[<<"test1">>]]}
    ]},
    {<<"value1_value0">>,[
      {PatternID1,[[<<"test101">>]]},
      {PatternID2,[[<<"test101">>]]}
    ]},
    {<<"value2_value0">>,[
      {PatternID1,[[<<"test201">>]]},
      {PatternID2,[[<<"test201">>]]}
    ]},
    {<<"value3_value0">>,[
      {PatternID1,[[<<"test301">>]]},
      {PatternID2,[[<<"test301">>]]}
    ]},
    {<<"value4_value0">>,[
      {PatternID1,[[<<"test401">>]]},
      {PatternID2,[[<<"test401">>]]}
    ]},
    {<<"value5_value1">>,[
      {PatternID1,[[<<"test501">>]]},
      {PatternID2,[[<<"test501">>]]}
    ]},
    {<<"value6_value1">>,[
      {PatternID1,[[<<"test601">>]]},
      {PatternID2,[[<<"test601">>]]}
    ]},
    {<<"value7_value1">>,[
      {PatternID1,[[<<"test701">>]]},
      {PatternID2,[[<<"test701">>]]}
    ]},
    {<<"value8_value1">>,[
      {PatternID1,[[<<"test801">>]]},
      {PatternID2,[[<<"test801">>]]}
    ]},
    {<<"value9_value1">>,[
      {PatternID1,[[<<"test901">>]]},
      {PatternID2,[[<<"test901">>]]}
    ]}
  ]=LocalResult5,

  {[<<"alias2">>,<<".pattern">>,<<"alias1">>],[
    [<<"value0_value0">>,[
      [PatternID2,[[<<"test1">>]]],
      [PatternID1,[[<<"test1">>]]]
    ]],
    [<<"value1_value0">>,[
      [PatternID2,[[<<"test101">>]]],
      [PatternID1,[[<<"test101">>]]]
    ]],
    [<<"value2_value0">>,[
      [PatternID2,[[<<"test201">>]]],
      [PatternID1,[[<<"test201">>]]]
    ]],
    [<<"value3_value0">>,[
      [PatternID2,[[<<"test301">>]]],
      [PatternID1,[[<<"test301">>]]]
    ]],
    [<<"value4_value0">>,[
      [PatternID2,[[<<"test401">>]]],
      [PatternID1,[[<<"test401">>]]]
    ]],
    [<<"value5_value1">>,[
      [PatternID2,[[<<"test501">>]]],
      [PatternID1,[[<<"test501">>]]]
    ]],
    [<<"value6_value1">>,[
      [PatternID2,[[<<"test601">>]]],
      [PatternID1,[[<<"test601">>]]]
    ]],
    [<<"value7_value1">>,[
      [PatternID2,[[<<"test701">>]]],
      [PatternID1,[[<<"test701">>]]]
    ]],
    [<<"value8_value1">>,[
      [PatternID2,[[<<"test801">>]]],
      [PatternID1,[[<<"test801">>]]]
    ]],
    [<<"value9_value1">>,[
      [PatternID2,[[<<"test901">>]]],
      [PatternID1,[[<<"test901">>]]]
    ]]
  ]}=Reduce5([LocalResult5]),

  %%-----------2 Level sorting-------------------------------
  {Map6,Reduce6}=ecomet_query:compile_map_reduce(get,Fields,[{order,[{<<"alias2">>,'ASC'},{<<".pattern">>,'DESC'}]}]),
  LocalResult5=Map6(RS),

  {[<<".pattern">>,<<"alias1">>,<<"alias2">>],[
    [PatternID2,<<"test1">>,<<"value0_value0">>],
    [PatternID1,<<"test1">>,<<"value0_value0">>],
    [PatternID2,<<"test101">>,<<"value1_value0">>],
    [PatternID1,<<"test101">>,<<"value1_value0">>],
    [PatternID2,<<"test201">>,<<"value2_value0">>],
    [PatternID1,<<"test201">>,<<"value2_value0">>],
    [PatternID2,<<"test301">>,<<"value3_value0">>],
    [PatternID1,<<"test301">>,<<"value3_value0">>],
    [PatternID2,<<"test401">>,<<"value4_value0">>],
    [PatternID1,<<"test401">>,<<"value4_value0">>],
    [PatternID2,<<"test501">>,<<"value5_value1">>],
    [PatternID1,<<"test501">>,<<"value5_value1">>],
    [PatternID2,<<"test601">>,<<"value6_value1">>],
    [PatternID1,<<"test601">>,<<"value6_value1">>],
    [PatternID2,<<"test701">>,<<"value7_value1">>],
    [PatternID1,<<"test701">>,<<"value7_value1">>],
    [PatternID2,<<"test801">>,<<"value8_value1">>],
    [PatternID1,<<"test801">>,<<"value8_value1">>],
    [PatternID2,<<"test901">>,<<"value9_value1">>],
    [PatternID1,<<"test901">>,<<"value9_value1">>]
  ]}=Reduce6([LocalResult5]).

grouping_sorting_page(Config)->
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),
  ecomet_user:on_init_state(),
  ConcatFun=
    fun([V1,V2])-><<V1/binary,"_",V2/binary>> end,
  Fields=[
    <<".pattern">>,
    {<<"alias1">>,<<".name">>},
    {<<"alias2">>,{ConcatFun,[<<"string2">>,<<"string3">>]}}
  ],
  RS=ecomet_query:get([root],rs,{<<"string1">>,'=',<<"value1">>}),

  {Map1,Reduce1}=ecomet_query:compile_map_reduce(get,Fields,[
    {group,[<<"alias2">>,<<".pattern">>]},
    {order,[{<<".pattern">>,'DESC'}]},
    {page,{2,3}}
  ]),
  % Local result reads only fields needed to group and sort results, only oid is inserted as row,
  % Query manager reads other fields only for objects within requested page
  LocalResult1=Map1(RS),
  [
    {<<"value0_value0">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]},
    {<<"value1_value0">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]},
    {<<"value2_value0">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]},
    {<<"value3_value0">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]},
    {<<"value4_value0">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]},
    {<<"value5_value1">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]},
    {<<"value6_value1">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]},
    {<<"value7_value1">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]},
    {<<"value8_value1">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]},
    {<<"value9_value1">>,[
      {PatternID1,[{_,_}]},
      {PatternID2,[{_,_}]}
    ]}
  ]=LocalResult1,

  {10,Result1}=Reduce1([LocalResult1]),
  {[<<"alias2">>,<<".pattern">>,<<"alias1">>],[
    [<<"value3_value0">>,[
      [PatternID2,[[<<"test301">>]]],
      [PatternID1,[[<<"test301">>]]]
    ]],
    [<<"value4_value0">>,[
      [PatternID2,[[<<"test401">>]]],
      [PatternID1,[[<<"test401">>]]]
    ]],
    [<<"value5_value1">>,[
      [PatternID2,[[<<"test501">>]]],
      [PatternID1,[[<<"test501">>]]]
    ]]
  ]}=Result1,

  {Map2,Reduce2}=ecomet_query:compile_map_reduce(get,Fields,[
    {order,[{<<"alias2">>,'ASC'},{<<".pattern">>,'DESC'}]},
    {page,{3,4}}
  ]),
  LocalResult1=Map2(RS),

  {20,Result2}=Reduce2([LocalResult1]),
  {[<<".pattern">>,<<"alias1">>,<<"alias2">>],[
    [PatternID2,<<"test401">>,<<"value4_value0">>],
    [PatternID1,<<"test401">>,<<"value4_value0">>],
    [PatternID2,<<"test501">>,<<"value5_value1">>],
    [PatternID1,<<"test501">>,<<"value5_value1">>]
  ]}=Result2.


aggregate(_Config)->
  Fields=[
    {<<"count1">>,count},
    {<<"max1">>,{max,<<"integer">>}},
    {<<"min1">>,{min,<<"integer">>}},
    {<<"sum1">>,{sum,<<"float">>}}
  ],
  ecomet_user:on_init_state(),
  RS=ecomet_query:get([root],rs,{<<"bool">>,'=',true}),

  %-----No grouping------------------------
  {Map1,Reduce1}=ecomet_query:compile_map_reduce(get,Fields,[]),
  LocalResult1=Map1(RS),
  [1000,99,1,_Sum]=LocalResult1,

  {[<<"count1">>,<<"max1">>,<<"min1">>,<<"sum1">>],LocalResult1}=Reduce1([LocalResult1]),

  %-----Grouping---------------------------
  {Map2,Reduce2}=ecomet_query:compile_map_reduce(get,[<<"string2">>|Fields],[
    {group,[<<"string2">>]},
    {order,[{<<"string2">>,'DESC'}]}
  ]),
  LocalResult2=Map2(RS),
  [
    {<<"value0">>,[100,99,1,_]},
    {<<"value1">>,[100,99,1,_]},
    {<<"value2">>,[100,99,1,_]},
    {<<"value3">>,[100,99,1,_]},
    {<<"value4">>,[100,99,1,_]},
    {<<"value5">>,[100,99,1,_]},
    {<<"value6">>,[100,99,1,_]},
    {<<"value7">>,[100,99,1,_]},
    {<<"value8">>,[100,99,1,_]},
    {<<"value9">>,[100,99,1,_]}
  ]=LocalResult2,

  {[<<"string2">>,<<"count1">>,<<"max1">>,<<"min1">>,<<"sum1">>],Result2}=Reduce2([LocalResult2]),
  [
    [<<"value9">>,100,99,1,_],
    [<<"value8">>,100,99,1,_],
    [<<"value7">>,100,99,1,_],
    [<<"value6">>,100,99,1,_],
    [<<"value5">>,100,99,1,_],
    [<<"value4">>,100,99,1,_],
    [<<"value3">>,100,99,1,_],
    [<<"value2">>,100,99,1,_],
    [<<"value1">>,100,99,1,_],
    [<<"value0">>,100,99,1,_]
  ]=Result2,

  %-------2 level grouping---------------
  {Map3,Reduce3}=ecomet_query:compile_map_reduce(get,[<<"string2">>,<<"string3">>|Fields],[
    {group,[<<"string3">>,<<"string2">>]},
    {order,[{<<"string2">>,'DESC'}]}
  ]),
  LocalResult3=Map3(RS),
  [
    {<<"value0">>,[
      {<<"value0">>,[100,99,1,_]},
      {<<"value1">>,[100,99,1,_]},
      {<<"value2">>,[100,99,1,_]},
      {<<"value3">>,[100,99,1,_]},
      {<<"value4">>,[100,99,1,_]}
    ]},
    {<<"value1">>,[
      {<<"value5">>,[100,99,1,_]},
      {<<"value6">>,[100,99,1,_]},
      {<<"value7">>,[100,99,1,_]},
      {<<"value8">>,[100,99,1,_]},
      {<<"value9">>,[100,99,1,_]}
    ]}
  ]=LocalResult3,

  {[<<"string3">>,<<"string2">>,<<"count1">>,<<"max1">>,<<"min1">>,<<"sum1">>],Result3}=Reduce3([LocalResult3]),
  [
    [<<"value0">>,[
      [<<"value4">>,100,99,1,_],
      [<<"value3">>,100,99,1,_],
      [<<"value2">>,100,99,1,_],
      [<<"value1">>,100,99,1,_],
      [<<"value0">>,100,99,1,_]
    ]],
    [<<"value1">>,[
      [<<"value9">>,100,99,1,_],
      [<<"value8">>,100,99,1,_],
      [<<"value7">>,100,99,1,_],
      [<<"value6">>,100,99,1,_],
      [<<"value5">>,100,99,1,_]
    ]]
  ]=Result3,

  %-------Order by aggregated fields---------------
  {Map4,Reduce4}=ecomet_query:compile_map_reduce(get,[<<"string2">>,<<"string3">>|Fields],[
    {group,[<<"string3">>,<<"string2">>]},
    {order,[{<<"sum1">>,'DESC'}]}
  ]),
  LocalResult3=Map4(RS),
  {[<<"string3">>,<<"string2">>,<<"count1">>,<<"max1">>,<<"min1">>,<<"sum1">>],Result4}=Reduce4([LocalResult3]),

  % field "float" arises to end
  [
    [<<"value0">>,[
      [<<"value4">>,100,99,1,_],
      [<<"value3">>,100,99,1,_],
      [<<"value2">>,100,99,1,_],
      [<<"value1">>,100,99,1,_],
      [<<"value0">>,100,99,1,_]
    ]],
    [<<"value1">>,[
      [<<"value9">>,100,99,1,_],
      [<<"value8">>,100,99,1,_],
      [<<"value7">>,100,99,1,_],
      [<<"value6">>,100,99,1,_],
      [<<"value5">>,100,99,1,_]
    ]]
  ]=Result4,

  %-------Paging---------------
  {Map5,Reduce5}=ecomet_query:compile_map_reduce(get,[<<"string2">>|Fields],[
    {group,[<<"string2">>]},
    {order,[{<<"sum1">>,'DESC'}]},
    {page,{3,3}}
  ]),
  LocalResult5=Map5(RS),
  [
    {<<"value0">>,[100,99,1,_]},
    {<<"value1">>,[100,99,1,_]},
    {<<"value2">>,[100,99,1,_]},
    {<<"value3">>,[100,99,1,_]},
    {<<"value4">>,[100,99,1,_]},
    {<<"value5">>,[100,99,1,_]},
    {<<"value6">>,[100,99,1,_]},
    {<<"value7">>,[100,99,1,_]},
    {<<"value8">>,[100,99,1,_]},
    {<<"value9">>,[100,99,1,_]}
  ]=LocalResult5,

  {10,Result5}=Reduce5([LocalResult5]),
  {[<<"string2">>,<<"count1">>,<<"max1">>,<<"min1">>,<<"sum1">>],[
    [<<"value3">>,100,99,1,_],
    [<<"value2">>,100,99,1,_],
    [<<"value1">>,100,99,1,_]
  ]}=Result5.

query_get(Config)->
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),
  ecomet_user:on_init_state(),
  ConcatFun=
    fun([V1,V2])-><<V1/binary,"_",V2/binary>> end,
  Fields=[
    <<".pattern">>,
    {<<"alias1">>,<<".name">>},
    {<<"alias2">>,{ConcatFun,[<<"string2">>,<<"string3">>]}}
  ],

  %--------No params------------------------
  {Header1,Rows1}=ecomet_query:get([root],Fields,{<<"string1">>,'=',<<"value1">>}),
  [<<".pattern">>,<<"alias1">>,<<"alias2">>]=Header1,
  [
    [PatternID1,<<"test1">>,<<"value0_value0">>],
    [PatternID1,<<"test101">>,<<"value1_value0">>],
    [PatternID1,<<"test201">>,<<"value2_value0">>],
    [PatternID1,<<"test301">>,<<"value3_value0">>],
    [PatternID1,<<"test401">>,<<"value4_value0">>],
    [PatternID1,<<"test501">>,<<"value5_value1">>],
    [PatternID1,<<"test601">>,<<"value6_value1">>],
    [PatternID1,<<"test701">>,<<"value7_value1">>],
    [PatternID1,<<"test801">>,<<"value8_value1">>],
    [PatternID1,<<"test901">>,<<"value9_value1">>],
    [PatternID2,<<"test1">>,<<"value0_value0">>],
    [PatternID2,<<"test101">>,<<"value1_value0">>],
    [PatternID2,<<"test201">>,<<"value2_value0">>],
    [PatternID2,<<"test301">>,<<"value3_value0">>],
    [PatternID2,<<"test401">>,<<"value4_value0">>],
    [PatternID2,<<"test501">>,<<"value5_value1">>],
    [PatternID2,<<"test601">>,<<"value6_value1">>],
    [PatternID2,<<"test701">>,<<"value7_value1">>],
    [PatternID2,<<"test801">>,<<"value8_value1">>],
    [PatternID2,<<"test901">>,<<"value9_value1">>]
  ]=Rows1,

  %---------Paging------------------------------------
  {20,{Header2,Rows2}}=ecomet_query:get([root],Fields,{<<"string1">>,'=',<<"value1">>},[
    {page,{2,5}}
  ]),
  [<<".pattern">>,<<"alias1">>,<<"alias2">>]=Header2,
  [
    [PatternID1,<<"test501">>,<<"value5_value1">>],
    [PatternID1,<<"test601">>,<<"value6_value1">>],
    [PatternID1,<<"test701">>,<<"value7_value1">>],
    [PatternID1,<<"test801">>,<<"value8_value1">>],
    [PatternID1,<<"test901">>,<<"value9_value1">>]
  ]=Rows2,

  %------Grouping, sorting, paging---------------------
  {10,{Header3,Rows3}}=ecomet_query:get([root],Fields,{<<"string1">>,'=',<<"value1">>},[
    {group,[<<"alias2">>,<<".pattern">>]},
    {order,[{<<".pattern">>,'DESC'}]},
    {page,{2,3}}
  ]),
  [<<"alias2">>,<<".pattern">>,<<"alias1">>]=Header3,
  [
    [<<"value3_value0">>,[
      [PatternID2,[[<<"test301">>]]],
      [PatternID1,[[<<"test301">>]]]
    ]],
    [<<"value4_value0">>,[
      [PatternID2,[[<<"test401">>]]],
      [PatternID1,[[<<"test401">>]]]
    ]],
    [<<"value5_value1">>,[
      [PatternID2,[[<<"test501">>]]],
      [PatternID1,[[<<"test501">>]]]
    ]]
  ]=Rows3,

  %----------Aggregated query-----------------------------
  AggregatedFields=[
    {<<"count1">>,count},
    {<<"max1">>,{max,<<"integer">>}},
    {<<"min1">>,{min,<<"integer">>}},
    {<<"sum1">>,{sum,<<"float">>}}
  ],

  {Header4,Rows4}=ecomet_query:get([root],[<<"string2">>,<<"string3">>|AggregatedFields],{<<"bool">>,'=',true},[
    {group,[<<"string3">>,<<"string2">>]},
    {order,[{<<"sum1">>,'DESC'}]}
  ]),
  [<<"string3">>,<<"string2">>,<<"count1">>,<<"max1">>,<<"min1">>,<<"sum1">>]=Header4,

  [
    [<<"value0">>,[
      [<<"value4">>,100,99,1,_],
      [<<"value3">>,100,99,1,_],
      [<<"value2">>,100,99,1,_],
      [<<"value1">>,100,99,1,_],
      [<<"value0">>,100,99,1,_]
    ]],
    [<<"value1">>,[
      [<<"value9">>,100,99,1,_],
      [<<"value8">>,100,99,1,_],
      [<<"value7">>,100,99,1,_],
      [<<"value6">>,100,99,1,_],
      [<<"value5">>,100,99,1,_]
    ]]
  ]=Rows4,
  ok.

set(_Config)->
  ecomet_user:on_init_state(),
  % Before update
  {_,[Count,SumBefore]}=ecomet_query:get([root],[count,{sum,<<"integer">>}],{<<"string1">>,'=',<<"value1">>}),

  % Update
  SetFields=#{
    <<"atom">> => test_set_value,
    <<"integer">> => {fun([V])->V+5 end,[<<"integer">>]}
  },
  Count=ecomet_query:set([root],SetFields,{<<"string1">>,'=',<<"value1">>}),

  % Check after update
  SumAfter=SumBefore+Count*5,
  {_,[Count,SumAfter]}=ecomet_query:get([root],[count,{sum,<<"integer">>}],{<<"string1">>,'=',<<"value1">>}),
  {[<<"atom">>],Rows}=ecomet_query:get([root],[<<"atom">>],{<<"string1">>,'=',<<"value1">>}),
  Count=length(Rows),
  [test_set_value]=ordsets:union(Rows),

  % Return
  Count=ecomet_query:set([root],#{<<"integer">> => {fun([V])->V-5 end,[<<"integer">>]}},{<<"string1">>,'=',<<"value1">>}),
  {_,[SumBefore]}=ecomet_query:get([root],[{sum,<<"integer">>}],{<<"string1">>,'=',<<"value1">>}).

parse_get(_Config) ->

  [{get, [{<<"alias">>, <<"foo">>}],[root], {'AND', [{<<"buz">>, ':>', 123}]}, []}]=
    ecomet_query:parse("get foo AS 'alias' from root where AND ( buz :> 123 )"),

  [{get, [<<"foo1">>, <<"foo2">>], [db1,db2],{'OR', [{<<"buz">>, ':LIKE', <<"f">>}, {<<"bar">>, '=', -42}]}, []}]=
    ecomet_query:parse("get foo1, foo2 from db1,db2 where OR ( buz :LIKE 'f', bar = -42 )"),

  [{get, [<<"foo">>],['TEST'], {'ANDNOT', {<<"buz">>, ':=', 45.5},{<<"bar">>, ':<', 4}}, [{order, [{<<"foo3">>, 'DESC'}]}]}]=
    ecomet_query:parse("get foo from 'TEST' where ANDNOT (buz := 45.5, bar :< 4) order by foo3 DESC"),

  [{get, [<<"foo">>], [root],{'AND', [{<<"buz">>, ':=', 1}]}, [{page, {5, 10}}]}]=
    ecomet_query:parse("get foo from root where AND (buz := 1) page 5:10"),

  [{get, [<<"foo">>], [root],{'AND', [{<<"bar">>, ':<', 4}]}, [{page, {5, 10}},{lock, read}]}]=
    ecomet_query:parse("get foo from * where AND (bar :< 4) page 5:10 lock read"),

  [{get, [<<"foo1">>],[d1,d2,'D3'],{'AND', [{<<"bar">>, ':<', 4}]}, [{group, [<<"foo2">>, <<"buz2">>]}, {order, [{<<"foo3">>, 'ASC'}, {<<"buz3">>,'DESC'}]}]}]=
    ecomet_query:parse("get foo1 from d1,d2,'D3' where AND (bar :< 4) group by foo2, buz2 order by foo3, buz3 DESC"),

  [{get, [<<"foo1">>],[root], {'AND', [{<<"bar">>, ':<', 4}]}, [{group, [1]},{order, [{2, 'DESC'}]}]}]=
    ecomet_query:parse("get foo1 from root where AND (bar :< 4) group by 1 order by 2 DESC"),

  [{get,[{Concat, [<<"buz">>,<<"foo">>]}, {<<"fun">>, {test}}],[root], {'AND', [{<<"bar">>, ':<', 4}]}, []}]=
    ecomet_query:parse("get $concat($buz,$foo), $term('{test}') AS 'fun' from root where AND (bar :< 4)"),

  <<"str1str2">> = Concat([<<"str1">>,<<"str2">>]),

  ok.

parse_set(_Config) ->
  [{set, #{<<"foo">>:={ FieldValue,[<<"hello">>] }},[root], {'AND', [{<<"bar">>, ':<', 1}]}, [{lock, write}]}]=
    ecomet_query:parse("set foo=$hello in root where AND (bar :< 1) lock write"),
  <<"value">>=FieldValue([<<"value">>]),

  [{set,#{<<"foo">>:={Concat,[<<"buz">>]}},[root], {'OR',[{<<"bar">>,':>',1}]}, [{lock,read}]}]=
    ecomet_query:parse("set foo=$concat($buz,'_test') in * where OR (bar :> 1) lock read"),
  <<"value_test">> = Concat([<<"value">>]),

  [{set, #{<<"foo1">>:= 123, <<"foo2">>:= {Term,[<<"buz">>]} },[db1,db2], {'AND', [{<<"bar">>, ':LIKE', <<"1">>}]}, []}]=
    ecomet_query:parse("set foo1=123, foo2=$term(['{test}',$buz]) in db1,db2 where AND (bar :LIKE '1')"),

  [{test},value] = Term([value]),

  ok.

parse_insert(_Config) ->
  [{insert, #{<<"foo1">>:=123, <<"foo2">>:=buz}}]=
    ecomet_query:parse("insert foo1=123, foo2=$term('buz')"),

  ok.

parse_delete(_Config) ->

  [{delete,[root], {'AND', [{<<"bar">>, ':<', 1}]}, [{lock, write}]}]=
    ecomet_query:parse("delete from root where AND (bar :< 1) lock write"),

  [{delete,[root], {'OR', [{<<"bar">>, ':>', 1.0}]}, [{lock, read}]}]=
    ecomet_query:parse("delete from * where OR (bar :> 1.0) lock read"),

  [{delete,[db1,db2], {'ANDNOT', {<<"bar">>, ':LIKE', <<"1">>}, {<<"buz">>, ':=', foo}}, []}]=
    ecomet_query:parse("delete from db1,db2 where ANDNOT (bar :LIKE '1', buz := foo)"),

  ok.