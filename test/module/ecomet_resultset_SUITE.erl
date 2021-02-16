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
-module(ecomet_resultset_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet_test.hrl").

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

%% Prepare group
-export([
  define_patterns/1,
  simple_tag/1,
  constructions/1,
  simple_norm/1,
  andnot_norm/1,
  direct_norm/1,
  optimize/1,
  prepare_summary/1
]).



%% Search group
-export([
  search_patterns/1,
  search_idhs/1,
  search_simple/1,
  execute_remote/1
]).



all()->
  [
    {group,prepare},
    {group,search}
  ].

groups()->[
  {prepare,
    [sequence],
    [
      define_patterns,
      {build_conditions,
        [sequence],
        [
          simple_tag,
          constructions
        ]
      },
      {normalize,
        [sequence],
        [
          simple_norm,
          andnot_norm,
          direct_norm
        ]
      },
      optimize,
      prepare_summary
    ]
  },
  {search,
    [sequence],
    [
      search_patterns,
      search_idhs,
      search_simple,
      execute_remote
    ]
  }
].

init_per_suite(Config)->
  ?BACKEND_INIT(),

  % Build patterns
  ecomet_user:on_init_state(),

  % Build patterns
  FolderOID={2,1000},
  PatternID1={3,1001},
  PatternID2={3,1002},
  PatternID3={3,1003},
  PatternID4={3,1004},
  PatternID5={3,1005},

  BaseObj = ecomet_pattern:get_fields({?PATTERN_PATTERN,?OBJECT_PATTERN}),

  %--------Pattern1-------------------------------------------
  Pattern1Map = BaseObj#{
    <<"field1">> => ecomet_field:build_description(#{ type=>string, storage=>ram }),
    <<"field2">> => ecomet_field:build_description(#{ type=>string, storage=>ramdisc }),
    <<"field3">> => ecomet_field:build_description(#{ type=>string, storage=>ramdisc }),
    <<"field4">> => ecomet_field:build_description(#{ type=>string, storage=>disc })
  },
  ecomet_schema:register_type(PatternID1, Pattern1Map),

  %--------Pattern2-------------------------------------------
  Pattern2Map = BaseObj#{
    <<"field1">> => ecomet_field:build_description(#{ type=>string, storage=>ram }),
    <<"field2">> => ecomet_field:build_description(#{ type=>string, storage=>disc })
  },
  ecomet_schema:register_type(PatternID2, Pattern2Map),

  %--------Pattern3-------------------------------------------
  Pattern3Map = BaseObj#{
    <<"field1">> => ecomet_field:build_description(#{ type=>string, storage=>ram }),
    <<"field3">> => ecomet_field:build_description(#{ type=>string, storage=>disc })
  },
  ecomet_schema:register_type(PatternID3, Pattern3Map),

  %--------Pattern4----------------------------PatternID2---------------
  Pattern4Map = Pattern1Map#{
    <<"string1">> => ecomet_field:build_description(#{ type=>string, index => [simple,'3gram'],storage=>ram }),
    <<"string2">> => ecomet_field:build_description(#{ type=>string, index => [simple],storage=>ramdisc }),
    <<"string3">> => ecomet_field:build_description(#{ type=>string, index => [simple], storage=>disc }),
    <<"integer">> => ecomet_field:build_description(#{ type=>integer, index => [simple],storage=>ram }),
    <<"float">> => ecomet_field:build_description(#{ type=>float, index => [simple],storage=>ramdisc }),
    <<"atom">> => ecomet_field:build_description(#{ type=>atom, index => [simple], storage=>disc }),
    <<"bool">> => ecomet_field:build_description(#{ type=>bool, index => [simple],storage=>ram })
  },
  ecomet_schema:register_type(PatternID4, Pattern4Map),

  %--------Pattern5-------------------------------------------
  Pattern5Map = Pattern2Map#{
    <<"string1">> => ecomet_field:build_description(#{ type=>string, index => [simple,'3gram'],storage=>ramdisc }),
    <<"string2">> => ecomet_field:build_description(#{ type=>string, index => [simple], storage=>disc }),
    <<"string3">> => ecomet_field:build_description(#{ type=>string, index => [simple],storage=>ram }),
    <<"integer">> => ecomet_field:build_description(#{ type=>integer, index => [simple],storage=>ramdisc }),
    <<"float">> => ecomet_field:build_description(#{ type=>float, index => [simple], storage=>disc }),
    <<"atom">> => ecomet_field:build_description(#{ type=>atom, index => [simple],storage=>ram }),
    <<"bool">> => ecomet_field:build_description(#{ type=>bool, index => [simple],storage=>ramdisc })
  },
  ecomet_schema:register_type(PatternID5, Pattern5Map),
  [
    {folder_id,FolderOID},
    {pattern_id1,PatternID1},
    {pattern_id2,PatternID2},
    {pattern_id3,PatternID3},
    {pattern_id4,PatternID4},
    {pattern_id5,PatternID5}
    |Config].

end_per_suite(Config)->
  ecomet_user:on_init_state(),
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),
  PatternID3=?config(pattern_id3,Config),
  PatternID4=?config(pattern_id4,Config),
  PatternID5=?config(pattern_id5,Config),
  ecomet_query:delete([root],{'OR',[
    {<<".pattern">>,':=',PatternID1},
    {<<".pattern">>,':=',PatternID2},
    {<<".pattern">>,':=',PatternID3},
    {<<".pattern">>,':=',PatternID4},
    {<<".pattern">>,':=',PatternID5}
  ]}),
  ?BACKEND_STOP(30000),
  ok.

init_per_group(search,Config)->
  ecomet_user:on_init_state(),
  FolderOID1={2,1001},
  FolderOID2={2,1002},
  PatternID4=?config(pattern_id4, Config),
  PatternID5=?config(pattern_id5, Config),

  %-----Create objects-------------------------------------------------------
  ObjectCount=1000,
  lists:foreach(fun(I)->
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
    % Pattern 1
    ecomet:create_object(#{
      <<".name">> => <<"test", IB/binary>>,
      <<".folder">> => FolderOID1,
      <<".pattern">> => PatternID4,
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
      <<".pattern">> => PatternID5,
      <<".ts">>=>ecomet_lib:log_ts(),
      <<"string1">> => String1,
      <<"string2">> => String2,
      <<"string3">> => String3,
      <<"integer">> => Integer,
      <<"float">> => Float,
      <<"atom">> => Atom,
      <<"bool">> => Bool
    })
  end,lists:seq(1,ObjectCount)),
  Config;
init_per_group(_,Config)->
  Config.

end_per_group(search,Config)->
  ecomet_user:on_init_state(),

  PatternID4=?config(pattern_id4,Config),
  PatternID5=?config(pattern_id5,Config),
  Query=ecomet_resultset:prepare({'OR',[
    {<<".pattern">>,':=',PatternID4},
    {<<".pattern">>,':=',PatternID5}
  ]}),
  Delete=
    fun(RS)->
      ecomet_resultset:foldl(fun(OID,Acc)->
        Object=ecomet_object:open(OID,none),
        ecomet_object:delete(Object),
        Acc+1
      end,0,RS)
    end,
  Result=ecomet_resultset:execute_local(root,Query,Delete,{'OR',ecomet_resultset:new()}),
  ct:pal("deleted - ~p",[Result]),
  ok;

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

define_patterns(_Config)->
  PatternID1={3,1000},
  PatternID2={3,1001},
  Defined=ecomet_resultset:define_patterns({'OR',[
    {'AND',[
      {<<"field1">>,'=',<<"value1">>},
      {<<".pattern">>,':=',PatternID1}
    ]},
    {'AND',[
      {<<"field1">>,'=',<<"value1">>},
      {<<"field2">>,'=',<<"value2">>}
    ]},
    {'ANDNOT',
      {<<".pattern">>,':=',PatternID2},
      {<<"field1">>,':>',<<"value1">>}
    }
  ]}),
  % Can not limit patterns for root OR
  {'OR',ORList}=Defined,
  [AND1,AND2,ANDNOT]=ORList,
  {'AND',AND1List}=AND1,

  [AND1C1,AND1C2]=AND1List,
  {'LEAF',{'=',<<"field1">>,<<"value1">>}}=AND1C1,
  {'LEAF',{'=',<<".pattern">>,PatternID1}}=AND1C2,

  {'AND',AND2List}=AND2,
  [AND2C1,AND2C2]=AND2List,
  {'LEAF',{'=',<<"field1">>,<<"value1">>}}=AND2C1,
  {'LEAF',{'=',<<"field2">>,<<"value2">>}}=AND2C2,

  {'ANDNOT',{ANDNOT1,ANDNOT2}}=ANDNOT,
  {'LEAF',{'=',<<".pattern">>,PatternID2}}=ANDNOT1,
  {'LEAF',{':>',<<"field1">>,<<"value1">>}}=ANDNOT2.

simple_tag(Config)->
  % Can not define storages, because patterns are not defined
  {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{'=',<<"field1">>,<<"value1">>}}),
  PatternID1=?config(pattern_id1,Config),


  % Field5 is not defined in pattern1 - no storages, nothing can be found
  {'TAG',{<<"field5">>,<<"value5">>,simple},'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{'=',<<"field5">>,<<"value5">>}}),
  % Invalid tag
  ?assertError({invalid_operation,'DUMMY'},ecomet_resultset:build_conditions({'LEAF',{'DUMMY',<<"field4">>,<<"value4">>}})),
  % Field4 defined only in pattern1 as disc
  {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{'=',<<"field4">>,<<"value4">>}}),
  % Field defined in both patterns in the same storage
  {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{'=',<<"field1">>,<<"value1">>}}),
  % Field defined in both patterns in different storages
  {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{'=',<<"field2">>,<<"value2">>}}),

  % Patterns defined by tag itself
  {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{'=',<<".pattern">>,PatternID1}}),
  % Patterns do not intersect, no sense to search tag
  {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{'=',<<".pattern">>,PatternID1}}),
  % Patterns have intersection
  T1={'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{'=',<<".pattern">>,PatternID1}}),
  false = ecomet_resultset:has_direct(T1),

  D1 = {'DIRECT',{':>',<<"field1">>,<<"value1">>},'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{':>',<<"field1">>,<<"value1">>}}),
  true = ecomet_resultset:has_direct(D1),

  AND1 = {'AND',LIKEList,'UNDEFINED'}=ecomet_resultset:build_conditions({'LEAF',{'LIKE',<<"field1">>,<<"value1">>}}),
  false = ecomet_resultset:has_direct(AND1),

  % String "value1" is splitted to:
  % [<<"_va">>,<<"val">>,<<"alu">>,<<"lue">>,<<"ue1">>,<<"e1_">>]
  % we drop first and last 3grams, because we are not linked to start and end of the phrase
  [
    {'TAG',{<<"field1">>,<<"VAL">>,'3gram'},'UNDEFINED'},
    {'TAG',{<<"field1">>,<<"ALU">>,'3gram'},'UNDEFINED'},
    {'TAG',{<<"field1">>,<<"LUE">>,'3gram'},'UNDEFINED'},
    {'TAG',{<<"field1">>,<<"UE1">>,'3gram'},'UNDEFINED'}
  ]=lists:reverse(LIKEList),
  % too short value
  LIKE1 = {'DIRECT',{':LIKE',<<"field1">>,<<"va">>},'UNDEFINED'} =ecomet_resultset:build_conditions({'LEAF',{'LIKE',<<"field1">>,<<"va">>}}),
  true = ecomet_resultset:has_direct(LIKE1),

  ok.


constructions(Config)->
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),

  % Patterns that actual for AND are actual for all it's branches
  AND1 = {'AND',AND1Tags,'UNDEFINED'}=ecomet_resultset:build_conditions({'AND',[
    {'LEAF',{'=',<<".pattern">>,PatternID1}},
    {'LEAF',{'=',<<"field1">>,<<"value1">>}}
  ]}),
  false = ecomet_resultset:has_direct(AND1),
  [
    {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'},
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ]=AND1Tags,

  % Patterns that defined for AND are intersected with external patterns
  AND2={'AND',AND1Tags,'UNDEFINED'}=ecomet_resultset:build_conditions({'AND',[
    {'LEAF',{'=',<<".pattern">>,PatternID1}},
    {'LEAF',{'=',<<"field1">>,<<"value1">>}}
  ]}),
  false = ecomet_resultset:has_direct(AND2),
  % Patterns are not defined for OR
  OR1={'OR',OR1Tags,'UNDEFINED'}=ecomet_resultset:build_conditions({'OR',[
    {'LEAF',{'=',<<".pattern">>,PatternID1}},
    {'LEAF',{'=',<<"field1">>,<<"value1">>}}
  ]}),
  false = ecomet_resultset:has_direct(OR1),
  [
    {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'},
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ]=OR1Tags,
  % External patterns are defined for OR
  OR2={'OR',OR2Tags,'UNDEFINED'}=ecomet_resultset:build_conditions({'OR',[
    {'LEAF',{'=',<<".pattern">>,PatternID1}},
    {'LEAF',{'=',<<"field1">>,<<"value1">>}}
  ]}),
  false = ecomet_resultset:has_direct(OR2),
  [
    {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'},
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ]=OR2Tags,
  % ANDNOT can be true only for patterns defined for condition1
  ANDNOT1={'ANDNOT',{C1,C2},'UNDEFINED'}=ecomet_resultset:build_conditions({'ANDNOT',{
    {'LEAF',{'=',<<".pattern">>,PatternID1}},
    {'LEAF',{'=',<<".pattern">>,PatternID2}}
  }}),
  false = ecomet_resultset:has_direct(ANDNOT1),
  {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'}=C1,
  {'TAG',{<<".pattern">>,PatternID2,simple},'UNDEFINED'}=C2,
  % Direct
  AND3 = {'AND',AND2Tags,'UNDEFINED'}=ecomet_resultset:build_conditions({'AND',[
    {'LEAF',{'=',<<".pattern">>,PatternID1}},
    {'OR',[
      {'LEAF',{'=',<<"field1">>,<<"value1">>}},
      {'LEAF',{':>',<<"field1">>,<<"value1">>}}
    ]}
  ]}),
  true = ecomet_resultset:has_direct(AND3),


  [AND2T1,AND2T2]=AND2Tags,
  {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'}=AND2T1,
  {'OR',AND2OR2Tags,'UNDEFINED'}=AND2T2,
  [
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'DIRECT',{':>',<<"field1">>,<<"value1">>},'UNDEFINED'}
  ]=AND2OR2Tags.

simple_norm(_Config)->
  % Simple tag
  {'OR',[{'NORM',{{AND1,ANDNOT1},{[],[]}},'UNDEFINED'}],'UNDEFINED'}=ecomet_resultset:normalize({'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}),
  {'AND',[{'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}],'UNDEFINED'}=AND1,
  {'OR',[],'UNDEFINED'}=ANDNOT1,
  % OR branch
  {'OR',S2NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'OR',[
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
      {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
    ],'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S2AND1,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'},
    {'NORM',{{S2AND2,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'}
  ]=S2NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S2AND1,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S2AND2,
  % AND(OR(T1,T2),OR(T3,T4))
  {'OR',S3NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'AND',[
    {'OR',[
      {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
    ],'UNDEFINED'},
    {'OR',[
      {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
      {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
    ],'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S3AND1,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'},
    {'NORM',{{S3AND2,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'},
    {'NORM',{{S3AND3,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'},
    {'NORM',{{S3AND4,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'}
  ]=S3NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S3AND1,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S3AND2,
  {'AND',[
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S3AND3,
  {'AND',[
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
    {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S3AND4,
  % OR(AND(T1,T2),AND(T3,T4))
  {'OR',S4NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'OR',[
    {'AND',[
      {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
    ],'UNDEFINED'},
    {'AND',[
      {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
      {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
    ],'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S4AND1,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'},
    {'NORM',{{S4AND2,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'}
  ]=S4NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S4AND1,
  {'AND',[
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
    {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S4AND2,
  % AND(T1,OR(T2,AND(T3,T4)))
  {'OR',S5NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'OR',[
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
      {'AND',[
        {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
        {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
      ],'UNDEFINED'}
    ],'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S5AND1,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'},
    {'NORM',{{S5AND2,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'}
  ]=S5NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5AND1,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
    {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5AND2,
  % AND(T1,AND(T2,OR(T3,T4)))
  {'OR',S6NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'AND',[
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
      {'OR',[
        {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
        {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
      ],'UNDEFINED'}
    ],'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S6AND1,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'},
    {'NORM',{{S6AND2,{'OR',[],'UNDEFINED'}},{[],[]}},'UNDEFINED'}
  ]=S6NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S6AND1,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
    {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S6AND2.

andnot_norm(_Config)->
  % ANDNOT(AND(T1,T2),T3)
  {'OR',S1NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'ANDNOT',{
    {'AND',[
      {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
    ],'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  },'UNDEFINED'}),
  [
    {'NORM',{{S1AND1,S1ANDNOT1},{[],[]}},'UNDEFINED'}
  ]=S1NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S1AND1,
  {'OR',[
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S1ANDNOT1,
  % AND(T1,ANDNOT(T2,T3))
  {'OR',S2NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'ANDNOT',{
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
      {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
    },'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S2AND1,S2ANDNOT1},{[],[]}},'UNDEFINED'}
  ]=S2NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S2AND1,
  {'OR',[
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S2ANDNOT1,
  % AND(T1,OR(AND(T2,T3),ANDNOT(T4,T5)))
  {'OR',S3NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'OR',[
      {'AND',[
        {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
        {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
      ],'UNDEFINED'},
      {'ANDNOT',{
        {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'},
        {'TAG',{<<"field5">>,<<"value5">>,simple},'UNDEFINED'}
      },'UNDEFINED'}
    ],'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S3AND1,S3ANDNOT1},{[],[]}},'UNDEFINED'},
    {'NORM',{{S3AND2,S3ANDNOT2},{[],[]}},'UNDEFINED'}
  ]=S3NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S3AND1,
  {'OR',[],'UNDEFINED'}=S3ANDNOT1,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S3AND2,
  {'OR',[
    {'TAG',{<<"field5">>,<<"value5">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S3ANDNOT2,
  % ANDNOT(T1,ANDNOT(T2,T3))
  {'OR',S4NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'ANDNOT',{
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'ANDNOT',{
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
      {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
    },'UNDEFINED'}
  },'UNDEFINED'}),
  [
    {'NORM',{{S4AND1,S4ANDNOT1},{[],[]}},'UNDEFINED'},
    {'NORM',{{S4AND2,S4ANDNOT2},{[],[]}},'UNDEFINED'}
  ]=S4NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S4AND1,
  {'OR',[
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S4ANDNOT1,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S4AND2,
  {'OR',[],'UNDEFINED'}=S4ANDNOT2,
  % ANDNOT(OR(T1,T2),ANDNOT(T3,T4))
  {'OR',S5NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'ANDNOT',{
    {'OR',[
      {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
    ],'UNDEFINED'},
    {'ANDNOT',{
      {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
      {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
    },'UNDEFINED'}
  },'UNDEFINED'}),
  [
    {'NORM',{{S5AND1,S5ANDNOT1},{[],[]}},'UNDEFINED'},
    {'NORM',{{S5AND2,S5ANDNOT2},{[],[]}},'UNDEFINED'},
    {'NORM',{{S5AND3,S5ANDNOT3},{[],[]}},'UNDEFINED'},
    {'NORM',{{S5AND4,S5ANDNOT4},{[],[]}},'UNDEFINED'}
  ]=S5NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5AND1,
  {'OR',[
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5ANDNOT1,
  {'AND',[
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5AND2,
  {'OR',[
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5ANDNOT2,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
    {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5AND3,
  {'OR',[],'UNDEFINED'}=S5ANDNOT3,
  {'AND',[
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
    {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5AND4,
  {'OR',[],'UNDEFINED'}=S5ANDNOT4.

direct_norm(_Config)->
  % AND(T1,D2)
  {'OR',S1NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'DIRECT',{':>',<<"field2">>,<<"value2">>},'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S1AND1,{'OR',[],'UNDEFINED'}},{S1DAND1,[]}},'UNDEFINED'}
  ]=S1NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S1AND1,
  [{'DIRECT',{':>',<<"field2">>,<<"value2">>},'UNDEFINED'}]=S1DAND1,
  % AND(T1,OR(T2,D3))
  {'OR',S2NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'OR',[
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
      {'DIRECT',{':>',<<"field3">>,<<"value3">>},'UNDEFINED'}
    ],'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S2AND1,{'OR',[],'UNDEFINED'}},{S2DAND1,[]}},'UNDEFINED'},
    {'NORM',{{S2AND2,{'OR',[],'UNDEFINED'}},{S2DAND2,[]}},'UNDEFINED'}
  ]=S2NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S2AND1,
  []=S2DAND1,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S2AND2,
  [{'DIRECT',{':>',<<"field3">>,<<"value3">>},'UNDEFINED'}]=S2DAND2,
  % AND(D1,D2)
  ?assertError(no_base_conditions,ecomet_resultset:normalize({'AND',[
    {'DIRECT',{':>',<<"field1">>,<<"value1">>},'UNDEFINED'},
    {'DIRECT',{':>',<<"field2">>,<<"value2">>},'UNDEFINED'}
  ],'UNDEFINED'})),
  % AND(T1,ANDNOT(D2,D3))
  {'OR',S3NORMList,'UNDEFINED'}=ecomet_resultset:normalize({'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'ANDNOT',{
      {'DIRECT',{':>',<<"field2">>,<<"value2">>},'UNDEFINED'},
      {'DIRECT',{':>',<<"field3">>,<<"value3">>},'UNDEFINED'}
    },'UNDEFINED'}
  ],'UNDEFINED'}),
  [
    {'NORM',{{S3AND1,{'OR',[],'UNDEFINED'}},{S3DAND1,S3DANDNOT1}},'UNDEFINED'}
  ]=S3NORMList,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S3AND1,
  [{'DIRECT',{':>',<<"field2">>,<<"value2">>},'UNDEFINED'}]=S3DAND1,
  [{'DIRECT',{':>',<<"field3">>,<<"value3">>},'UNDEFINED'}]=S3DANDNOT1.

optimize(_Config)->
  S1={'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'},
  S1=ecomet_resultset:optimize(S1),
  S2={'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'},
  {'AND',S2XAND,'UNDEFINED'}=ecomet_resultset:optimize(S2),
  [{'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},S2]=S2XAND,
  % No intersection
  S3={'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'},
  S3=ecomet_resultset:optimize(S3),
  % T1 intersection
  S4={'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'},
  {'AND',S4XAND,'UNDEFINED'}=ecomet_resultset:optimize(S4),
  [{'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},S4Clear]=S4XAND,
  {'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'}=S4Clear,
  % T2 intersecton
  S5={'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'},
  {'AND',S5XAND,'UNDEFINED'}=ecomet_resultset:optimize(S5),
  [{'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},S5Clear]=S5XAND,
  {'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'}=S5Clear,
  % 3 'NORM', no intersection
  S6={'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'},
  S6=ecomet_resultset:optimize(S6),
  % 3 'NORM' T1,T2 intersection
  S7={'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'},
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
          {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'},
  {'AND',S7XAND,'UNDEFINED'}=ecomet_resultset:optimize(S7),
  [
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'},
    S7Clear
  ]=S7XAND,
  {'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'}=S7Clear,
  % 2 'NORM' intersection AND + ANDNOT
  S8={'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'},
  {'AND',S8XAND,'UNDEFINED'}=ecomet_resultset:optimize(S8),
  [{'ANDNOT',{S8XANDAND,S8XANDANDNOT},'UNDEFINED'},S8Clear]=S8XAND,
  {'AND',[{'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}],'UNDEFINED'}=S8XANDAND,
  {'OR',[{'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}],'UNDEFINED'}=S8XANDANDNOT,
  {'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'}=S8Clear,
  % 2 'NORM' only ANDNOT intersection
  S9={'OR',[
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
          {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'},
    {'NORM',{
      {
        {'AND',[
          {'TAG',{<<"field4">>,<<"value4">>,simple},'UNDEFINED'},
          {'TAG',{<<"field5">>,<<"value5">>,simple},'UNDEFINED'}
        ],'UNDEFINED'},
        {'OR',[
          {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
        ],'UNDEFINED'}
      },
      {[],[]}
    },'UNDEFINED'}
  ],'UNDEFINED'},
  S9=ecomet_resultset:optimize(S9).

prepare_summary(Config)->
  PatternID1=?config(pattern_id1,Config),
  PatternID2=?config(pattern_id2,Config),

  % Simple tag
  {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}=ecomet_resultset:prepare({<<"field1">>,'=',<<"value1">>}),
  % Pattern tag
  {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'}=ecomet_resultset:prepare({<<".pattern">>,':=',PatternID1}),
  ?assertError(no_base_conditions,ecomet_resultset:prepare({<<"field1">>,':=',<<"value1">>})),
  % Patterns are not defined
  {'AND',S4ANDList,'UNDEFINED'}=ecomet_resultset:prepare({'AND',[
    {'OR',[
      {<<".pattern">>,':=',PatternID1},
      {<<"field1">>,'=',<<"value1">>}
    ]},
    {'OR',[
      {<<".pattern">>,':=',PatternID2},
      {<<"field2">>,'=',<<"value2">>}
    ]}
  ]}),
  [
    {'OR',[
      {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'},
      {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
    ],'UNDEFINED'},
    {'OR',[
      {'TAG',{<<".pattern">>,PatternID2,simple},'UNDEFINED'},
      {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
    ],'UNDEFINED'}
  ]=S4ANDList,
  % Patterns are defined
  {'OR',[S5AND1,S5AND2],'UNDEFINED'}=ecomet_resultset:prepare({'OR',[
    {'AND',[
      {<<".pattern">>,':=',PatternID1},
      {<<"field1">>,'=',<<"value1">>}
    ]},
    {'AND',[
      {<<".pattern">>,':=',PatternID2},
      {<<"field2">>,'=',<<"value2">>}
    ]}
  ]}),
  {'AND',[
    {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'},
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5AND1,
  {'AND',[
    {'TAG',{<<".pattern">>,PatternID2,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S5AND2,

  % AND2 contains no base conditions and can not be attached to any
  ?assertError(no_base_conditions,ecomet_resultset:prepare({'OR',[
    {'AND',[
      {<<".pattern">>,':=',PatternID1},
      {<<"field1">>,'=',<<"value1">>}
    ]},
    {'AND',[
      {<<"field2">>,':=',<<"value2">>},
      {<<"field3">>,':>',<<"value3">>}
    ]}
  ]})),
  % Patterns spread to branch
  {'AND',[S6AND1,S6AND2],'UNDEFINED'}=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {'ANDNOT',
      {<<"field1">>,'=',<<"value1">>},
      {<<"field2">>,'=',<<"value2">>}
    }
  ]}),
  {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'}=S6AND1,
  {'ANDNOT',{
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'TAG',{<<"field2">>,<<"value2">>,simple},'UNDEFINED'}
  },'UNDEFINED'}=S6AND2,
  % Direct conditions needs query normalization
  {'OR',[S7Norm1],'UNDEFINED'}=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"field2">>,':>',<<"value2">>}
  ]}),
  {'NORM',{{S7N1TAND,S7N1TANDNOT},{S7N1DAND,[]}},'UNDEFINED'}=S7Norm1,
  {'AND',[
    {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S7N1TAND,
  {'OR',[],'UNDEFINED'}=S7N1TANDNOT,
  [{'DIRECT',{':>',<<"field2">>,<<"value2">>},'UNDEFINED'}]=S7N1DAND,
  % OR with direct
  {'OR',[S8Norm1,S8Norm2],'UNDEFINED'}=ecomet_resultset:prepare({'OR',[
    {<<"field1">>,'=',<<"value1">>},
    {'AND',[
      {<<".pattern">>,':=',PatternID1},
      {<<"field3">>,':>',<<"value3">>}
    ]}
  ]}),
  {'NORM',{{S8N1TAND,S8N1TANDNOT},{[],[]}},'UNDEFINED'}=S8Norm1,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S8N1TAND,
  {'OR',[],'UNDEFINED'}=S8N1TANDNOT,
  {'NORM',{{S8N2TAND,S8N2TANDNOT},{S8N2DAND,[]}},'UNDEFINED'}=S8Norm2,
  {'AND',[
    {'TAG',{<<".pattern">>,PatternID1,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S8N2TAND,
  {'OR',[],'UNDEFINED'}=S8N2TANDNOT,
  [{'DIRECT',{':>',<<"field3">>,<<"value3">>},'UNDEFINED'}]=S8N2DAND,
  % ANDNOT with DIRECT
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'},
    {'OR',[S9Norm1,S9Norm2],'UNDEFINED'}
  ],'UNDEFINED'}=ecomet_resultset:prepare({'ANDNOT',
    {'AND',[
      {<<"field1">>,'=',<<"value1">>},
      {<<"field2">>,':<',<<"value2">>}
    ]},
    {'AND',[
      {<<"field3">>,'=',<<"value3">>},
      {<<"field4">>,':>',<<"value4">>}
    ]}
  }),
  {'NORM',{{S9N1TAND,S9N1TANDNOT},{S9N1DAND,S9N1DANDNOT}},'UNDEFINED'}=S9Norm1,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S9N1TAND,
  {'OR',[
    {'TAG',{<<"field3">>,<<"value3">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S9N1TANDNOT,
  [{'DIRECT',{':<',<<"field2">>,<<"value2">>},'UNDEFINED'}]=S9N1DAND,
  []=S9N1DANDNOT,
  {'NORM',{{S9N2TAND,S9N2TANDNOT},{S9N2DAND,S9N2DANDNOT}},'UNDEFINED'}=S9Norm2,
  {'AND',[
    {'TAG',{<<"field1">>,<<"value1">>,simple},'UNDEFINED'}
  ],'UNDEFINED'}=S9N2TAND,
  {'OR',[],'UNDEFINED'}=S9N2TANDNOT,
  [{'DIRECT',{':<',<<"field2">>,<<"value2">>},'UNDEFINED'}]=S9N2DAND,
  [{'DIRECT',{':>',<<"field4">>,<<"value4">>},'UNDEFINED'}]=S9N2DANDNOT.

%%================================================================================
%%    SEARCH
%%================================================================================
search_patterns(Config)->
  PatternID1=?config(pattern_id4,Config),
  PatternID2=?config(pattern_id5,Config),
  IDP1=ecomet_object:get_id(PatternID1),
  IDP2=ecomet_object:get_id(PatternID2),
  Ext1=ecomet_bitmap:set_bit(none,IDP1),
  Ext2=ecomet_bitmap:set_bit(none,IDP2),
  Ext12=ecomet_bitmap:set_bit(Ext1,IDP2),

  % S1. Single tag, pattern is defined
  S1Query=ecomet_resultset:prepare({<<".pattern">>,':=',PatternID1}),
  S1Res=ecomet_resultset:search_patterns(S1Query,root,'UNDEFINED'),
  {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[]}]}}=S1Res,
  {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[]}]}}=ecomet_resultset:search_patterns(S1Query,root,Ext12),
  {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[]}]}}=ecomet_resultset:search_patterns(S1Query,root,Ext2),

  % S2. Single tag, pattern is not defined
  S2Query=ecomet_resultset:prepare({<<"string1">>,'=',<<"value1">>}),
  S2Res=ecomet_resultset:search_patterns(S2Query,root,'UNDEFINED'),
  {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext12,[{ramdisc,Ext2,[]},{ram,Ext1,[]}]}}=S2Res,
  {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext2,[{ramdisc,Ext2,[]}]}}=ecomet_resultset:search_patterns(S2Query,root,Ext2),

  % S3. AND, pattern is defined
  S3Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,'=',<<"value1">>}
  ]}),
  {'AND',[
    {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[]}]}},
    {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext12,[{ramdisc,Ext2,[]},{ram,Ext1,[]}]}}
  ],{Ext1,[]}}=ecomet_resultset:search_patterns(S3Query,root,'UNDEFINED'),

  % S3A. Empty AND
  S3AQuery=ecomet_resultset:prepare({'AND',[
    {<<"string1">>,'=',<<"value1">>},
    {<<"string1">>,'=',<<"empty">>}
  ]}),
  {'AND',[
    {'TAG',{<<"string1">>,<<"value1">>,simple},{none,[]}},
    {'TAG',{<<"string1">>,<<"empty">>,simple},{none,[]}}
  ],{none,[]}}=ecomet_resultset:search_patterns(S3AQuery,root,'UNDEFINED'),


  % S4. OR, pattern not defined
  S4Query=ecomet_resultset:prepare({'OR',[
    {<<"string1">>,'=',<<"value1">>},
    {<<"string2">>,'=',<<"value2">>}
  ]}),
  {'OR',[
    {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext12,[{ramdisc,Ext2,[]},{ram,Ext1,[]}]}},
    {'TAG',{<<"string2">>,<<"value2">>,simple},{Ext12,[{disc,Ext2,[]},{ramdisc,Ext1,[]}]}}
  ],{Ext12,[]}}=ecomet_resultset:search_patterns(S4Query,root,'UNDEFINED'),
  {'OR',[
    {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext1,[{ram,Ext1,[]}]}},
    {'TAG',{<<"string2">>,<<"value2">>,simple},{Ext1,[{ramdisc,Ext1,[]}]}}
  ],{Ext1,[]}}=ecomet_resultset:search_patterns(S4Query,root,Ext1),

  % S5. ANDNOT, pattern not defined
  S5Query=ecomet_resultset:prepare({'ANDNOT',
    {'OR',[
      {<<".pattern">>,':=',PatternID1},
      {<<"string1">>,'=',<<"value1">>}
    ]},
    {<<"string2">>,'=',<<"value2">>}
  }),
  {'ANDNOT',{
    {'OR',[
      {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[]}]}},
      {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext12,[{ramdisc,Ext2,[]},{ram,Ext1,[]}]}}
    ],{Ext12,[]}},
    {'TAG',{<<"string2">>,<<"value2">>,simple},{Ext12,[{disc,Ext2,[]},{ramdisc,Ext1,[]}]}}
  },{Ext12,[]}}=ecomet_resultset:search_patterns(S5Query,root,'UNDEFINED'),

  % S6. ANDNOT is actual only for patterns in condition1
  S6Query=ecomet_resultset:prepare({'ANDNOT',
    {<<".pattern">>,':=',PatternID1},
    {'OR',[
      {<<"string1">>,'=',<<"value1">>},
      {<<"string2">>,'=',<<"value2">>}
    ]}
  }),
  {'ANDNOT',{
    {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[]}]}},
    {'OR',[
      {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext1,[{ram,Ext1,[]}]}},
      {'TAG',{<<"string2">>,<<"value2">>,simple},{Ext1,[{ramdisc,Ext1,[]}]}}
    ],{Ext1,[]}}
  },{Ext1,[]}}=ecomet_resultset:search_patterns(S6Query,root,'UNDEFINED'),

  % S7. OR on AND + ANDNOT
  S7Query=ecomet_resultset:prepare({'OR',[
    {'AND',[
      {<<".pattern">>,':=',PatternID1},
      {<<"string1">>,'=',<<"value1">>}
    ]},
    {'ANDNOT',
      {<<"string2">>,'=',<<"value2">>},
      {<<"string1">>,'=',<<"value2">>}
    }
  ]}),
  {'OR',[
    {'AND',[
      {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[]}]}},
      {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext12,[{ramdisc,Ext2,[]},{ram,Ext1,[]}]}}
    ],{Ext1,[]}},
    {'ANDNOT',{
      {'TAG',{<<"string2">>,<<"value2">>,simple},{Ext12,[{disc,Ext2,[]},{ramdisc,Ext1,[]}]}},
      {'TAG',{<<"string1">>,<<"value2">>,simple},{Ext12,[{ramdisc,Ext2,[]},{ram,Ext1,[]}]}}
    },{Ext12,[]}}
  ],{Ext12,[]}}=ecomet_resultset:search_patterns(S7Query,root,'UNDEFINED'),

  % S8. ANDNOT with directs
  S8Query=ecomet_resultset:prepare({'ANDNOT',
    {'AND',[
      {<<".pattern">>,':=',PatternID1},
      {<<"string1">>,':>',<<"value1">>}
    ]},
    {'OR',[
      {<<"string2">>,'=',<<"value2">>},
      {<<"string3">>,':<',<<"value2">>}
    ]}
  }),
  S8PLevel=ecomet_resultset:search_patterns(S8Query,root,'UNDEFINED'),
  {'OR',[
    {'NORM',{{S8N1AND,S8N1ANDNOT},{S8N1DAND,S8N1DANDNOT}},{Ext1,[]}}
  ],{Ext1,[]}}=S8PLevel,
  {'AND',[
    {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[]}]}}
  ],{Ext1,[]}}=S8N1AND,
  {'OR',[
    {'TAG',{<<"string2">>,<<"value2">>,simple},{Ext1,[{ramdisc,Ext1,[]}]}}
  ],{Ext1,[]}}=S8N1ANDNOT,
  [{'DIRECT',{':>',<<"string1">>,<<"value1">>},'UNDEFINED'}]=S8N1DAND,
  [{'DIRECT',{':<',<<"string3">>,<<"value2">>},'UNDEFINED'}]=S8N1DANDNOT.

search_idhs(Config)->
  PatternID1=?config(pattern_id4,Config),
  PatternID2=?config(pattern_id5,Config),
  IDP1=ecomet_object:get_id(PatternID1),
  IDP2=ecomet_object:get_id(PatternID2),
  Ext1=ecomet_bitmap:set_bit(none,IDP1),
  Ext2=ecomet_bitmap:set_bit(none,IDP2),
  Ext12=ecomet_bitmap:set_bit(Ext1,IDP2),

  NodeID = ecomet_node:get_unique_id(),
  DB = ecomet_schema:get_db_id( root ),
  IDH1_0 = ((0 bsl 16) + NodeID) bsl 8 + DB,
  IDH1 = ecomet_bitmap:set_bit(none,IDH1_0),

  % S1. tag on pattern
  S1Query=ecomet_resultset:prepare({<<".pattern">>,':=',PatternID1}),
  S1Query1 = ecomet_resultset:search_patterns(S1Query,root,'UNDEFINED'),
  {IDH1,S1P1TAG}=ecomet_resultset:seacrh_idhs(S1Query1,root,IDP1),
  {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[
    {IDP1,IDH1}
  ]}]}}=S1P1TAG,
  {none,S1P2TAG}=ecomet_resultset:seacrh_idhs(S1Query1,root,IDP2),
  {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[{disc,Ext1,[]}]}}=S1P2TAG,

  %S2. Simple tag
  S2Query=ecomet_resultset:prepare({<<"string1">>,'=',<<"value1">>}),
  S2PLevel=ecomet_resultset:search_patterns(S2Query,root,'UNDEFINED'),
  {IDH1,S2P1TAG}=ecomet_resultset:seacrh_idhs(S2PLevel,root,IDP1),
  {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext12,[
    {ram,Ext1,[{IDP1,IDH1}]},
    {ramdisc,Ext2,[]}
  ]}}=S2P1TAG,
  {IDH1,S2P2TAG}=ecomet_resultset:seacrh_idhs(S2P1TAG,root,IDP2),
  {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext12,[
    {ramdisc,Ext2,[{IDP2,IDH1}]},
    {ram,Ext1,[{IDP1,IDH1}]}
  ]}}=S2P2TAG,

  % S3. AND with pattern
  S3Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,'=',<<"value1">>}
  ]}),
  S3Query1 = ecomet_resultset:search_patterns(S3Query,root,'UNDEFINED'),
  {IDH1,S3P1COND}=ecomet_resultset:seacrh_idhs(S3Query1,root,IDP1),
  {'AND',[
    {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[
      {disc,Ext1,[{IDP1,IDH1}]}
    ]}},
    {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext12,[
      {ram,Ext1,[{IDP1,IDH1}]},
      {ramdisc,Ext2,[]}
    ]}}
  ],{Ext1,[{IDP1,IDH1}]}}=S3P1COND,
  {none,S3P1COND}=ecomet_resultset:seacrh_idhs(S3P1COND,root,IDP2),

  % S4. AND with direct
  S4Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,':>',<<"value1">>}
  ]}),
  S4Query1 = ecomet_resultset:search_patterns(S4Query,root,'UNDEFINED'),
  % Pattern is defined, search idhs
  {IDH1,S4P1COND}=ecomet_resultset:seacrh_idhs(S4Query1,root,IDP1),
  {'OR',[S4Norm1],{Ext1,[{IDP1,IDH1}]}}=S4P1COND,
  {'NORM',{
    {
      {'AND',[
        {'TAG',{<<".pattern">>,PatternID1,simple},{Ext1,[
          {disc,Ext1,[{IDP1,IDH1}]}
        ]}}
      ],{Ext1,[{IDP1,IDH1}]}},
      {'OR',[],{none,[]}}
    },
    {
      [{'DIRECT',{':>',<<"string1">>,<<"value1">>},'UNDEFINED'}],
      []
    }
  },{Ext1,[{IDP1,IDH1}]}}=S4Norm1,

  % S5. ANDNOT with direct
  S5Query=ecomet_resultset:prepare({'ANDNOT',
    {<<"string1">>,'=',<<"value1">>},
    {<<"string2">>,':>',<<"value2">>}
  }),
  S5Plevel=ecomet_resultset:search_patterns(S5Query,root,'UNDEFINED'),
  {IDH1,S5P1COND}=ecomet_resultset:seacrh_idhs(S5Plevel,root,IDP1),
  {'OR',[S5Norm1],{Ext12,[{IDP1,IDH1}]}}=S5P1COND,
  {'NORM',{
    {
      {'AND',[
        {'TAG',{<<"string1">>,<<"value1">>,simple},{Ext12,[
          {ram,Ext1,[{IDP1,IDH1}]},
          {ramdisc,Ext2,[]}
        ]}}
      ],{Ext12,[{IDP1,IDH1}]}},
      {'OR',[],{none,[]}}
    },
    {
      [],
      [{'DIRECT',{':>',<<"string2">>,<<"value2">>},'UNDEFINED'}]
    }
  },{Ext12,[{IDP1,IDH1}]}}=S5Norm1.

search_simple(Config)->
  PatternID1=?config(pattern_id4,Config),
  PatternID2=?config(pattern_id5,Config),

%%  1000 objects
%%  IB=integer_to_binary(I*ProcessCount*J),
%%  S1=integer_to_binary(I rem 100),
%%  String1= <<"value",S1/binary>>,
%%  S2= integer_to_binary(I div 100),
%%  String2= <<"value",S2/binary>>,
%%  S3=integer_to_binary(I div 500),
%%  String3= <<"value",S3/binary>>,
%%  Integer= I rem 100,
%%  Float= I/7,
%%  Atom=list_to_atom(binary_to_list(String3)),
%%  Bool=((I rem 2)==1),

  % S1. Simple tag
  S1Query=ecomet_resultset:prepare({<<"string1">>,'=',<<"value1">>}),
  FunPatName=fun(RS)->
    ecomet_resultset:foldl(fun(OID,Acc)->
      Object=ecomet_object:construct(OID),
      {ok,Pattern}=ecomet_object:read_field(Object,<<".pattern">>),
      {ok,Name}=ecomet_object:read_field(Object,<<".name">>),
      [{Pattern,Name}|Acc]
    end,[],RS)
  end,
  S1Res=ecomet_resultset:execute_local(root,S1Query,FunPatName,{'OR',ecomet_resultset:new()}),

  20=length(S1Res),
  []=lists:subtract(S1Res,[
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
  ]),

  % S2. AND
  S2Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,'=',<<"value1">>}
  ]}),
  S2Res=ecomet_resultset:execute_local(root,S2Query,FunPatName,{'OR',ecomet_resultset:new()}),
  10=length(S2Res),
  []=lists:subtract(S2Res,[
    {PatternID1,<<"test1">>},
    {PatternID1,<<"test101">>},
    {PatternID1,<<"test201">>},
    {PatternID1,<<"test301">>},
    {PatternID1,<<"test401">>},
    {PatternID1,<<"test501">>},
    {PatternID1,<<"test601">>},
    {PatternID1,<<"test701">>},
    {PatternID1,<<"test801">>},
    {PatternID1,<<"test901">>}
  ]),

  % S3. AND
  S3Query=ecomet_resultset:prepare({'AND',[
    {<<"string1">>,'=',<<"value3">>},
    {<<"string2">>,'=',<<"value3">>}
  ]}),
  S3Res=ecomet_resultset:execute_local(root,S3Query,FunPatName,{'OR',ecomet_resultset:new()}),
  2=length(S3Res),
  []=lists:subtract(S3Res,[
    {PatternID1,<<"test303">>},
    {PatternID2,<<"test303">>}
  ]),

  % S4. AND with OR branch
  S4Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,'=',<<"value1">>},
    {'OR',[
      {<<"string2">>,'=',<<"value1">>},
      {<<"string3">>,'=',<<"value1">>}
    ]}
  ]}),
  FunName=
    fun(RS)->
      ecomet_resultset:foldl(fun(OID,Acc)->
        Object=ecomet_object:construct(OID),
        {ok,Name}=ecomet_object:read_field(Object,<<".name">>),
        [Name|Acc]
                             end,[],RS)
    end,
  S4Res=ecomet_resultset:execute_local(root,S4Query,FunName,{'OR',ecomet_resultset:new()}),
  6=length(S4Res),
  []=lists:subtract(S4Res,[<<"test101">>,<<"test501">>,<<"test601">>,<<"test701">>,<<"test801">>,<<"test901">>]),

  % S5. ANDNOT
  S5Query=ecomet_resultset:prepare({'ANDNOT',
    {'AND',[
      {<<".pattern">>,':=',PatternID2},
      {<<"string1">>,'=',<<"value5">>},
      {<<"string3">>,'=',<<"value0">>}
    ]},
    {'OR',[
      {<<"string2">>,'=',<<"value2">>},
      {<<"string2">>,'=',<<"value3">>}
    ]}
  }),
  S5Res=ecomet_resultset:execute_local(root,S5Query,FunName,{'OR',ecomet_resultset:new()}),
  3=length(S5Res),
  []=lists:subtract(S5Res,[<<"test5">>,<<"test105">>,<<"test405">>]),

  % S6. Direct
  S6Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,':=',<<"value1">>}
  ]}),
  S6Res=ecomet_resultset:execute_local(root,S6Query,FunPatName,{'OR',ecomet_resultset:new()}),
  10=length(S6Res),
  []=lists:subtract(S6Res,[
    {PatternID1,<<"test1">>},
    {PatternID1,<<"test101">>},
    {PatternID1,<<"test201">>},
    {PatternID1,<<"test301">>},
    {PatternID1,<<"test401">>},
    {PatternID1,<<"test501">>},
    {PatternID1,<<"test601">>},
    {PatternID1,<<"test701">>},
    {PatternID1,<<"test801">>},
    {PatternID1,<<"test901">>}
  ]),
  % S6_A. Direct not equal
  S6_AQuery=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,':=',<<"value1">>},
    {<<".name">>,':<>',<<"test1">>}
  ]}),
  S6_ARes=ecomet_resultset:execute_local(root,S6_AQuery,FunPatName,{'OR',ecomet_resultset:new()}),
  9=length(S6_ARes),
  []=lists:subtract(S6_ARes,[
    {PatternID1,<<"test101">>},
    {PatternID1,<<"test201">>},
    {PatternID1,<<"test301">>},
    {PatternID1,<<"test401">>},
    {PatternID1,<<"test501">>},
    {PatternID1,<<"test601">>},
    {PatternID1,<<"test701">>},
    {PatternID1,<<"test801">>},
    {PatternID1,<<"test901">>}
  ]),

  % S7, :>
  S7Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,':>',<<"value98">>}
  ]}),
  S7Res=ecomet_resultset:execute_local(root,S7Query,FunName,{'OR',ecomet_resultset:new()}),
  10=length(S7Res),
  []=lists:subtract(S7Res,[
    <<"test99">>,
    <<"test199">>,
    <<"test299">>,
    <<"test399">>,
    <<"test499">>,
    <<"test599">>,
    <<"test699">>,
    <<"test799">>,
    <<"test899">>,
    <<"test999">>
  ]),

  % S8, :>
  S8Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,':>=',<<"value98">>}
  ]}),
  S8Res=ecomet_resultset:execute_local(root,S8Query,FunName,{'OR',ecomet_resultset:new()}),
  20=length(S8Res),
  []=lists:subtract(S8Res,[
    <<"test98">>,<<"test99">>,
    <<"test198">>,<<"test199">>,
    <<"test298">>,<<"test299">>,
    <<"test398">>,<<"test399">>,
    <<"test498">>,<<"test499">>,
    <<"test598">>,<<"test599">>,
    <<"test698">>,<<"test699">>,
    <<"test798">>,<<"test799">>,
    <<"test898">>,<<"test899">>,
    <<"test998">>,<<"test999">>
  ]),

  % S9. ANDNOT with directs
  S9Query=ecomet_resultset:prepare({'ANDNOT',
    {'AND',[
      {<<".pattern">>,':=',PatternID1},
      {<<"string3">>,'=',<<"value1">>},
      {<<"string2">>,':<',<<"value6">>}
    ]},
    {'OR',[
      {<<"string1">>,':>',<<"value2">>},
      {<<"string1">>,':>=',<<"value10">>}
    ]}
  }),
  S9Res=ecomet_resultset:execute_local(root,S9Query,FunName,{'OR',ecomet_resultset:new()}),
  2=length(S9Res),
  % value2 > value10, so test502 is not here
  []=lists:subtract(S9Res,[<<"test500">>,<<"test501">>]),

  % S10 LIKE
  S10Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,'LIKE',<<"lue99">>}
  ]}),
  S10Res=ecomet_resultset:execute_local(root,S10Query,FunName,{'OR',ecomet_resultset:new()}),
  10=length(S10Res),
  []=lists:subtract(S10Res,[
    <<"test99">>,
    <<"test199">>,
    <<"test299">>,
    <<"test399">>,
    <<"test499">>,
    <<"test599">>,
    <<"test699">>,
    <<"test799">>,
    <<"test899">>,
    <<"test999">>
  ]),

  % S10_A. Direct LIKE
  S10_AQuery=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,':LIKE',<<"lue99">>}
  ]}),
  S10_ARes=ecomet_resultset:execute_local(root,S10_AQuery,FunName,{'OR',ecomet_resultset:new()}),
  10=length(S10_ARes),
  []=lists:subtract(S10_ARes,[
    <<"test99">>,
    <<"test199">>,
    <<"test299">>,
    <<"test399">>,
    <<"test499">>,
    <<"test599">>,
    <<"test699">>,
    <<"test799">>,
    <<"test899">>,
    <<"test999">>
  ]),

  % S11. OR on directs
  S11Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {'OR',[
      {'AND',[
        {<<"integer">>,':<',1},
        {<<"string2">>,':=<',<<"value2">>}
      ]},
      {'AND',[
        {<<"integer">>,':>=',99},
        {<<"string2">>,':>',<<"value8">>}
      ]}
    ]}
  ]}),
  S11Res=ecomet_resultset:execute_local(root,S11Query,FunName,{'OR',ecomet_resultset:new()}),
  4=length(S11Res),
  % test1000 is true for integer<1 and string2=<value2, because value10 < value2
  []=lists:subtract(S11Res,[<<"test100">>,<<"test200">>,<<"test999">>,<<"test1000">>]).

execute_remote(Config)->
  PatternID1=?config(pattern_id4,Config),

  FunPatName=fun(RS)->
    ecomet_resultset:foldl(fun(OID,Acc)->
      Object=ecomet_object:construct(OID),
      {ok,Pattern}=ecomet_object:read_field(Object,<<".pattern">>),
      {ok,Name}=ecomet_object:read_field(Object,<<".name">>),
      [{Pattern,Name}|Acc]
    end,[],RS)
  end,

  Query=ecomet_resultset:prepare({'AND',[
    {<<".pattern">>,':=',PatternID1},
    {<<"string1">>,'=',<<"value1">>}
  ]}),
  PID=ecomet_resultset:execute_remote([root],Query,FunPatName,{'OR',ecomet_resultset:new()}),
  [{root,Res}]=ecomet_resultset:wait_remote(PID,[]),

  10=length(Res),
  []=lists:subtract(Res,[
    {PatternID1,<<"test1">>},
    {PatternID1,<<"test101">>},
    {PatternID1,<<"test201">>},
    {PatternID1,<<"test301">>},
    {PatternID1,<<"test401">>},
    {PatternID1,<<"test501">>},
    {PatternID1,<<"test601">>},
    {PatternID1,<<"test701">>},
    {PatternID1,<<"test801">>},
    {PatternID1,<<"test901">>}
  ]).