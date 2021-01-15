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
-module(ecomet_common_SUITE).

-include("ecomet.hrl").
-include("ecomet_test.hrl").

-define(PROCESSES,5).
-define(OBJECTS,600000).
-define(STORAGE,disc).

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

%% Create group
-export([
  create/1
]).

%% Create group
-export([
  search1/1,
  search2/1,
  search3/1,
  search4/1,
  search5/1,
  search6/1
]).



all()->
  [
    {group,create}
    %{group,create_log_index}
    ,{group,search}
  ].

groups()->[
  {create,
    [parallel],
    [create||_<-lists:seq(1,?PROCESSES)]
  },
  {create_log_index,
    [parallel],
    [create||_<-lists:seq(1,?PROCESSES)]
  },

  {search,
    [sequence],
    [
      search1,
      search2,
      search3,
      search4,
      search5,
      search6
    ]
  }
].

-define(FIELD(Schema),maps:merge(ecomet_field:from_schema(Schema),#{<<".pattern">> => ?OID(<<"/root/.patterns/.field">>)})).

init_per_suite(Config)->

  ?BACKEND_INIT(),
  % Build patterns
  ecomet_user:on_init_state(),

  Tree = [
    { <<".patterns">>, #{
      children=>[
        { <<"test_pattern">>, #{
          fields=>#{
            <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
            <<"parent_pattern">>=>?OID(<<"/root/.patterns/.object">>)
          },
          children=>[
            { <<".name">>, #{ fields=>?FIELD(#{
              storage=>?STORAGE
            })}},
            { <<".folder">>, #{ fields=>?FIELD(#{
              storage=>?STORAGE
            })}},
            { <<".pattern">>, #{ fields=>?FIELD(#{
              storage=>?STORAGE
            })}},
            { <<".ts">>, #{ fields=>?FIELD(#{
              storage=>?STORAGE
            })}},
            { <<".readgroups">>, #{ fields=>?FIELD(#{
              storage=>?STORAGE
            })}},
            { <<".writegroups">>, #{ fields=>?FIELD(#{
              storage=>?STORAGE
            })}},
            { <<"field1">>, #{ fields=>?FIELD(#{
              type=>string, storage=>?STORAGE
            })}},
            { <<"field2">>, #{ fields=>?FIELD(#{
              type=>string, storage=>?STORAGE
            })}},
            { <<"field3">>, #{ fields=>?FIELD(#{
              type=>string, storage=>?STORAGE
            })}},
            { <<"field4">>, #{ fields=>?FIELD(#{
              type=>string, storage=>?STORAGE
            })}},
            { <<"string1">>, #{ fields=>?FIELD(#{
              type=>string, index => [simple,'3gram'],storage=>?STORAGE
            })}},
            { <<"string2">>, #{ fields=>?FIELD(#{
              type=>string, index => [simple],storage=>?STORAGE
            })}},
            { <<"string3">>, #{ fields=>?FIELD(#{
              type=>string, index => [simple], storage=>?STORAGE
            })}},
            { <<"integer">>, #{ fields=>?FIELD(#{
              type=>integer, index => [simple],storage=>?STORAGE
            })}},
            { <<"float">>, #{ fields=>?FIELD(#{
              type=>float, index => [simple],storage=>?STORAGE
            })}},
            { <<"atom">>, #{ fields=>?FIELD(#{
              type=>atom, index => [simple], storage=>?STORAGE
            })}},
            { <<"bool">>, #{ fields=>?FIELD(#{
              type=>bool, index => [simple],storage=>?STORAGE
            })}}
          ]
        }}
      ]
    }},
    { <<"test_folder">>, #{
      fields=>#{<<".pattern">>=> ?OID(<<"/root/.patterns/.folder">>)},
      children=>[]
    }}
  ],

  ecomet_schema:init_tree(?OID(<<"/root">>),Tree),

  [
    { folder, ?OID(<<"/root/test_folder">>) },
    { pattern , ?OID(<<"/root/.patterns/test_pattern">>) }
    |Config].

end_per_suite(_Config)->
  ?BACKEND_STOP(30000),
  ok.


init_per_group(create_log_index,Config)->
  meck:new(ecomet_index, [non_strict,no_link,passthrough]),
  meck:expect(ecomet_index, build_bitmap, fun(Oper,Tag,DB,Storage,PatternID,IDHN,IDLN)->
    %ok
    ecomet_backend:write(DB,?INDEX,Storage,{index_log,Tag,{PatternID,IDHN,IDLN}},Oper,none)
  end),

%%  meck:expect(ecomet_index, build_index, fun(_OID,_Map,_ChangedFields,BackTags)->
%%    {[],[],[],BackTags}
%%  end),

%%  meck:new(ecomet_backend, [non_strict,no_link,passthrough]),
%%  meck:expect(ecomet_backend, read, fun(DB,Storage,Type,Key,Lock)->
%%    ecomet_backend:dirty_read(DB,Storage,Type,Key,Lock)
%%  end),

  Config;

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

create(Config)->

  Pattern=?config(pattern, Config),
  Folder=?config(folder, Config),

  ecomet_user:on_init_state(),

  PID = integer_to_binary(erlang:unique_integer([positive])),

  ct:timetrap(12*3600*1000),

  %put(index_off,true),

  [begin
     if I rem 1000=:=0->ct:pal("create ~p",[I]); true->ok end,
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
       <<".name">> => <<"test",PID/binary,"_", IB/binary>>,
       <<".folder">> => Folder,
       <<".pattern">> => Pattern,
       <<"string1">> => String1,
       <<"string2">> => String2,
       <<"string3">> => String3,
       <<"integer">> => Integer,
       <<"float">> => Float,
       <<"atom">> => Atom,
       <<"bool">> => Bool
     })
   end||I<-lists:seq(1,?OBJECTS)],

  ok.

search1(Config)->
  ecomet_user:on_init_state(),
  Pattern=?config(pattern, Config),
  {_Header,Items} = ecomet_query:get([root],[<<".name">>,<<"integer">>],{'AND',[
    {<<".pattern">>,'=',Pattern},
    {'OR',[
      {<<"string1">>,'=',<<"value1">>},
      {<<"string1">>,'=',<<"value2">>}
    ]}
  ]}),
  ct:pal("result: ~p",[length(Items)]),
  ok.

search2(Config)->
  ecomet_user:on_init_state(),
  Pattern=?config(pattern, Config),
  Res = ecomet_query:get([root],[count],{'AND',[
    {<<".pattern">>,'=',Pattern},
    {'OR',[
      {<<"string1">>,'=',<<"value1">>},
      {<<"string1">>,'=',<<"value2">>}
    ]}
  ]}),
  ct:pal("result: ~p",[Res]),
  ok.

search3(Config)->
  ecomet_user:on_init_state(),
  Pattern=?config(pattern, Config),
  Res = ecomet_query:get([root],[<<".oid">>],{'AND',[
    {<<".pattern">>,'=',Pattern},
    {'OR',[
      {<<"string1">>,'=',<<"value1">>},
      {<<"string1">>,'=',<<"value2">>}
    ]}
  ]}),
  ct:pal("result: ~p",[length(Res)]),
  ok.

search4(Config)->
  ecomet_user:on_init_state(),
  Pattern=?config(pattern, Config),
  {_Header,Items} = ecomet_query:get([root],[<<".name">>,<<"integer">>],{'AND',[
    {<<".pattern">>,'=',Pattern},
    {'OR',[
      {<<"string2">>,'=',<<"value53">>},
      {<<"string2">>,'=',<<"value2">>}
    ]}
  ]}),
  ct:pal("result: ~p",[length(Items)]),
  ok.

search5(Config)->
  ecomet_user:on_init_state(),
  Pattern=?config(pattern, Config),
  Res=ecomet_query:get([root],[count],{'AND',[
    {<<".pattern">>,'=',Pattern},
    {'OR',[
      {<<"string2">>,'=',<<"value53">>},
      {<<"string2">>,'=',<<"value2">>}
    ]}
  ]}),
  ct:pal("result: ~p",[Res]),
  ok.

search6(Config)->
  ecomet_user:on_init_state(),
  Pattern=?config(pattern, Config),
  Res=ecomet_query:get([root],[<<".oid">>],{'AND',[
    {<<".pattern">>,'=',Pattern},
    {'OR',[
      {<<"string2">>,'=',<<"value53">>},
      {<<"string2">>,'=',<<"value2">>}
    ]}
  ]}),
  ct:pal("result: ~p",[length(Res)]),
  ok.
