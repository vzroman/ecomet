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

-module(ecomet_index_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").

-record(log,{field,value,type,storage,oper}).
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
  simple_index/1,
  '3gram_index'/1,
  datetime_index/1,
  single_storage/1,
  multi_storage/1,
  create_index/1,
  add_to_index/1,
  delete_object/1,
  unchanged/1,
  concurrent_build/1,
  concurrent_check/1
]).

all()->
  [
    {group,build_backtag},
    {group,merge_backtag},
    {group,dump_log},
    unchanged,
    {group,concurrent}
  ].

groups()->[
  {build_backtag,
    [sequence],  % Run strategy
    [                   % Cases:
      simple_index,       % Fields with simple indexes
      '3gram_index',       % Fields with 3gram indexes
      datetime_index             % Fields with datetime indexes
    ]
  },
  {merge_backtag,
    [sequence],  % Run strategy
    [                   % Cases:
      single_storage,       % All fields are in the same storage
      multi_storage       % Fields are in different storage
    ]
  },
  {dump_log,
    [sequence],  % Run strategy
    [                   % Cases:
      create_index,       % Create indexes for 1 object
      add_to_index,       % Add next object to the same tags
      delete_object       % Delete object
    ]
  },
  {concurrent,
    [sequence],  % Run strategy
    [                   % Cases:
      {build,  % Building index
        [parallel],
        [concurrent_build||_<-lists:seq(1,10)]
      },
      concurrent_check
    ]
  }
].

init_per_suite(Config)->
  ?BACKEND_INIT(),
  PatternID={?PATTERN_PATTERN,10000},
  OID={10000,1},
  FID={?FOLDER_PATTERN,10000},
  [
    {suite_pid,?SUITE_PROCESS_START()},
    {oid,OID},
    {folder_id,FID},
    {pattern_id,PatternID}
    |Config].

end_per_suite(Config)->
  ?SUITE_PROCESS_STOP(?GET(suite_pid,Config)),
  ?BACKEND_STOP(30000),
  ok.

init_per_group(concurrent,Config)->
  ?SUITE_PROCESS_EXECUTE(?GET(suite_pid,Config),fun()->
    % local params table
    ets:new(index_concurrent,[named_table,public,set])
  end),
  [{count,4000}|Config];
init_per_group(_,Config)->
  Config.

end_per_group(concurrent,Config)->
  ?SUITE_PROCESS_EXECUTE(?GET(suite_pid,Config),fun()->
    ets:delete(index_concurrent)
  end),
  ok;
end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

%%--------------------------------------------------------------
%%  Build back tags
%%--------------------------------------------------------------
simple_index(Config)->
  % Build new object with simple indexes
  FID=?GET(folder_id,Config),
  PatternID=?GET(pattern_id,Config),
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap =
    #{<<".name">> => #{ type=> string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<".folder">> => #{ type=>link, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<".pattern">> => #{ type=>link, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      <<".readgroups">> => #{ type=>list, subtype=>link, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      index_storage => [
        {ram,[<<".name">>]},
        {ramdisc,[<<".folder">>,<<".pattern">>]},
        {disc,[<<".pattern">>]}
      ]
    },
  RG1={10000,2},
  RG2={10000,3},
  Fields=ecomet_field:build_new(FieldsMap,#{
    <<".name">>=><<"theName">>,
    <<".pattern">>=>PatternID,
    <<".folder">>=>FID,
    <<".readgroups">>=>[RG2,RG1]
  }),
  % Create back tag structure
  BackTags=ecomet_index:build_back_tags(maps:to_list(Fields),FieldsMap),
  %%  Returns :
  %%  #{
  %%    ram => #{
  %%      <<".name">> => [ { <<"theName">>, simple } ]
  %%    },
  %%    ramdisc => #{
  %%      <<".folder">> => [ { {?ECOMET_FOLDERPATTERN,10000}, simple } ],
  %%      <<".readgroups">> => [ { {10000,2}, simple} , { {10000,3} , simple } ]
  %%    },
  %%    disc => #{
  %%      <<".pattern">> => [ { {?ECOMET_PATTERNPATTERN,10000} , simple } ]
  %%    }
  %%  }

  % Check tag for .name
  RamTags=maps:get(ram,BackTags),
  [{<<"theName">>,simple}]=maps:get(<<".name">>,RamTags),
  1=map_size(RamTags),
  RamDiscTags=maps:get(ramdisc,BackTags),
  % Tag 4 .folder
  [{FID,simple}]=maps:get(<<".folder">>,RamDiscTags),
  % Tags 4 .readgroups. Tags are sorted
  [{RG1,simple},{RG2,simple}]=maps:get(<<".readgroups">>,RamDiscTags),
  2=map_size(RamDiscTags),
  % .pattern tag
  DiscTags=maps:get(disc,BackTags),
  [{PatternID,simple}]=maps:get(<<".pattern">>,DiscTags),
  1=map_size(DiscTags).


'3gram_index'(_Config)->
  % Build new object with 3gram indexes
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap=
    #{<<"no_index">> => #{ type=> string, subtype=>none, index=>none, required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"none_value">> => #{ type=> string, subtype=>none, index=>[simple], required=>false, storage=>ram, default=>none, autoincrement=>false },
      <<".pattern">> => #{ type=>link, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      <<"simple">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"3gram">> => #{ type=>string, subtype=>none, index=>['3gram'], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"3gram_list">> => #{ type=>list, subtype=>string, index=>['3gram'], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"3gram_utf">> => #{ type=>string, subtype=>none, index=>['3gram'], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"simple_3gram">> => #{ type=>string, subtype=>none, index=>[simple,'3gram'], required=>true, storage=>ram, default=>none, autoincrement=>false },
      index_storage => [
        {ram,[<<"none_value">>, <<"simple">>, <<"3gram">>,<<"3gram_list">>, <<"3gram_utf">>, <<"simple_3gram">> ]}
      ]
    },
  Fields=ecomet_field:build_new(FieldsMap,#{
    <<"no_index">> => <<"not indexed field">>,
    <<"simple">> => <<"simple index">>,
    <<".pattern">> => {2,3},
    <<"3gram">> => <<"3gram index">>,
    <<"3gram_list">> => [<<"3gram value1">>,<<"3gram value2">>],
    <<"3gram_utf">> => unicode:characters_to_binary("кириллица"),
    <<"simple_3gram">> => <<"simple and 3gram">>
  }),
  %% Returns
  %%  #{<<"simple">>=><<"simple index">>,
  %%  <<"none_value">>=>none,
  %%  <<"3gram">>=><<"3gram index">>,
  %%  <<"3gram_list">>=>[<<"3gram value1">>,<<"3gram value2">>],
  %%  <<"3gram_utf">>=><<208,186,208,184,209,128,208,184,208,187,208,187,208,184,209,134,208,176>>,    <<<< - "кириллица"
  %%  <<"simple_3gram">>=><<"simple and 3gram">>
  %%  }.

  % Create back tag structure
  BackTags=ecomet_index:build_back_tags(maps:to_list(Fields),FieldsMap),
  %%  Returns :
  %%  #{
  %%    ram => #{
  %%      <<"simple">> => [ { <<"simple index">>,  simple } ],
  %%      <<"none_value">> => [ { none, simple } ],
  %%      <<"3gram index">> => [{<<" IN">>,'3gram'},{<<"3GR">>,'3gram'},{<<"AM ">>,'3gram'},{<<"DEX">>,'3gram'},{<<"EX$">>,'3gram'},{<<"GRA">>,'3gram'},{<<"IND">>,'3gram'},{<<"M I">>,'3gram'},{<<"NDE">>,'3gram'},{<<"RAM">>,'3gram'},{<<"^3G">>,'3gram'}]
  %%      <<"3gram_list">> =>  [ {<<" VA">>,'3gram'},{<<"3GR">>,'3gram'},{<<"ALU">>,'3gram'},{<<"AM ">>,'3gram'},{<<"E1$">>,'3gram'},{<<"E2$">>,'3gram'},{<<"GRA">>,'3gram'},{<<"LUE">>,'3gram'},{<<"M V">>,'3gram'},{<<"RAM">>,'3gram'},{<<"UE1">>,'3gram'},{<<"UE2">>,'3gram'},{<<"VAL">>,'3gram'},{<<"^3G">>,'3gram'}]
  %%      <<"3gram_utf">> =>   [{<<94,208,154,208,152>>,'3gram'}, {<<208,152,208,155,208,155>>,'3gram'}, {<<208,152,208,160,208,152>>,'3gram'}, {<<208,152,208,166,208,144>>,'3gram'}, {<<208,154,208,152,208,160>>,'3gram'}, {<<208,155,208,152,208,166>>,'3gram'}, {<<208,155,208,155,208,152>>,'3gram'},{<<208,160,208,152,208,155>>,'3gram'}, {<<208,166,208,144,36>>,'3gram'}]
  %%      <<"simple_3gram">> => [{<<" 3G">>,'3gram'},{<<" AN">>,'3gram'},{<<"3GR">>,'3gram'},{<<"AM$">>,'3gram'},{<<"AND">>,'3gram'},{<<"D 3">>,'3gram'},{<<"E A">>,'3gram'},{<<"GRA">>,'3gram'},{<<"IMP">>,'3gram'},{<<"LE ">>,'3gram'},{<<"MPL">>,'3gram'},{<<"ND ">>,'3gram'},{<<"PLE">>,'3gram'},{<<"RAM">>,'3gram'},{<<"SIM">>,'3gram'},{<<"^SI">>,'3gram'},{<<"simple and 3gram">>,simple}]
  %%    }
  %%  }
  Tags=maps:get(ram,BackTags),
  % No index field
  undefined=maps:get(<<"no_index">>,Tags,undefined),
  % None value field
  []=maps:get(<<"none_value">>,Tags),
  % Simple index
  [{<<"simple index">>,simple}]=maps:get(<<"simple">>,Tags),

  % 3gram index. String "3gram index" is splitted to:
  The3Grams=[<<"^3G">>,<<"3GR">>,<<"GRA">>,<<"RAM">>,<<"AM ">>,<<"M I">>,<<" IN">>,<<"IND">>,<<"NDE">>,<<"DEX">>,<<"EX$">>],
  Tags3Gram=maps:get(<<"3gram">>,Tags),
  % Check all tags are exists
  lists:foreach(fun(TheGram)->
    true=lists:member({TheGram,'3gram'},Tags3Gram)
  end,The3Grams),
  % Check no other tags
  []=lists:subtract(Tags3Gram,[{Tag,'3gram'}||Tag<-The3Grams]),

  % 3gram for list fields. String "3gram value1" is splitted to:
  Val1_3grams=[<<"^3G">>,<<"3GR">>,<<"GRA">>,<<"RAM">>,<<"AM ">>,<<"M V">>,<<" VA">>,<<"VAL">>,<<"ALU">>,<<"LUE">>,<<"UE1">>,<<"E1$">>],
  Val2_3grams=[<<"^3G">>,<<"3GR">>,<<"GRA">>,<<"RAM">>,<<"AM ">>,<<"M V">>,<<" VA">>,<<"VAL">>,<<"ALU">>,<<"LUE">>,<<"UE2">>,<<"E2$">>],
  % Tags for field are organized to sorted list - no repeated tags
  TheList3Grams=ordsets:from_list(Val1_3grams++Val2_3grams),
  Tags3GramList=maps:get(<<"3gram_list">>,Tags),
  % Check all tags are exists
  lists:foreach(fun(TheGram)->
    true=lists:member({TheGram,'3gram'},Tags3GramList)
  end,TheList3Grams),
  % Check no other tags
  []=lists:subtract(Tags3GramList,[{Tag,'3gram'}||Tag<-TheList3Grams]),

  % 3gram for utf string. String "кириллица" is splitted to:
  UTF_3grams=[unicode:characters_to_binary(I)||I<-["^КИ","КИР","ИРИ","РИЛ","ИЛЛ","ЛЛИ","ЛИЦ","ИЦА","ЦА$"]],
  Tags3GramUTF=maps:get(<<"3gram_utf">>,Tags),
  io:format("Tags3GramUTF: ~p",[Tags3GramUTF]),
  io:format("UTF_3grams: ~p",[UTF_3grams]),
  % Check all tags are exists
  lists:foreach(fun(TheGram)->
    true=lists:member({TheGram,'3gram'},Tags3GramUTF)
  end,UTF_3grams),
  % Check no other tags
  []=lists:subtract(Tags3GramUTF,[{Tag,'3gram'}||Tag<-UTF_3grams]),

  % Mixed index. simple and 3gram. String "simple and 3gram" is splitted to:
  Mixed3Grams=[<<"^SI">>,<<"SIM">>,<<"IMP">>,<<"MPL">>,<<"PLE">>,<<"LE ">>,<<"E A">>,<<" AN">>,<<"AND">>,<<"ND ">>,<<"D 3">>,<<" 3G">>,<<"3GR">>,<<"GRA">>,<<"RAM">>,<<"AM$">>],
  TagsMixed=maps:get(<<"simple_3gram">>,Tags),
  % Check all 3 gram tags are exists
  lists:foreach(fun(TheGram)->
    true=lists:member({TheGram,'3gram'},TagsMixed)
  end,Mixed3Grams),
  % Check simple tag
  true=lists:member({<<"simple and 3gram">>,simple},TagsMixed),
  % Check no other tags
  []=lists:subtract(TagsMixed,[{<<"simple and 3gram">>,simple}]++[{Tag,'3gram'}||Tag<-Mixed3Grams]).

datetime_index(_Config)->
  % Build new object with 3gram indexes
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap=
    #{<<"no_index">> => #{ type=> string, subtype=>none, index=>none, required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"none_value">> => #{ type=> string, subtype=>none, index=>[simple], required=>false, storage=>ram, default=>none, autoincrement=>false },
      <<".pattern">> => #{ type=>link, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      <<"simple">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"datetime">> => #{ type=>integer, subtype=>none, index=>[datetime], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"datetime_list">> => #{ type=>list, subtype=>integer, index=>[datetime], required=>true, storage=>ram, default=>none, autoincrement=>false },
      index_storage => [
        {ram,[<<"none_value">>, <<"simple">>, <<"datetime">> ]}
      ]
    },
  DT1 = calendar:rfc3339_to_system_time("2020-01-01 15:59:59.088Z", [{unit, millisecond}]),
  DT2 = calendar:rfc3339_to_system_time("2020-12-31 23:59:59.088Z", [{unit, millisecond}]),
  DT3 = calendar:rfc3339_to_system_time("2020-01-01 01:01:01.088Z", [{unit, millisecond}]),
  DT4 = calendar:rfc3339_to_system_time("2020-01-01 00:00:00.088Z", [{unit, millisecond}]),

  Fields=ecomet_field:build_new(FieldsMap,#{
    <<"no_index">> => <<"not indexed field">>,
    <<"simple">> => <<"simple index">>,
    <<".pattern">> => {2,3},
    <<"datetime">> => DT1,
    <<"datetime_list">> => [DT2,DT3,DT4]
  }),

  % Create back tag structure
  BackTags=ecomet_index:build_back_tags(maps:to_list(Fields),FieldsMap),
  %%  Returns :
  %%  #{disc => #{<<".pattern">> => [{{2,3},simple}]},
  %%    ram =>
  %%    #{<<"datetime">> =>
  %%    [{{d,1},datetime},
  %%      {{h,15},datetime},
  %%      {{m,1},datetime},
  %%      {{mi,59},datetime},
  %%      {{s,59},datetime},
  %%      {{y,2020},datetime}],
  %%      <<"datetime_list">> =>
  %%      [{{d,1},datetime},
  %%        {{d,31},datetime},
  %%        {{h,0},datetime},
  %%        {{h,1},datetime},
  %%        {{h,23},datetime},
  %%        {{m,1},datetime},
  %%        {{m,12},datetime},
  %%        {{mi,0},datetime},
  %%        {{mi,1},datetime},
  %%        {{mi,59},datetime},
  %%        {{s,0},datetime},
  %%        {{s,1},datetime},
  %%        {{s,59},datetime},
  %%        {{y,2020},datetime}],
  %%      <<"none_value">> => [],
  %%      <<"simple">> => [{<<"simple index">>,simple}]}}
  RamTags=maps:get(ram,BackTags),
  % No index field
  undefined=maps:get(<<"no_index">>,RamTags,undefined),
  % None value field
  []=maps:get(<<"none_value">>,RamTags),
  DT=maps:get(<<"datetime">>,RamTags),
  % Datetime index
  [{{d,1},datetime},
    {{h,15},datetime},
    {{m,1},datetime},
    {{mi,59},datetime},
    {{s,59},datetime},
    {{y,2020},datetime}] = DT,
  % Datetime List index
  DT_List=maps:get(<<"datetime_list">>,RamTags),
  % subtract years
  DTL1 = lists:subtract(DT_List, [{{y,2020},datetime}]),
  % subtract months
  DTL2 = lists:subtract(DTL1, [{{m,1},datetime}]),
  DTL3 = lists:subtract(DTL2, [{{m,12},datetime}]),
  % subtract days
  DTL4 = lists:subtract(DTL3, [{{d,1},datetime}]),
  DTL5 = lists:subtract(DTL4, [{{d,31},datetime}]),
  % subtract hours
  DTL6 = lists:subtract(DTL5, [{{h,0},datetime}]),
  DTL7 = lists:subtract(DTL6, [{{h,1},datetime}]),
  DTL8 = lists:subtract(DTL7, [{{h,23},datetime}]),
  % subtract minutes
  DTL9 = lists:subtract(DTL8, [{{mi,0},datetime}]),
  DTL10 = lists:subtract(DTL9, [{{mi,1},datetime}]),
  DTL11 = lists:subtract(DTL10, [{{mi,59},datetime}]),
  % subtract seconds
  DTL12 = lists:subtract(DTL11, [{{s,0},datetime}]),
  DTL13 = lists:subtract(DTL12, [{{s,1},datetime}]),
  [] = lists:subtract(DTL13, [{{s,59},datetime}]),

  % Simple index
  [{<<"simple index">>,simple}]=maps:get(<<"simple">>,RamTags),
  ok.

%%--------------------------------------------------------------
%%  Merge back tags
%%--------------------------------------------------------------
single_storage(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap =
    #{<<"field1">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<"field2">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<"field3">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      index_storage => [ {ramdisc,[<<"field1">>,<<"field2">>,<<"field3">>]} ]
    },
  Fields=ecomet_field:build_new(FieldsMap,#{
    <<"field1">>=><<"value1">>,
    <<"field2">>=><<"value2">>,
    <<"field3">>=><<"value3">>
  }),
  %% Returns:
  %%  #{<<"field1">>=><<"value1">>,
  %%  <<"field2">>=><<"value2">>,
  %%  <<"field3">>=><<"value3">>}.
  NewTags=ecomet_index:build_back_tags(maps:to_list(Fields),FieldsMap),
  %% Returns:
  %%  #{
  %%    ramdisc=> #{
  %%      <<"field1">>=>[{ <<"value1">>, simple }],
  %%      <<"field2">>=>[{ <<"value2">>, simple }],
  %%      <<"field3">>=>[{ <<"value3">>, simple }]
  %%    }
  %%  }
  {Merged,Log}=ecomet_index:merge_backtags(NewTags,#{}),
  %% Returns:
  %%  {#{
  %%    ramdisc=> #{
  %%      <<"field1">>=>[{ [<<"value1">>], simple }],
  %%      <<"field2">>=>[{ [<<"value2">>], simple }],
  %%      <<"field3">>=>[{ [<<"value3">>], simple }]
  %%    },
  %%    [#log{field=<<"field1">>,value=[<<"value1">>],type=simple,storage=ramdisc,oper=add},
  %%      #log{field=<<"field2">>,value=[<<"value2">>],type=simple,storage=ramdisc,oper=add},
  %%      #log{field=<<"field3">>,value=[<<"value3">>],type=simple,storage=ramdisc,oper=add} ]
  %%  }.
  1=maps:size(NewTags),
  1=maps:size(Merged),
  NewFields=maps:get(ramdisc,NewTags),
  MergedFields=maps:get(ramdisc,Merged),
  NewFieldsList=maps:to_list(NewFields),
  % It's creation, merged fields are fully identical new fields
  NewFieldsList=maps:to_list(MergedFields),
  % Check log
  3=length(Log),
  #log{value= <<"value1">>,type=simple,storage=ramdisc,oper=add}=lists:keyfind(<<"field1">>,2,Log),
  #log{value= <<"value2">>,type=simple,storage=ramdisc,oper=add}=lists:keyfind(<<"field2">>,2,Log),
  #log{value= <<"value3">>,type=simple,storage=ramdisc,oper=add}=lists:keyfind(<<"field3">>,2,Log),

  % Change field value
  ChangedFields=ecomet_field:merge(FieldsMap,Fields,#{<<"field2">>=><<"value2 new">>}),
  %% Returns:
  %%  #{<<"field1">>=><<"value1">>,
  %%  <<"field2">>=><<"value2 new">>,
  %%  <<"field3">>=><<"value3">>}.
  ChangedTagsBuild=ecomet_index:build_back_tags(maps:to_list(ChangedFields),FieldsMap),
  %% Returns:
  %%  #{
  %%    ramdisc=> #{
  %%      <<"field1">>=>[{ <<"value1">>, simple }],
  %%      <<"field2">>=>[{ <<"value2 new">>, simple }],
  %%      <<"field3">>=>[{ <<"value3">>, simple }]
  %%    }
  %%  }
  {ChangedTags,ChangeLog}=ecomet_index:merge_backtags(ChangedTagsBuild,Merged),
  %% Returns:
  %%  #{
  %%    ramdisc=> #{
  %%      <<"field1">>=>[{ [<<"value1">>], simple }],
  %%      <<"field2">>=>[{ [<<"value2 new">>], simple }],
  %%      <<"field3">>=>[{ [<<"value3">>], simple }]
  %%    },
  %%    [#log{field=<<"field2">>,value=[<<"value2">>],type=simple,storage=ramdisc,oper=del},
  %%      #log{field=<<"field2">>,value=[<<"value2 new">>],type=simple,storage=ramdisc,oper=add}]
  %%  }.
  ChangedFieldsTags=maps:get(ramdisc,ChangedTags),
  [{<<"value2 new">>,simple}]=maps:get(<<"field2">>,ChangedFieldsTags),
  [{<<"value1">>,simple}]=maps:get(<<"field1">>,ChangedFieldsTags),
  [{<<"value3">>,simple}]=maps:get(<<"field3">>,ChangedFieldsTags),
  % Check log
  2=length(ChangeLog),
  #log{oper=add,field = <<"field2">>,type=simple,storage=ramdisc}=lists:keyfind(<<"value2 new">>,3,ChangeLog),
  #log{oper=del,field = <<"field2">>,type=simple,storage=ramdisc}=lists:keyfind(<<"value2">>,3,ChangeLog).

multi_storage(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap=
    #{<<"field1">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field2">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field3">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<"field4">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      index_storage => [ {ram,[<<"field1">>,<<"field2">>]},
        {ramdisc,[<<"field3">>]},
        {disc,[<<"field4">>]} ]
    },
  Fields=ecomet_field:build_new(FieldsMap,#{
    <<"field1">>=><<"value1">>,
    <<"field2">>=><<"value2">>,
    <<"field3">>=><<"value3">>,
    <<"field4">>=><<"value4">>
  }),
  ct:pal("Fields ~p",[Fields]),
  %% Returns:
  %%  #{<<"field1">>=><<"value1">>,
  %%  <<"field2">>=><<"value2">>,
  %%  <<"field3">>=><<"value3">>},
  %%  <<"field4">>=><<"value4">>}.
  NewTags=ecomet_index:build_back_tags(maps:to_list(Fields),FieldsMap),
  ct:pal("NewTags ~p",[NewTags]),
  %% Returns:
  %%  #{
  %%    ram=> #{
  %%      <<"field1">>=>[{ <<"value1">>, simple }],
  %%      <<"field2">>=>[{ <<"value2">>, simple }]
  %%    },
  %%    ramdisc=> #{
  %%      <<"field3">>=>[{ <<"value3">>, simple }]
  %%    },
  %%    disc=> #{
  %%      <<"field4">>=>[{ <<"value4">>, simple }]
  %%    }
  %%  }
  {Merged,Log}=ecomet_index:merge_backtags(NewTags,#{}),
  ct:pal("Merged ~p",[Merged]),
  ct:pal("Log ~p",[Log]),
  %% Returns:
  %%  {#{
  %%    ram=> #{
  %%        <<"field1">>=>[{ <<"value1">>, simple }],
  %%        <<"field2">>=>[{ <<"value2">>, simple }]
  %%      },
  %%      ramdisc=> #{
  %%        <<"field3">>=>[{ <<"value3">>, simple }]
  %%      },
  %%     disc=> #{
  %%      <<"field4">>=>[{ <<"value4">>, simple }]
  %%     },
  %%    [#log{field=<<"field1">>,value=[<<"value1">>],type=simple,storage=ram,oper=add},
  %%      #log{field=<<"field2">>,value=[<<"value2">>],type=simple,storage=ram,oper=add},
  %%      #log{field=<<"field3">>,value=[<<"value3">>],type=simple,storage=ramdisc,oper=add},
  %%      #log{field=<<"field4">>,value=[<<"value4">>],type=simple,storage=disc,oper=add}]
  %%  }.

  % 3 Storage types are involved
  3=maps:size(Merged),
  Ram=maps:get(ram,Merged),
  RamDisc=maps:get(ramdisc,Merged),
  Disc=maps:get(disc,Merged),
  % 2 fields in ram storage
  2=maps:size(Ram),
  [{<<"value1">>,simple}]=maps:get(<<"field1">>,Ram),
  [{<<"value2">>,simple}]=maps:get(<<"field2">>,Ram),
  % 1 field in ramdisc
  1=maps:size(RamDisc),
  [{<<"value3">>,simple}]=maps:get(<<"field3">>,RamDisc),
  % 1 field in disc
  1=maps:size(Disc),
  [{<<"value4">>,simple}]=maps:get(<<"field4">>,Disc),
  % Check log
  4=length(Log),
  #log{value= <<"value1">>,type=simple,storage=ram,oper=add}=lists:keyfind(<<"field1">>,2,Log),
  #log{value= <<"value2">>,type=simple,storage=ram,oper=add}=lists:keyfind(<<"field2">>,2,Log),
  #log{value= <<"value3">>,type=simple,storage=ramdisc,oper=add}=lists:keyfind(<<"field3">>,2,Log),
  #log{value= <<"value4">>,type=simple,storage=disc,oper=add}=lists:keyfind(<<"field4">>,2,Log),
  % Change values
  ChangedFields=ecomet_field:merge(FieldsMap,Fields,#{<<"field2">>=><<"value2 new">>,<<"field4">>=><<"value4 new">>}),
  %% Returns:
  %%  #{<<"field1">>=><<"value1">>,
  %%  <<"field2">>=><<"value2 new">>,
  %%  <<"field3">>=><<"value3">>,
  %% <<"field4">>=><<"value4 new">>}.
  ChangedTagsBuild=ecomet_index:build_back_tags(maps:to_list(ChangedFields),FieldsMap),
  %% Returns:
  %%  #{
  %%    ram=> #{
  %%      <<"field1">>=>[{ <<"value1">>, simple }],
  %%      <<"field2">>=>[{ <<"value2 new">>, simple }]
  %%    },
  %%    ramdisc=> #{
  %%      <<"field3">>=>[{ <<"value3">>, simple }]
  %%    },
  %%    disc=> #{
  %%      <<"field4">>=>[{ <<"value4 new">>, simple }]
  %%    }
  %%  }
  {ChangedTags,ChangeLog}=ecomet_index:merge_backtags(ChangedTagsBuild,Merged),
  %% Returns:
  %%  {#{
  %%    ram=> #{
  %%        <<"field1">>=>[{ <<"value1">>, simple }],
  %%        <<"field2">>=>[{ <<"value2 new">>, simple }]
  %%      },
  %%      ramdisc=> #{
  %%        <<"field3">>=>[{ <<"value3">>, simple }]
  %%      },
  %%     disc=> #{
  %%      <<"field4">>=>[{ <<"value4 new">>, simple }]
  %%     },
  %%    [#log{field=<<"field2">>,value=<<"value2">>,type=simple,storage=ram,oper=del},
  %%      #log{field=<<"field4">>,value=<<"value4">>,type=simple,storage=disc,oper=del},
  %%      #log{field=<<"field2">>,value=<<"value2 new">>,type=simple,storage=ram,oper=add},
  %%      #log{field=<<"field4">>,value=<<"value4 new">>,type=simple,storage=disc,oper=add}]
  %%  }.
  3=maps:size(ChangedTags),
  ChangedRam=maps:get(ram,ChangedTags),
  ChangedRamDisc=maps:get(ramdisc,ChangedTags),
  ChangedDisc=maps:get(disc,ChangedTags),
  % Check ram
  2=maps:size(ChangedRam),
  [{<<"value1">>,simple}]=maps:get(<<"field1">>,ChangedRam),
  [{<<"value2 new">>,simple}]=maps:get(<<"field2">>,ChangedRam),
  % Check ramdisc
  1=maps:size(ChangedRamDisc),
  [{<<"value3">>,simple}]=maps:get(<<"field3">>,ChangedRamDisc),
  % Check disc
  1=maps:size(ChangedDisc),
  [{<<"value4 new">>,simple}]=maps:get(<<"field4">>,ChangedDisc),
  % Check log
  4=length(ChangeLog),
  #log{oper=add,field = <<"field2">>,type=simple,storage=ram}=lists:keyfind(<<"value2 new">>,3,ChangeLog),
  #log{oper=del,field = <<"field2">>,type=simple,storage=ram}=lists:keyfind(<<"value2">>,3,ChangeLog),
  #log{oper=add,field = <<"field4">>,type=simple,storage=disc}=lists:keyfind(<<"value4 new">>,3,ChangeLog),
  #log{oper=del,field = <<"field4">>,type=simple,storage=disc}=lists:keyfind(<<"value4">>,3,ChangeLog).



%%--------------------------------------------------------------
%%  Save to storage
%%--------------------------------------------------------------
create_index(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap=
    #{<<"field1">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field2">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field3">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<"field4">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      index_storage => [ {ram,[<<"field1">>,<<"field2">>]},
        {ramdisc,[<<"field3">>]},
        {disc,[<<"field4">>]} ]
    },
  Fields=ecomet_field:build_new(FieldsMap,#{
    <<"field1">>=><<"value1">>,
    <<"field2">>=><<"value2">>,
    <<"field3">>=><<"value3">>,
    <<"field4">>=><<"value4">>
  }),
  %% Returns:
  %%  #{<<"field1">>=><<"value1">>,
  %%  <<"field2">>=><<"value2">>,
  %%  <<"field3">>=><<"value3">>},
  %%  <<"field4">>=><<"value4">>}.
  NewTags=ecomet_index:build_back_tags(maps:to_list(Fields),FieldsMap),
  %% Returns:
  %%  #{
  %%    ram=> #{
  %%      <<"field1">>=>[{ <<"value1">>, simple }],
  %%      <<"field2">>=>[{ <<"value2">>, simple }]
  %%    },
  %%    ramdisc=> #{
  %%      <<"field3">>=>[{ <<"value3">>, simple }]
  %%    },
  %%    disc=> #{
  %%      <<"field4">>=>[{ <<"value4">>, simple }]
  %%    }
  %%  }
  {Merged,Log}=ecomet_index:merge_backtags(NewTags,#{}),
  %% Returns:
  %%  {#{
  %%    ram=> #{
  %%        <<"field1">>=>[{ <<"value1">>, simple }],
  %%        <<"field2">>=>[{ <<"value2">>, simple }]
  %%      },
  %%      ramdisc=> #{
  %%        <<"field3">>=>[{ <<"value3">>, simple }]
  %%      },
  %%     disc=> #{
  %%      <<"field4">>=>[{ <<"value4">>, simple }]
  %%     },
  %%    [#log{field=<<"field1">>,value=<<"value1">>,type=simple,storage=ram,oper=add},
  %%      #log{field=<<"field2">>,value=<<"value2">>,type=simple,storage=ram,oper=add},
  %%      #log{field=<<"field3">>,value=<<"value3">>,type=simple,storage=ramdisc,oper=add},
  %%      #log{field=<<"field4">>,value=<<"value4">>,type=simple,storage=disc,oper=add}]
  %%  }.
  OID={10000,1},
  DB=ecomet_object:get_db_name(OID),
  ct:pal("DB: ~p ",[DB]),
  Need = ecomet_schema:get_registered_databases(),
  ct:pal("ALLL registered BDs: ~p ",[Need]),
  {ok,{Add,Del}}=ecomet:transaction(fun()->
    ecomet_index:dump_log(OID,Log)
    end),
  ct:pal("Add: ~p, Del: ~p",[Add, Del]),
  %%  Returns:
  %%    {[ {<<"field1">>,<<"value1">>,simple},
  %%        {<<"field2">>,<<"value2">>,simple},
  %%        {<<"field3">>,<<"value3">>,simple},
  %%        {<<"field4">>,<<"value4">>,simple}],
  %%    []
  %%  }.
  Value1Tag={<<"field1">>,<<"value1">>,simple},
  Value2Tag={<<"field2">>,<<"value2">>,simple},
  Value3Tag={<<"field3">>,<<"value3">>,simple},
  Value4Tag={<<"field4">>,<<"value4">>,simple},
  % Check changes
  [Value1Tag,Value2Tag,Value3Tag,Value4Tag]=ordsets:from_list(Add),
  []=Del,
  Db=ecomet_object:get_db_name(OID),
  ct:pal("Db ~p",[Db]),
  check_tag(Value1Tag,Db,[ram],[OID]),
  check_tag(Value2Tag,Db,[ram],[OID]),
  check_tag(Value3Tag,Db,[ramdisc],[OID]),
  check_tag(Value4Tag,Db,[disc],[OID]),

  % Change values
  ChangedFields=ecomet_field:merge(FieldsMap,Fields,#{<<"field2">>=><<"value2 new">>,<<"field4">>=><<"value4 new">>}),
  ChangedTagsBuild=ecomet_index:build_back_tags(maps:to_list(ChangedFields),FieldsMap),
  {_ChangedTags,ChangeLog}=ecomet_index:merge_backtags(ChangedTagsBuild,Merged),
  {ok,{ChangeAdd,ChangeDel}}=ecomet:transaction(fun()->
    ecomet_index:dump_log(OID,ChangeLog)
    end),
  ct:pal("ChangeAdd: ~p, ChangeDel: ~p",[ChangeAdd, ChangeDel]),
  Value2TagNew={<<"field2">>,<<"value2 new">>,simple},
  Value4TagNew={<<"field4">>,<<"value4 new">>,simple},
  % New tags added
  [Value2TagNew,Value4TagNew]=ordsets:from_list(ChangeAdd),
  % Old tags deleted
  [Value2Tag,Value4Tag]=ordsets:from_list(ChangeDel),
  check_tag(Value2Tag,Db,[],[]),
  check_tag(Value2TagNew,Db,[ram],[OID]),
  check_tag(Value4Tag,Db,[],[]),
  check_tag(Value4TagNew,Db,[disc],[OID]).


add_to_index(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap=
    #{<<"field1">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field2">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field3">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<"field4">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      index_storage => [ {ram,[<<"field1">>,<<"field2">>]},
        {ramdisc,[<<"field3">>]},
        {disc,[<<"field4">>]} ]
    },
  Fields=ecomet_field:build_new(FieldsMap,#{
    <<"field1">>=><<"value1">>,
    <<"field2">>=><<"value2">>,
    <<"field3">>=><<"value3">>,
    <<"field4">>=><<"value4">>
  }),
  %% Returns:
  %%  #{<<"field1">>=><<"value1">>,
  %%  <<"field2">>=><<"value2">>,
  %%  <<"field3">>=><<"value3">>},
  %%  <<"field4">>=><<"value4">>}.
  NewTags=ecomet_index:build_back_tags(maps:to_list(Fields),FieldsMap),
  {_Merged,Log}=ecomet_index:merge_backtags(NewTags,#{}),
  OID2={10000,3},
  Db2=ecomet_object:get_db_name(OID2),
  ecomet:transaction(fun()->
    ecomet_index:dump_log(OID2,Log)
                     end),
  Value1Tag={<<"field1">>,<<"value1">>,simple},
  Value2Tag={<<"field2">>,<<"value2">>,simple},
  Value3Tag={<<"field3">>,<<"value3">>,simple},
  Value4Tag={<<"field4">>,<<"value4">>,simple},

  % On create_index step we added tags for OID1 object:
  % {<<"field1">>,<<"value1">>,simple},
  % {<<"field2">>,<<"value2 new">>,simple},
  % {<<"field3">>,<<"value3">>,simple},
  % {<<"field4">>,<<"value4 new">>,simple},
  OID1={10000,1},
  check_tag(Value1Tag,Db2,[ram],[OID1,OID2]),
  check_tag(Value2Tag,Db2,[ram],[OID2]),
  check_tag(Value3Tag,Db2,[ramdisc],[OID1,OID2]),
  check_tag(Value4Tag,Db2,[disc],[OID2]).


delete_object(_Config)->
  % We created on create_index and add_to_index steps 3 objects:
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap1=
    #{<<"field1">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field2">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field3">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<"field4">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      index_storage => [ {ram,[<<"field1">>,<<"field2">>]},
        {ramdisc,[<<"field3">>]},
        {disc,[<<"field4">>]} ]
    },
  Fields1=ecomet_field:build_new(FieldsMap1,#{
    <<"field1">>=><<"value1">>,
    <<"field2">>=><<"value2 new">>,
    <<"field3">>=><<"value3">>,
    <<"field4">>=><<"value4 new">>
  }),
  %% Returns:
  %%  #{<<"field1">>=><<"value1">>,
  %%  <<"field2">>=><<"value2">>,
  %%  <<"field3">>=><<"value3">>},
  %%  <<"field4">>=><<"value4">>}.
  OID1={10000,1},
  Tags1=ecomet_index:build_back_tags(maps:to_list(Fields1),FieldsMap1),

  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap2=
    #{<<"field1">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field2">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field3">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<"field4">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      index_storage => [ {ram,[<<"field1">>,<<"field2">>]},
        {ramdisc,[<<"field3">>]},
        {disc,[<<"field4">>]} ]
    },
  Fields2=ecomet_field:build_new(FieldsMap2,#{
    <<"field1">>=><<"value1">>,
    <<"field2">>=><<"value2">>,
    <<"field3">>=><<"value3">>,
    <<"field4">>=><<"value4">>
  }),
  OID2={10000,3},
  DB = ecomet_object:get_db_name(OID2),

  Tags2=ecomet_index:build_back_tags(maps:to_list(Fields2),FieldsMap2),

  Value1Tag={<<"field1">>,<<"value1">>,simple},
  Value2Tag={<<"field2">>,<<"value2">>,simple},
  Value3Tag={<<"field3">>,<<"value3">>,simple},
  Value4Tag={<<"field4">>,<<"value4">>,simple},

  % Delete OID1
  ecomet:transaction(fun()-> ecomet_index:delete_object(OID1,Tags1) end),
  check_tag(Value1Tag,DB,[ram],[OID2]),
  check_tag(Value2Tag,DB,[ram],[OID2]),
  check_tag(Value3Tag,DB,[ramdisc],[OID2]),
  check_tag(Value4Tag,DB,[disc],[OID2]),


  % Delete OID2
  ecomet:transaction(fun()-> ecomet_index:delete_object(OID2,Tags2) end).


check_tag(Tag,Db,StorageTypes,OIDs)->
  ct:pal("Tag ~p ,Db ~p ,StorageTypes  ~p", [Tag,Db,StorageTypes]),
  SStorages=ordsets:from_list(StorageTypes),
  RStorages=lists:reverse(ordsets:from_list([ram,ramdisc,disc,ramlocal])),
  PatternsBits=
    lists:foldl(fun(Storage,AccBits)->
      case ecomet_index:read_tag(Db,Storage,[],Tag) of
        none->
          false=lists:member(Storage,SStorages),
          AccBits;
        Bits->
          true=lists:member(Storage,SStorages),
          ecomet_bits:oper('OR',AccBits,Bits)
      end
    end,none,RStorages),
  ct:pal("PatternsBits: ~p",[PatternsBits]),
  FoundOIDs=
    lists:foldl(fun(Storage,AccOIDs)->
      element(2,ecomet_bits:foldl(fun(PatternId,POIDs)->
        IDHBits=ecomet_index:read_tag(Db,Storage,[PatternId],Tag),
        element(2,ecomet_bits:foldl(fun(IDH,HOIDs)->
          IDLBits=ecomet_index:read_tag(Db,Storage,[PatternId,IDH],Tag),
          element(2,ecomet_bits:foldl(fun(IDL,LOIDs)->
            [{PatternId,IDH*?BITSTRING_LENGTH+IDL}|LOIDs]
          end,HOIDs,IDLBits,{none,none}))
        end,POIDs,IDHBits,{none,none}))
      end,AccOIDs,PatternsBits,{none,none}))
  end,[],RStorages),
  ct:pal("FoundOIDs: ~p",[FoundOIDs]),
  ct:pal("OIDs: ~p",[OIDs]),
  % Found expected
  []=lists:subtract(OIDs,FoundOIDs),
  % Did not found unexpected
  []=lists:subtract(FoundOIDs,OIDs).


%%--------------------------------------------------------------
%%  Calculated unchanged tags
%%--------------------------------------------------------------
unchanged(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap=
    #{<<"field1">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field2">> => #{ type=>list, subtype=>string, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field3">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<"field4">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      index_storage => [ {ram,[<<"field1">>,<<"field2">>]},
        {ramdisc,[<<"field3">>]},
        {disc,[<<"field4">>]} ]
    },
  Fields=ecomet_field:build_new(FieldsMap,#{
    <<"field1">>=><<"value1">>,
    <<"field2">>=>[<<"value21">>,<<"value22">>,<<"value23">>],
    <<"field3">>=><<"value3">>,
    <<"field4">>=><<"value4">>
  }),
  Tags=ecomet_index:build_back_tags(maps:to_list(Fields),FieldsMap),
  % Create new, no unchanged
  []=ecomet_index:get_unchanged(Tags,#{}),
  % No changes
  [{<<"field1">>,<<"value1">>,simple},
    {<<"field2">>,<<"value21">>,simple},
    {<<"field2">>,<<"value22">>,simple},
    {<<"field2">>,<<"value23">>,simple},
    {<<"field3">>,<<"value3">>,simple},
    {<<"field4">>,<<"value4">>,simple}]=ordsets:from_list(ecomet_index:get_unchanged(Tags,Tags)),

  % Change values
  ChangedFields1=ecomet_field:merge(FieldsMap,Fields,#{<<"field1">>=><<"value1 new">>,<<"field4">>=><<"value4 new">>}),
  ChangedTags1=ecomet_index:build_back_tags(maps:to_list(ChangedFields1),FieldsMap),
  [{<<"field2">>,<<"value21">>,simple},
    {<<"field2">>,<<"value22">>,simple},
    {<<"field2">>,<<"value23">>,simple},
    {<<"field3">>,<<"value3">>,simple}]=ordsets:from_list(ecomet_index:get_unchanged(ChangedTags1,Tags)),

  % Change values for one field
  ChangedFields2=ecomet_field:merge(FieldsMap,Fields,#{<<"field2">>=>[<<"value21 new">>,<<"value22">>,<<"value23 new">>]}),
  ChangedTags2=ecomet_index:build_back_tags(maps:to_list(ChangedFields2),FieldsMap),
  [{<<"field1">>,<<"value1">>,simple},
    {<<"field2">>,<<"value22">>,simple},
    {<<"field3">>,<<"value3">>,simple},
    {<<"field4">>,<<"value4">>,simple}]=ordsets:from_list(ecomet_index:get_unchanged(ChangedTags2,Tags)).


%%--------------------------------------------------------------
%%  Calculated unchanged tags
%%--------------------------------------------------------------
concurrent_build(Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  FieldsMap=
    #{<<"field1">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field2">> => #{ type=>list, subtype=>string, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false },
      <<"field3">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false },
      <<"field4">> => #{ type=>string, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false },
      index_storage => [ {ram,[<<"field1">>,<<"field2">>]},
        {ramdisc,[<<"field3">>]},
        {disc,[<<"field4">>]} ]
    },
  PID=erlang:unique_integer([positive,monotonic]),
  UInt=integer_to_binary(PID),
  Count=?GET(count,Config),
  %Values=term_to_binary([{I,I*30}||I<-lists:seq(1,300)]),
  OIDList=
    lists:foldl(fun(_,OIDs)->
      %io:format("create"),
      %ID=ets:update_counter(index_concurrent,id,1),
      ID=erlang:unique_integer([positive,monotonic]),
      ID1 = ID rem (1 bsl 16),
      OID={10005,ID1},
      Fields=ecomet_field:build_new(FieldsMap,#{
        <<"field1">>=><<"value1">>,
        <<"field2">>=>[<<"value2">>,<<"unique_value_",UInt/binary>>],
        <<"field3">>=><<"value3">>,
        <<"field4">>=><<"value4">>
      }),
      Tags=ecomet_index:build_back_tags(maps:to_list(Fields),FieldsMap),
      {_Merged,Log}=ecomet_index:merge_backtags(Tags,#{}),
      ecomet_backend:transaction(fun()->
        ecomet_index:dump_log(OID,Log)
        end), [OID|OIDs]
      end,[],lists:seq(1,Count)),
  ets:insert(index_concurrent,{PID,OIDList}).

concurrent_check(_Config)->
  {OIDList,PIDList}=
    ets:foldl(fun({PID,ProcessOIDs},{OIDs,PIDs})->
      {ProcessOIDs++OIDs,[PID|PIDs]}
    end,{[],[]},index_concurrent),

  % Intersecting tags
  Value1Tag={<<"field1">>,<<"value1">>,simple},
  Value2Tag={<<"field2">>,<<"value2">>,simple},
  Value3Tag={<<"field3">>,<<"value3">>,simple},
  Value4Tag={<<"field4">>,<<"value4">>,simple},

  [OID1|_] = OIDList,
  Db = ecomet_object:get_db_name(OID1),
  % Check intersecting
  check_tag(Value1Tag,Db,[ram],OIDList),
  check_tag(Value2Tag,Db,[ram],OIDList),
  check_tag(Value3Tag,Db,[ramdisc],OIDList),
  check_tag(Value4Tag,Db,[disc],OIDList),

  % Check unique
  lists:foreach(fun(PID)->
    [{PID,ProcessOIDs}]=ets:lookup(index_concurrent,PID),
    UInt=integer_to_binary(PID),
    UniqueTag={<<"field2">>,<<"unique_value_",UInt/binary>>,simple},
    check_tag(UniqueTag,Db,[ram],ProcessOIDs)
    end,PIDList).

