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
-module(ecomet_field_SUITE).

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


-export([
  build/1,
  build_fail/1,
  get_changes/1,
  object_creation/1,
  object_edit/1,
  dump_storages/1,
  merge/1,
  helpers/1,
  autoincrement_test/1,
  increment1/1,
  increment2/1,
  increment3/1
]).

all()->
  [
    {group,build_group},
    {group,save_group},
    merge,
    helpers,
    autoincrement_test,
    {group,increment_bencmark}
  ].

groups()->[
  {build_group,
    [sequence],
    [
      build,       % Build new fields
      build_fail   % Errors on field checks
    ]
  },
  {save_group,
    [sequence],
    [
      get_changes,
      {merge_storages,
        [sequence],
        [
          object_creation,
          object_edit
        ]
      },
      dump_storages
    ]
  },
  {increment_bencmark,
    [sequence],
    [
      {increment1_group,
        [parallel],
        [
          increment1,
          increment1,
          increment1,
          increment1,
          increment1,
          increment1,
          increment1,
          increment1
        ]
      },
      {increment2_group,
        [parallel],
        [
          increment2,
          increment2,
          increment2,
          increment2,
          increment2,
          increment2,
          increment2,
          increment2
        ]
      },
      {increment3_group,
        [parallel],
        [
          increment3,
          increment3,
          increment3,
          increment3,
          increment3,
          increment3,
          increment3,
          increment3
        ]
      }
    ]
  }
].

init_per_suite(Config)->
  ?BACKEND_INIT(),
  Config.

end_per_suite(_Config)->
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

%%--------------------------------------------------------------
%%  Build group
%%--------------------------------------------------------------
build(_Config)->
  OID = {<<"sys">>, 1},
  % {name,type,subtype,index,required,storage,default,autoincrement}
  StartT=erlang:monotonic_time(),
  ObjectDescription = #{
    <<"simple">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false),
    <<"required">> => ecomet_schema:field_description(string,none,none,true,ramdisc,none,false),
    <<"default1">> => ecomet_schema:field_description(string,none,none,true,ramdisc,<<"default1_string">>,false),
    <<"default2">> => ecomet_schema:field_description(string,none,none,true,ramdisc,<<"default2_string">>,false),
    <<"autoincrement1">> => ecomet_schema:field_description(string,none,none,true,ramdisc,none,true),
    <<"autoincrement2">> => ecomet_schema:field_description(string,none,none,true,ramdisc,none,true),
    <<"field1">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false),
    <<"field2">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false)
  },
  BuildMapT=erlang:monotonic_time(),
  io:format("build_map: ~p",[BuildMapT-StartT]),
  Fields = ecomet_field:build_new(ObjectDescription,
    #{
      <<".pattern">> => key1,
      <<"simple">> => <<"simple_value">>,
      <<"required">> => <<"required_value">>,
      <<"default1">> => <<"defined_value">>,   % set value for field with defined default
      <<"autoincrement1">> => <<"defined_increment">>, % set value for field with autoincrement
      <<"field1">> => <<"field1_value">>
    }
  ),
  % Project must contain all fields that defined in the map
  8=maps:size(Fields),
  BuildNewT=erlang:monotonic_time(),
  io:format("build_new: ~p",[BuildNewT-BuildMapT]),
  % Check defined values
  <<"simple_value">>=maps:get(<<"simple">>,Fields),
  <<"required_value">>=maps:get(<<"required">>,Fields),
  <<"defined_value">>=maps:get(<<"default1">>,Fields),
  <<"defined_increment">>=maps:get(<<"autoincrement1">>,Fields),
  <<"field1_value">>=maps:get(<<"field1">>,Fields),
  % Check simple field, that was not defined
  none=maps:get(<<"field2">>,Fields),
  % Check field with defined default value
  <<"default2_string">>=maps:get(<<"default2">>,Fields),
  % Autoincremented field
  Increment=maps:get(<<"autoincrement2">>,Fields),
  _=binary_to_integer(Increment),

  % Changes for defined fields
  {<<"simple_value">>,none}=ecomet_field:field_changes(ObjectDescription,Fields,OID,<<"simple">>),
  {<<"default2_string">>,none}=ecomet_field:field_changes(ObjectDescription,Fields,OID,<<"default2">>),
  {Increment,none}=ecomet_field:field_changes(ObjectDescription,Fields,OID,<<"autoincrement2">>),
  % No changes for undefined
  none=ecomet_field:field_changes(ObjectDescription,Fields,OID,<<"field2">>),

  ChecksT=erlang:monotonic_time(),
  io:format("checks: ~p",[ChecksT-BuildNewT]),
  % Change project
  Changed=ecomet_field:merge(ObjectDescription,Fields, #{<<"simple">> => <<"simple_value1">>}),

  MergeT=erlang:monotonic_time(),
  io:format("merge: ~p",[MergeT-ChecksT]),
  {<<"simple_value1">>,none}=ecomet_field:field_changes(ObjectDescription,Changed,OID,<<"simple">>).

build_fail(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  ObjectDescription = #{
    <<"simple">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false),
    <<"required">> => ecomet_schema:field_description(string,none,none,true,ramdisc,none,false),
    <<"default">> => ecomet_schema:field_description(string,none,none,true,ramdisc,<<"default1_string">>,false),
    <<"autoincrement">> => ecomet_schema:field_description(string,none,none,true,ramdisc,none,true)
  },
  % Invalid value
  ?assertError(
    invalid_value,
    ecomet_field:build_new(ObjectDescription,
      #{
        <<".pattern">> => key1,
        <<"simple">> => not_string,
        <<"required">> => <<"required_value">>
      }
    )
  ),
  % Undefined required field
  ?assertError(
    required_field,
    ecomet_field:build_new(ObjectDescription,
      #{
        <<".pattern">> => key1,
        <<"simple">> => <<"simple_value">>
      }
    )
  ),
  % OK
  ecomet_field:build_new(ObjectDescription,
    #{
      <<".pattern">> => key1,
      <<"simple">> => <<"simple_value">>,
      <<"required">> => <<"required_value">>
    }
  ).

%%--------------------------------------------------------------
%% Save group
%%--------------------------------------------------------------
get_changes(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  ObjectDescription = #{
    <<"ram_field1">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ram_field2">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ramdisc_field">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false),
    <<"disc_field1">> => ecomet_schema:field_description(string,none,none,false,disc,none,false),
    <<"disc_field2">> => ecomet_schema:field_description(string,none,none,false,disc,none,false),
    <<"disc_field3">> => ecomet_schema:field_description(string,none,none,false,disc,none,false)
  },
  % Empty project
  Empty=ecomet_field:get_changes([],ObjectDescription,#{}),
  0=maps:size(Empty),
  % Fill project
  Fields=ecomet_field:build_new(ObjectDescription,
    #{
      <<".pattern">> => key1,
      <<"ram_field1">> => <<"ram_value1">>,
      <<"ramdisc_field">> => <<"ramdisc_value">>,
      <<"disc_field1">> => <<"disc_value1">>,
      <<"disc_field3">> => <<"disc_value3">>
    }
  ),
  Storages=ecomet_field:get_changes(maps:to_list(Fields),ObjectDescription,#{}),
  3=maps:size(Storages),
  % Ram storage
  Ram=maps:get(ram,Storages),
  2=maps:size(Ram),
  <<"ram_value1">> =maps:get(<<"ram_field1">>,Ram),
  none=maps:get(<<"ram_field2">>,Ram),
  % Ramdisc storage
  Ramdisc=maps:get(ramdisc,Storages),
  1=maps:size(Ramdisc),
  <<"ramdisc_value">> =maps:get(<<"ramdisc_field">>,Ramdisc),
  % Disc storage
  Disc=maps:get(disc,Storages),
  3=maps:size(Disc),
  <<"disc_value1">> =maps:get(<<"disc_field1">>,Disc),
  none=maps:get(<<"disc_field2">>,Disc),
  <<"disc_value3">> =maps:get(<<"disc_field3">>,Disc).

object_creation(_Config)->
  OID={<<"sys">>,10000},
  Preloaded=#{
    ram=>#{},
    ramdisc=>#{},
    disc=>#{}
  },
  % {name,type,subtype,index,required,storage,default,autoincrement}
  ObjectDescription = #{
    <<"ram_field1">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ram_field2">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ram_field3">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ram_field4">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ramdisc_field">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false),
    <<"disc_field1">> => ecomet_schema:field_description(string,none,none,false,disc,none,false),
    <<"disc_field2">> => ecomet_schema:field_description(string,none,none,false,disc,none,false),
    <<"disc_field3">> => ecomet_schema:field_description(string,none,none,false,disc,none,false)
  },
  %-----------------------------------------------------------
  % Scenario 1. Single storage changed
  %-----------------------------------------------------------
  S1_Fields=ecomet_field:build_new(ObjectDescription,
    #{
      <<".pattern">> => key1,
      <<"ram_field1">> => <<"ram_value1">>,
      <<"ram_field4">> => <<"ram_value4">>
    }
  ),
  S1_changes=ecomet_field:get_changes(maps:to_list(S1_Fields),ObjectDescription,#{}),
  {S1_Merged,S1_FieldsChanges}=ecomet_field:merge_storages(maps:to_list(S1_changes),Preloaded,OID,{#{},[]}),
  % Only one storage changed
  1=maps:size(S1_Merged),
  S1_Ram=maps:get(ram,S1_Merged),
  % Storage contains 2 fields
  2=maps:size(S1_Ram),
  <<"ram_value1">> =maps:get(<<"ram_field1">>,S1_Ram),
  <<"ram_value4">> =maps:get(<<"ram_field4">>,S1_Ram),
  % 2 fields has changes, but on creation project contains all fields defined in the Map
  8=length(S1_FieldsChanges),
  <<"ram_value1">>=proplists:get_value(<<"ram_field1">>,S1_FieldsChanges),
  <<"ram_value4">>=proplists:get_value(<<"ram_field4">>,S1_FieldsChanges),
  none=proplists:get_value(<<"disc_field1">>,S1_FieldsChanges),
  %-----------------------------------------------------------
  % Scenario 2. All storages changed
  %-----------------------------------------------------------
  S2_Fields=ecomet_field:build_new(ObjectDescription,
    #{
      <<".pattern">> => key2,
      <<"ram_field2">> => <<"ram_value2">>,
      <<"ram_field3">> => <<"ram_value3">>,
      <<"ramdisc_field">> => <<"ramdisc_value">>,
      <<"disc_field2">> => <<"disc_value2">>
    }
  ),
  S2_changes=ecomet_field:get_changes(maps:to_list(S2_Fields),ObjectDescription,#{}),
  {S2_Merged,S2_FieldsChanges}=ecomet_field:merge_storages(maps:to_list(S2_changes),Preloaded,OID,{#{},[]}),

  % 3 storages changed
  3=maps:size(S2_Merged),

  % Ram
  S2_Ram=maps:get(ram,S2_Merged),
  % Ram storage contains 2 fields
  2=maps:size(S2_Ram),
  <<"ram_value2">> =maps:get(<<"ram_field2">>,S2_Ram),
  <<"ram_value3">> =maps:get(<<"ram_field3">>,S2_Ram),

  % Ramdisc
  S2_Ramdisc=maps:get(ramdisc,S2_Merged),
  % Ramdisc storage contains 1 field
  1=maps:size(S2_Ramdisc),
  <<"ramdisc_value">> =maps:get(<<"ramdisc_field">>,S2_Ramdisc),

  % Disc
  S2_Disc=maps:get(disc,S2_Merged),
  % Disc storage contains 1 field
  1=maps:size(S2_Disc),
  <<"disc_value2">> =maps:get(<<"disc_field2">>,S2_Disc),

  % 4 fields has changes
  8=length(S2_FieldsChanges),
  <<"ram_value2">>=proplists:get_value(<<"ram_field2">>,S2_FieldsChanges),
  <<"ram_value3">>=proplists:get_value(<<"ram_field3">>,S2_FieldsChanges),
  <<"ramdisc_value">>=proplists:get_value(<<"ramdisc_field">>,S2_FieldsChanges),
  <<"disc_value2">>=proplists:get_value(<<"disc_field2">>,S2_FieldsChanges).

object_edit(_Config)->
  OID={1, 42},
  % {name,type,subtype,index,required,storage,default,autoincrement}
  ObjectDescription = #{
    <<"ram_field1">> => ecomet_schema:field_description(string,none,none,false,ram,none,fals),
    <<"ram_field2">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ram_field3">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ram_field4">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ramdisc_field">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false),
    <<"disc_field1">> => ecomet_schema:field_description(string,none,none,false,disc,none,false),
    <<"disc_field2">> => ecomet_schema:field_description(string,none,none,false,disc,none,false),
    <<"disc_field3">> => ecomet_schema:field_description(string,none,none,false,disc,none,false)
  },

  %-----------------------------------------------------------
  % Scenario 1. Storages preloaded
  %-----------------------------------------------------------
  Preloaded=#{
    ram=>#{<<"ram_field1">> => <<"ram_value1">>,<<"ram_field2">> => <<"ram_value2">> },
    ramdisc=>#{<<"ramdisc_field">> => <<"ramdisc_value">>},
    disc=>none
  },
  object_edit(OID,ObjectDescription,Preloaded),

  ok.


object_edit(OID,ObjectDescription,Preloaded)->
  %-----------------------------------------------------------
  % Scenario 1. Single storage changes
  %-----------------------------------------------------------
  S1_Fields=ecomet_field:merge(ObjectDescription,#{},
    #{
      <<"ram_field1">> => <<"ram_value1_new">>,
      <<"ram_field2">> => none,                 % Field will be deleted from storage
      <<"ram_field3">> => <<"ram_value3">>,     % Field will be added to storage
      <<"ramdisc_field">> => <<"ramdisc_value">>
    }
  ),
  S1_changes=ecomet_field:get_changes(maps:to_list(S1_Fields),ObjectDescription,#{}),
  {S1_Merged,S1_FieldsChanges}=ecomet_field:merge_storages(maps:to_list(S1_changes),Preloaded,OID,{#{},[]}),
  % Only ram storage has real changes, ramdisc_field got the same value
  1=maps:size(S1_Merged),
  S1_Ram=maps:get(ram,S1_Merged),
  % Ram storage contains 2 fields
  2=maps:size(S1_Ram),
  <<"ram_value1_new">> =maps:get(<<"ram_field1">>,S1_Ram),
  <<"ram_value3">> =maps:get(<<"ram_field3">>,S1_Ram),
  % 3 fields has real changes
  3=length(S1_FieldsChanges),
  <<"ram_value1_new">> =proplists:get_value(<<"ram_field1">>,S1_FieldsChanges),
  none=proplists:get_value(<<"ram_field2">>,S1_FieldsChanges),
  <<"ram_value3">> =proplists:get_value(<<"ram_field3">>,S1_FieldsChanges),
  %-----------------------------------------------------------
  % Scenario 2. All storages has changes
  %-----------------------------------------------------------
  S2_Fields=ecomet_field:merge(ObjectDescription,#{},
    #{
      <<"ram_field1">> => <<"ram_value1_new">>,
      <<"ramdisc_field">> => none,                % Storage will be marked for deleting (empty map)
      <<"disc_field1">> => <<"disc_value1">>     % Storage will be added
    }
  ),
  S2_changes=ecomet_field:get_changes(maps:to_list(S2_Fields),ObjectDescription,#{}),
  {S2_Merged,S2_FieldsChanges}=ecomet_field:merge_storages(maps:to_list(S2_changes),Preloaded,OID,{#{},[]}),
  % 3 Storages has changes
  3=maps:size(S2_Merged),

  % Ram
  S2_Ram=maps:get(ram,S2_Merged),
  % Ram storage contains 2 fields
  2=maps:size(S2_Ram),
  <<"ram_value1_new">> =maps:get(<<"ram_field1">>,S2_Ram),
  <<"ram_value2">> =maps:get(<<"ram_field2">>,S2_Ram),

  % Ramdisc
  S2_Ramdisc=maps:get(ramdisc,S2_Merged),
  % Ramdisc storage is empty, it will be deleted on dump step
  0=maps:size(S2_Ramdisc),

  % Disc
  S2_Disc=maps:get(disc,S2_Merged),
  % Disc storage contains 1 field
  1=maps:size(S2_Disc),
  <<"disc_value1">> =maps:get(<<"disc_field1">>,S2_Disc),

  % 3 fields has real changes
  3=length(S2_FieldsChanges),
  <<"ram_value1_new">> =proplists:get_value(<<"ram_field1">>,S2_FieldsChanges),
  none=proplists:get_value(<<"ramdisc_field">>,S2_FieldsChanges),
  <<"disc_value1">> =proplists:get_value(<<"disc_field1">>,S2_FieldsChanges).


dump_storages(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  ObjectDescription = #{
    <<"ram_field1">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ram_field2">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ramdisc_field">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false),
    <<"disc_field1">> => ecomet_schema:field_description(string,none,none,false,disc,none,false),
    <<"disc_field2">> => ecomet_schema:field_description(string,none,none,false,disc,none,false)
  },
  Fields=ecomet_field:build_new(ObjectDescription,
    #{
      <<".pattern">> => key2,
      <<"ram_field1">> => <<"ram_value1">>,
      <<"disc_field1">> => <<"disc_value1">>,
      <<"disc_field2">> => <<"disc_value2">>
    }
  ),
  OID={1, 42},
  Changes=ecomet_field:get_changes(maps:to_list(Fields),ObjectDescription,#{}),
  {Storages,_}=ecomet_field:merge_storages(maps:to_list(Changes),#{},OID,{#{},[]}),
  2=maps:size(Storages),

  %-----------------------------------------------------------------------------------------------------
  % The dumping is not any more performed by the ecomet_field module,
  % it is totally under ecomet_object control now.
  % The following is a legacy code. It is going to be purged soon
  %-----------------------------------------------------------------------------------------------------
%%  {ok, _} = ecomet_transaction:internal(fun()->ecomet_field:dump_storages(maps:to_list(Storages),OID) end),
%%  % Ram
%%  DB = ecomet_object:get_db_name(OID),
%%  ct:pal("DB Name = ~p", [DB]),
%%  Ram=ecomet_backend:dirty_read(DB,data,ram,{OID,fields},false),
%%  io:format("Ram: ~p",[Ram]),
%%  1=maps:size(Ram),
%%  <<"ram_value1">> =maps:get(<<"ram_field1">>,Ram),
%%  % Ramdisc, no fields
%%  not_found=ecomet_backend:dirty_read(DB,data,ramdisc,{OID,fields},false),
%%  % Disc
%%  Disc=ecomet_backend:dirty_read(DB,data,disc,{OID,fields},false),
%%  2=maps:size(Disc),
%%  <<"disc_value1">> =maps:get(<<"disc_field1">>,Disc),
%%  <<"disc_value2">> =maps:get(<<"disc_field2">>,Disc),
%%
%%  % lookup_value checks
%%  <<"ram_value1">> =ecomet_field:lookup_storage(ram,OID,<<"ram_field1">>),
%%  none=ecomet_field:lookup_storage(ram,OID,<<"ram_field2">>),
%%  none=ecomet_field:lookup_storage(ramdisc,OID,<<"ramdisc_field">>),
%%  <<"disc_value1">> =ecomet_field:lookup_storage(disc,OID,<<"disc_field1">>),
%%
%%  % get_value checks
%%  {ok,<<"ram_value1">>}=ecomet_field:get_value(ObjectDescription,OID,<<"ram_field1">>),
%%  {ok,none}=ecomet_field:get_value(ObjectDescription,OID,<<"ramdisc_field">>),
%%  {ok,none}=ecomet_field:get_value(ObjectDescription,OID,<<"ram_field2">>),
%%
%%  {error,undefined_field}=ecomet_field:get_value(ObjectDescription,OID,<<"invalid_field">>),
%%
%%  % Delete object
%%  {ok, _} = ecomet_transaction:internal(fun()->ecomet_field:delete_object(ObjectDescription,OID) end),
%%  not_found=ecomet_backend:dirty_read(DB,data,ram,{OID,fields},false),
%%  not_found=ecomet_backend:dirty_read(DB,data,ramdisc,{OID,fields},false),
%%  not_found=ecomet_backend:dirty_read(DB,data,disc,{OID,fields},false).
  ok.


%%--------------------------------------------------------------
%% Update fields values
%%--------------------------------------------------------------
merge(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  ObjectDescription = #{
    <<"ram_field">> => ecomet_schema:field_description(string,none,none,false,ram,none,false),
    <<"ram_required">> => ecomet_schema:field_description(string,none,none,true,ram,none,false),
    <<"ramdisc_field">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false),
    <<"disc_field1">> => ecomet_schema:field_description(string,none,none,false,disc,none,false),
    <<"disc_field2">> => ecomet_schema:field_description(string,none,none,false,disc,none,false),
    <<"disc_field3">> => ecomet_schema:field_description(string,none,none,false,disc,none,false)
  },
  % Fill project
  Fields = ecomet_field:build_new(ObjectDescription,
    #{
      <<".pattern">> => key1,
      <<"ram_field">> => <<"ram_value">>,
      <<"ram_required">> => <<"required_value">>,
      <<"ramdisc_field">> => <<"ramdisc_value">>,
      <<"disc_field1">> => <<"disc_value1">>,
      <<"disc_field3">> => <<"disc_value3">>
    }
  ),
  Merged=ecomet_field:merge(ObjectDescription, Fields,
    #{
      <<"ram_field">> => <<"ram_value_new">>,
      <<"ram_required">> => <<"required_value_new">>
    }
  ),
  % Check new values
  <<"ram_value_new">>=maps:get(<<"ram_field">>,Merged),
  <<"required_value_new">>=maps:get(<<"ram_required">>,Merged),
  % Check unchanged
  <<"ramdisc_value">>=maps:get(<<"ramdisc_field">>,Merged),
  <<"disc_value1">>=maps:get(<<"disc_field1">>,Merged),

  % We can not delete required field
  ?assertError(
    required_field,
    ecomet_field:merge(ObjectDescription, Fields, #{<<"ram_required">> => none})
  ),

  ok.

%%--------------------------------------------------------------
%%  Helper functions
%%--------------------------------------------------------------
helpers(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  ObjectDescription = #{
    <<"simple">> => ecomet_schema:field_description(string,none,[simple],false,ramdisc,none,false),
    <<"list">> => ecomet_schema:field_description(list,string,none,false,ramdisc,none,false),
    <<"required">> => ecomet_schema:field_description(string,none,none,true,ramdisc,none,false),
    <<"disc">> => ecomet_schema:field_description(string,none,[simple],false,disc,none,false),
    <<"default">> => ecomet_schema:field_description(string,none,none,false,ramdisc,<<"default_string">>,false)
  },
  % type
  {ok,string}=ecomet_field:get_type(ObjectDescription,<<"simple">>),
  {ok,{list,string}}=ecomet_field:get_type(ObjectDescription,<<"list">>),
  {error,undefined_field}=ecomet_field:get_type(ObjectDescription,<<"invalid_field">>),
  % index
  {ok,[simple]}=ecomet_field:get_index(ObjectDescription,<<"simple">>),
  {ok,none}=ecomet_field:get_index(ObjectDescription,<<"list">>),
  % required
  {ok,true}=ecomet_field:is_required(ObjectDescription,<<"required">>),
  {ok,false}=ecomet_field:is_required(ObjectDescription,<<"simple">>),
  % storage
  {ok,ramdisc}=ecomet_field:get_storage(ObjectDescription,<<"simple">>),
  {ok,disc}=ecomet_field:get_storage(ObjectDescription,<<"disc">>).

%%--------------------------------------------------------------
%%  Autoincrement fields
%%--------------------------------------------------------------
autoincrement_test(_Config)->
  % {name,type,subtype,index,required,storage,default,autoincrement}
  Description = #{
    <<"simple">> => ecomet_schema:field_description(string,none,none,false,ramdisc,none,false),
    <<"autoincrement">> => ecomet_schema:field_description(string,none,none,true,ramdisc,none,true)
  },
  ParentPid = self(),
  SmallCnt = 10,
  BigCnt = 1,
  MySmallList = autoinc_list(Description, SmallCnt),
  erlang:spawn(
    fun() ->
      ParentPid!{neighbor, autoinc_list(Description, BigCnt)}
    end
  ),
  receive
    {neighbor, NeighborList} when is_list(NeighborList) ->
      ?assertEqual(
        (SmallCnt + BigCnt),
        erlang:length(lists:usort(NeighborList ++ MySmallList))
      )
  after 10000 ->
    erlang:error(test_timeout)
  end.

autoinc_list(Description, Count) ->
  lists:reverse(lists:foldl(
    fun(_, Res)->
      #{<<"autoincrement">> := Value} = ecomet_field:build_new(Description,
        #{
          <<".pattern">> => key1,
          <<"simple">> => <<"simple_value">>
        }
      ),
      [binary_to_integer(Value)|Res]
    end,
    [],
    lists:seq(1, Count)
  )).

increment1(_Config)->
  lists:foreach(fun(_)->
    erlang:unique_integer([positive])
  end,lists:seq(1,1000000)).

increment2(_Config)->
  lists:foreach(fun(_)->
    erlang:unique_integer([positive,monotonic])
  end,lists:seq(1,1000000)).

increment3(_Config)->
  lists:foreach(fun(_)->
    ecomet_schema:local_increment(test_key)
  end,lists:seq(1,1000000)).
