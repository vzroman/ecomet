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
-module(ecomet_object_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([all/0, group/0, init_per_suite/1, end_per_suite/1]).

-export([
  on_create_test/1,
  on_edit_test/1,
  on_delete_test/1,
  check_storage_type_test/1,
  check_path_test/1,
  edit_rights_test/1,
  check_db_test/1

]).

all() ->
  [
    check_storage_type_test,
    check_path_test,
    edit_rights_test,
    check_db_test,
    on_create_test,
    on_edit_test,
    on_delete_test
  ]
.

group() ->
  []
.

init_per_suite(Config)->
  ?BACKEND_INIT(),
  Config.
end_per_suite(_Config)->
  ?BACKEND_STOP(30000),
  ok.

check_storage_type_test(_Config) ->
  meck:new([ecomet, ecomet_pattern]),
  meck:expect(ecomet, read_field,
    fun(Object, Field) ->
      {ok, maps:get(Field, element(2, Object))}
    end),
  meck:expect(ecomet_pattern, get_storage, fun(Object) -> element(2, Object) end),

  % Create meck object %
  % ?RAM, ?RAMLOCAL, ?RAMDISC, ?DISC%
  % {
  %    ?RAM,
  %   #{<<".folder">> => {?RAM, <<"nomatter">>}},
  % },
  % <<.folder>> contain FolderStorage

  % Persistent folders can contain any types of objects %
  % Persistent means FolderStorage != ?RAM %
  ok = ecomet_object:check_storage_type({ ?RAM, #{<<".folder">> => {?DISC, none}}}),
  ok = ecomet_object:check_storage_type({ ?RAMDISC, #{<<".folder">> => {?RAMLOCAL, none}} }),

  % Ram only folders cannot contain persistent objects %
  % It means in case FolderStorage == ?RAM, object storage could be only ?RAM%
  ok = ecomet_object:check_storage_type({ ?RAM, #{<<".folder">> => {?RAM, none}} }),
  ?assertError(ram_only_folder, ecomet_object:check_storage_type({ ?DISC, #{<<".folder">> => {?RAM, none}} })),

  meck:unload([ecomet, ecomet_pattern]),

  ok.

check_path_test(_Config) ->
  ecomet_user:on_init_state(),
  Object = ecomet:create_object(#{
    <<".name">> => <<"Forest">>,
    <<".folder">> => {2, 1},
    <<".pattern">> => {3, 2}
  }),

  % We are trying to create object with invalid name %
  ?assertError({"the '/' symbol is not allowed in names", _},
    ecomet:edit_object(Object, #{<<".name">> => <<"Se/verus">>})),

  Object2 = ecomet:create_object(#{
    <<".name">> => <<"Forest">>,
    <<".folder">> => {2, 2},
    <<".pattern">> => {3, 2}
  }),

  ?assertError({{not_unique,<<"Forest">>}, _}, ecomet:edit_object(Object, #{<<".folder">> => {2, 2}})),
  ok = ecomet:delete_object(Object2),
  ok = ecomet:edit_object(Object, #{<<".folder">> => {2, 2}}),

  % Every object must have unique name %
  ?assertError({{not_unique,<<"Forest">>}, _}, ecomet:create_object(#{
    <<".name">> => <<"Forest">>,
    <<".folder">> => {2, 2},
    <<".pattern">> => {3, 2}
  })),

  ok = ecomet:delete_object(Object),
  ok.

edit_rights_test(_Config) ->
  ecomet_user:on_init_state(),
  Object = ecomet:create_object(#{
    <<".name">> => <<"Forest">>,
    <<".folder">> => {2, 1},
    <<".pattern">> => {3, 2},
    <<".writegroups">> => [],
    <<".readgroups">> => []
  }),
  ecomet:edit_object(Object, #{<<".writegroups">> => [{1, 2}], <<".readgroups">> => [{2, 3}]}),
  {ok, WriteG} = ecomet_object:read_field(Object, <<".writegroups">>),
  {ok, ReadG} = ecomet_object:read_field(Object, <<".readgroups">>),
  % ReadGroups has to contain WriteGroups %
  [] = WriteG -- ReadG,
  ok = ecomet:delete_object(Object),
  ok
.


check_db_test(_Config) ->
  ecomet_user:on_init_state(),
  Folder1 = ecomet:create_object(#{
    <<".name">> => <<"Forest">>,
    <<".folder">> => ?OID(<<"/root">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),
  Folder2 = ecomet:create_object(#{
    <<".name">> => <<"Folder1">>,
    <<".folder">> => ?OID(<<"/root">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),
  RealDB = ecomet:create_object(#{
    <<".name">> => <<"RealDB">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.database">>)
  }),
  Folder3 = ecomet:create_object(#{
    <<".name">> => <<"Folder2">>,
    <<".folder">> => ?OID(<<"/root">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<"database">> => ?OID(RealDB)
  }),
  'RealDB' = ecomet_schema:get_mounted_db(?OID(Folder3)),
  ct:pal("RealDB ID ~n~p~n",[ecomet_schema:get_db_id('RealDB')]),
  ct:pal("Folder3 dbID ~n~p~n",[ecomet_folder:get_db_id(?OID(Folder3))]),
  ct:pal("FOlder2 dbID ~n~p~n",[ecomet_folder:get_db_id(?OID(Folder2))]),
  TestObject = ecomet:create_object(#{
    <<".name">> => <<"Test">>,
    <<".folder">> => ?OID(Folder1),
    <<".pattern">> => ?OID(<<"/root/.patterns/.object">>)
  }),

  ok = ecomet:edit_object(TestObject, #{<<".folder">> => ?OID(Folder2)}),

  % TODO. Create a real database and mount it to some folder.
  % then try to move the object to this new folder
  ?assertError({different_database, _}, ecomet:edit_object(TestObject, #{<<".folder">> => ?OID(Folder3)})),
  ecomet:delete_object(Folder1),
  ecomet:delete_object(Folder2),
  ecomet:delete_object(Folder3),
  ecomet:delete_object(RealDB),
  ok
.

on_create_test(_Config) ->

  ecomet_user:on_init_state(),

  Object = ecomet:create_object(#{
    <<".name">> => <<"Simple">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.database">>),
    <<".readgroups">> => [{2, 3}],
    <<".writegroups">> => [{4, 5}]
  }),
  {ok, Writeg=[{4, 5}]} = ecomet_object:read_field(Object, <<".writegroups">>),
  {ok, Readg=[{2, 3},{4, 5}]} = ecomet_object:read_field(Object, <<".readgroups">>),

  ct:pal("Why ~p ~p", [Readg, Writeg]),
  ok = ecomet:edit_object(Object, #{<<".writegroups">> => [{1, 2}, {4, 3}], <<".readgroups">> => [{2 ,3}]}),
  {ok, WriteG} = ecomet_object:read_field(Object, <<".writegroups">>),
  {ok, ReadG} = ecomet_object:read_field(Object, <<".readgroups">>),

  [] = WriteG -- ReadG,
  {ok, Time} = ecomet_object:read_field(Object, <<".ts">>),
  true = is_integer(Time),
  ct:pal("time ~p", [Time]),


  ?assertError({{not_unique,<<"Simple">>} , _}, ecomet:create_object(#{
    <<".name">> => <<"Simple">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.database">>)
  })),

  ok = ecomet:delete_object(Object),
  ok
.

on_edit_test(_Config) ->
  ecomet_user:on_init_state(),

  Object = ecomet:create_object(#{
    <<".name">> => <<"OptimusPrime">>,
    <<".folder">> => {2, 1},
    <<".pattern">> => {3, 2},
    <<".readgroups">> => [{2, 3}],
    <<".writegroups">> => [{4, 5}]
  }),

  ?assertError({"the '/' symbol is not allowed in names", _},
    ecomet:edit_object(Object, #{<<".name">> => <<"Optimus/Prime">>})),
  ?assertError({can_not_change_pattern, _}, ecomet:edit_object(Object, #{<<".pattern">> => {3, 1}})),
  ?assertError({can_not_change_ts, _}, ecomet:edit_object(Object, #{<<".ts">> => erlang:system_time(nano_seconds)})),

  ok = ecomet:delete_object(Object),
  ok
.

on_delete_test(_Config) ->

  ecomet_user:on_init_state(),
  Object = ecomet:create_object(#{
    <<".name">> => <<".OptimusPrime">>,
    <<".folder">> => {2, 1},
    <<".pattern">> => {3, 2},
    <<".readgroups">> => [{2, 3}],
    <<".writegroups">> => [{4, 5}]
  }),
  % We cannot delete system object %
  ?assertError({system_object, _}, ecomet:delete_object(Object)),
  ok
.
