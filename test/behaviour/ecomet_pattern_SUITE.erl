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
-module(ecomet_pattern_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").
-include_lib("eunit/include/eunit.hrl").


%% API
-export([all/0, group/0, init_per_suite/1, end_per_suite/1]).


-export([
  check_handler_module_test/1,
  check_handler_test/1,
  check_parent_test/1,
  check_db_test/1,
  get_parent_test/1,
  get_parents_test/1,
  set_parents_test/1,
  on_delete_test/1,
  on_edit_test/1,
  on_create_test/1
]).


all() ->
  [
    check_handler_module_test,
    check_handler_test,
    check_parent_test,
    check_db_test,
    get_parent_test,
    get_parents_test,
    set_parents_test,
    on_delete_test,
    on_edit_test,
    on_create_test
  ].

group() ->
  [].



init_per_suite(Config)->
  ?BACKEND_INIT(),
  Config.
end_per_suite(_Config)->
  ?BACKEND_STOP(30000),
  ok.


check_handler_module_test(_Config) ->
  % Module 'valid_mod' does not exist, error should occur %
  ?assertError(invalid_module, ecomet_pattern:check_handler_module(valid_mod)),

  % 'ecomet_query' - our module, but does not contain necessary functions %
  ?assertError(undefined_on_create, ecomet_pattern:check_handler_module(ecomet_query)),

  % 'ecomet_db' - module of our project and contain all needed functions %
  ok = ecomet_pattern:check_handler_module(ecomet_db),
  ok
.

% Object == #{behaviour_module => {NewValue, OldValue}} %
% Other fields are not important in this case %
check_handler_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We do not change <<behaviour_module>>, should return ok
  ok = ecomet_pattern:check_handler(#{ 1 => 2, {old, new} => {new, old} }),
  ok = ecomet_pattern:check_handler(#{ 1.5 => 4.2, <<"old">> => <<"new">> }),

  % Changing <<behaviour_module>> to none, should return ok %
  ok = ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {none, ecomet_db} }),
  ok = ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {none, ecomet_node}, 1 => 2 }),

  % Changing <<behaviour_module>> to invalid module or
  % module does not contain on_edit, on_create, on_delete functions %
  ?assertError(invalid_module, ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {invalid_module, none}})),
  ?assertError(undefined_on_create, ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {ecomet_query, none}})),

  % Valid module with on_create, on_edit, on_delete functions %
  ok = ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {ecomet_field, none}}),
  meck:unload(ecomet)
.

check_db_test(_Config) ->
  ecomet_user:on_init_state(),

  Folder1 = ecomet:create_object(#{
    <<".name">> => <<"folder1">>,
    <<".folder">> => ?OID(<<"/root">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),
  {ok, Fold1} = ecomet:read_field(Folder1, <<".folder">>),
  ct:pal("DB folder1 name ~n~p ~p~n",[Fold1, ecomet_object:get_db_name(Fold1)]),
  DB = ecomet:create_object(#{
    <<".name">> => <<"DB">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.database">>)
  }),
  Folder2 = ecomet:create_object(#{
    <<".name">> => <<"folder2">>,
    <<".folder">> => ?OID(Folder1),
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<"database">> => ?OID(DB),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),
  {ok, Fold2} = ecomet:read_field(Folder2, <<".folder">>),
  ct:pal("DB folder2 name ~n~p ~p~n",[Fold2, ecomet_object:get_db_name(Fold2)]),
  Folder3 = ecomet:create_object(#{
    <<".name">> => <<"folder3">>,
    <<".folder">> => ?OID(Folder2),
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.object">>)
  }),
  {ok, Fold3} = ecomet:read_field(Folder3, <<".folder">>),
  io:fwrite("~n~p~n",[Fold3]),
  ct:pal("DB folder3 name ~n~p ~p~n",[Fold3, ecomet_object:get_db_name(Fold3)]),
  io:fwrite("~n~p~n",[Folder3]),
  io:fwrite("~n~p~n",[ecomet_pattern:get_map(Folder3)]),
  ecomet:delete_object(Folder1),
  ecomet:delete_object(DB),
  ok.

get_parent_test(_Config) ->
  meck:new([ecomet, ecomet_lib]),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  meck:expect(ecomet_lib, to_object_system, fun(Pattern) -> Pattern end),

  pattern = ecomet_pattern:get_parent(#{<<"parent_pattern">> => pattern}),

  meck:unload([ecomet, ecomet_lib])
.

get_parents_test(_Config) ->
  meck:new([ecomet, ecomet_lib]),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  meck:expect(ecomet_lib, to_object_system, fun(Pattern) -> Pattern end),

  parents = ecomet_pattern:get_parents(#{<<"parents">> => parents}),

  meck:unload([ecomet, ecomet_lib])
.

set_parents_test(_Config) ->
  meck:new([ecomet, ecomet_lib], [no_link]),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  meck:expect(ecomet, edit_object, fun(_Object, Fields) -> put(side_effect, Fields), ok end),
  meck:expect(ecomet_lib, to_object_system, fun(Pattern) -> Pattern end),

  Object1 = #{<<"parent_pattern">> => #{<<"parents">> => [a, b, c]} },
  Exp = #{<<"parents">> => [#{<<"parents">> => [a, b, c]}, a, b, c]},
  ecomet_pattern:set_parents(Object1),
  Exp = get(side_effect),

  meck:unload([ecomet, ecomet_lib]),

  ok.

% Object == #{parent_pattern => Smth} OR #{} %
% Other fields are not important in this case %
check_parent_test(_Config) ->
  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Field) -> maps:get(Field, Object, none) end),

  % We aren`t changing parent, should return ok %
  ok = ecomet_pattern:check_parent(#{ 1 => 2, 3 => 4, 5 => 6 }),
  ok = ecomet_pattern:check_parent(#{ <<"Key">> => value, <<"value">> => key }),

  % We are changing parent field, error must occur %
  ?assertError(cannot_change_parent, ecomet_pattern:check_parent(#{ <<"parent_pattern">> => {{1, 2}, {3, 4}} })),
  ?assertError(cannot_change_parent, ecomet_pattern:check_parent(#{ <<"parent_pattern">> => {{1, 4}, {123, 100}} })),

  meck:unload(ecomet),
  ok
.

on_create_test(_Config) ->
  ecomet_user:on_init_state(),

  ct:pal("FolderID /root/CustomPath ~n~p~n~p~n~p~n~p~n" , [
    ?OID(<<"/root">>),            %{2, 1}
    ?OID(<<"/root/.patterns">>),  %{2, 2}
    ?OID(<<"/root/.nodes">>),     %{2, 3}
    ?OID(<<"/root/.databases">>)  %{2, 4}
  ]),
  ct:pal("PatternID /root/.patterns/.object ~n~p~n~p~n~p~n~p~n~p~n~p~n~p~n", [
    ?OID(<<"/root/.patterns/.object">>),  % {3, 1}
    ?OID(<<"/root/.patterns/.folder">>),  % {3, 2}
    ?OID(<<"/root/.patterns/.pattern">>), % {3, 3}
    ?OID(<<"/root/.patterns/.field">>),   % {3, 4}
    ?OID(<<"/root/.patterns/.database">>),% {3, 5}
    ?OID(<<"/root/.patterns/.segment">>), % {3, 6}
    ?OID(<<"/root/.patterns/.node">>)     % {3, 7}
  ]),

  Object = ecomet:create_object(#{
    <<".name">> => <<"STALKER">>,
    <<".folder">> => ?OID(<<"/root/.patterns">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),

  ct:pal("Object ~p, ", [Object]),
  {ok, ParentPat} = ecomet:read_field(Object, <<"parent_pattern">>),
  {ok, Parents} = ecomet:read_field(Object, <<"parents">>),
  % <<parents>> contains <<parent_patterns>> %
  [] = [ParentPat] -- Parents,

  % <<behaviour_module>> field contain invalid module%
  ?assertError({invalid_module, _}, ecomet:create_object(#{
    <<".name">> => <<"ClearSky">>,
    <<".folder">> => ?OID(<<"/root/.patterns">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<"behaviour_module">> => qwe
  })),

  % <<behaviour_module>> field contain valid module,
  % but module does not contain on_create function
  ?assertError({undefined_on_create, _}, ecomet:create_object(#{
    <<".name">> => <<"ClearSky">>,
    <<".folder">> => ?OID(<<"/root/.patterns">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<"behaviour_module">> => ecomet_query
  })),

  % <<behaviour_module>> field contain valid module,
  % and module contain {on_create, on_edit, on_delete} functions
  Object1 = ecomet:create_object(#{
    <<".name">> => <<"Jaina">>,
    <<".folder">> => ?OID(<<"/root/.patterns">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<"behaviour_module">> => ecomet_db
  }),

  ecomet:delete_object(Object),
  ecomet:delete_object(Object1),

  ok
.


on_edit_test(_Config) ->
  ecomet_user:on_init_state(),
  Object = ecomet:create_object(#{
    <<".name">> => <<"Strelok">>,
    <<".folder">> => ?OID(<<"/root/.patterns">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<"behaviour_module">> => ecomet_db
  }),
  % We cannot change <<parent_pattern>> field %
  ?assertError({cannot_change_parent, _}, ecomet:edit_object(Object, #{
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.node">>)
  })),

  % We changing <<behaviour_module>> to invalid module %
  ?assertError({invalid_module, _}, ecomet:edit_object(Object, #{
    <<"behaviour_module">> => non_ecomet_module
  })),

  % We changing <<behaviour_module>> to valid module,
  % but module does not contain on_create function
  ?assertError({undefined_on_create, _}, ecomet:edit_object(Object, #{
    <<"behaviour_module">> => ecomet_query})),

  % We changing <<behaviour_module>> to valid module,
  % and module contain {on_create, on_edit, on_delete} functions
  ok = ecomet:edit_object(Object, #{<<"behaviour_module">> => ecomet_object}),
  ecomet:delete_object(Object),
  ok.


on_delete_test(_Config) ->
  ecomet_user:on_init_state(),

  Pattern= ecomet:create_object(#{
    <<".name">> => <<"Rexxar">>,
    <<".folder">> => ?OID(<<"/root/.patterns">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.database">>)
  }),

  UselessDB = ecomet:create_object(#{
    <<".name">> => <<"IamUseless">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(Pattern)
  }),

  ct:pal("Databases ~n~p~n", [ecomet_db:get_databases()]),
  % We are trying to delete Pattern, but we have UselessDB
  % which <<.pattern>> is Pattern %
  ?assertError({has_objects, _}, ecomet:delete_object(Pattern)),
  ecomet_object:delete(UselessDB),
  ecomet:delete_object(Pattern),
  ok.

