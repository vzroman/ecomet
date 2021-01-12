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
-module(ecomet_db_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").


%% API
-export([all/0, group/0, init_per_suite/1, end_per_suite/1]).

-export([
  check_id_test/1,
  create_database_test/1,
  check_name_test/1,
  on_create_test/1,
  on_edit_test/1,
  on_delete_test/1
]).


all() ->
  [
    check_id_test,
    create_database_test,
    check_name_test,
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

% Object == #{<<".name">> => {NewName, OldName}} OR #{} %
% Other fields are not important in this case %
check_name_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We do not edit name, have to return ok %
  ok = ecomet_db:check_name(#{}),
  ok = ecomet_db:check_name(#{<<"id">> => {1, 2}, value => [1, 2, 3]}),
  ok = ecomet_db:check_name(#{1 => 2, <<"happy">> => <<"sad">>}),


% We create new Object with valid name %
  ok = ecomet_db:check_name(#{<<".name">> => {<<"v_a_l_i_d_n_a_m_e">>, none}, value => 123, <<"id">> => 100000}),
  ok = ecomet_db:check_name(#{<<".name">> => {"TheValid_nAmE", none}, value => 123, <<"id">> => 100000}),
  ok = ecomet_db:check_name(#{<<".name">> => {"1_23_4_5", none}, value => 123, <<"id">> => 100000}),
  ok = ecomet_db:check_name(#{<<".name">> => {"Ev4n_t4i5_po56ible_", none}, value => 123, <<"id">> => 100000}),
  ok = ecomet_db:check_name(#{<<".name">> => {"_____", none}, value => 123, <<"id">> => 100000}),


% We create new Object with invalid name, prog have to crash %
  ?assertError(invalid_name, ecomet_db:check_name(#{<<".name">> => {<<"Invalid-name(?)">>, none},
    value => 123, <<"id">> => 100000})),
  ?assertError(invalid_name, ecomet_db:check_name(#{<<".name">> => {<<"1nvaliI) Nam3">>, none},
    value => 123, <<"id">> => 100000})),

  ?assertError(invalid_name, ecomet_db:check_name(#{<<".name">> => {<<"Invalid&&Name">>, none},
    value => 123, <<"id">> => 100000})),

  ?assertError(invalid_name, ecomet_db:check_name(#{<<".name">> => {<<";)(:%%%">>, none},
    value => 123, <<"id">> => 100000})),

  ?assertError(invalid_name, ecomet_db:check_name(#{<<".name">> => {"Seems_to_be_valid)", none},
    value => 123, <<"id">> => 100000})),


  % We edit name, have to crash or smth like that %
  ?assertError(renaming_is_not_allowed, ecomet_db:check_name(#{<<".name">> => {<<"Issa">>, <<"Roman">>},
    val => [1, 1, {11, 12}]})),
  ?assertError(renaming_is_not_allowed, ecomet_db:check_name(#{<<".name">> => {<<"Galym">>, <<"Abay">>},
    value => 123, <<"id">> => <<"qwe">>})),

  ?assertError(renaming_is_not_allowed, ecomet_db:check_name(#{<<".name">> => {<<"Faceplate">>, <<"EComet">>},
    <<"id">> => {1, 1}})),

  meck:unload(ecomet),
  ok
.

% Object == {<<"id">> => {NewId, OldID}}  %
% Other fieds are not important in this case%
check_id_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We do not edit ID, should return OK %
  ok = ecomet_db:check_id(#{<<".name">> => {<<"NewOne">>, <<"OldOne">>}, key => value}),
  ok = ecomet_db:check_id(#{1.5 => {<<"NewOne">>, <<"OldOne">>}, value => key}),
  ok = ecomet_db:check_id(#{1 => 2, 3 => 4, 5 => 6}),

  % We create new Object, should return OK %
  ok = ecomet_db:check_id(#{ <<"id">> => {1, none}, <<".name">> => {<<"Change">>, <<"Me">>} }),
  ok = ecomet_db:check_id(#{ <<"id">> => {{1,2}, none}, <<".name">> => {<<"Hack">>, <<"Me">>} }),
  ok = ecomet_db:check_id(#{ <<"id">> => {<<"ID">>, none}, <<".name">> => {<<"Change">>, <<"Me">>} }),

  % We try to change id, error occur here %
  ?assertError(change_id_is_not_allowed, ecomet_db:check_id(#{ <<"id">> => {1, 2},
    <<".name">> => {<<"Change">>, <<"Me">>} })),

  ?assertError(change_id_is_not_allowed, ecomet_db:check_id(#{ <<"id">> => {happy, sad},
    <<".name">> => smth, 3 => 5.5 })),

  ?assertError(change_id_is_not_allowed, ecomet_db:check_id(#{ <<"id">> => {<<"qwe">>, <<"asd">>} })),

  meck:unload(ecomet),
  ok
.

% Just putting %
create_database_test(_Config) ->

  DBs0 = ecomet_db:get_databases(),
  Storages0 = ecomet_backend:get_storages(),
  % We are creating new DB, should return ok %
  {ok, _ID1} = ecomet_db:create_database(mydb1),
  {ok, _ID2} = ecomet_db:create_database(mydb2),
  {ok, _ID3} = ecomet_db:create_database(mydb3),

  Storages=ecomet_backend:get_storages(),
  DBs = ecomet_db:get_databases(),

  % We are creating existing DB, error should occur%
  error = ecomet_db:create_database(mydb1),
  error = ecomet_db:create_database(mydb2),
  error = ecomet_db:create_database(mydb3),

  Storages=ecomet_backend:get_storages(),
  DBs = ecomet_db:get_databases(),

  ecomet_db:remove_database(mydb1),
  ecomet_db:remove_database(mydb2),
  ecomet_db:remove_database(mydb3),

  DBs0 = ecomet_db:get_databases(),
  Storages0 = ecomet_backend:get_storages(),

  ok.

% Object == {id1, id2} %
on_delete_test(_Config) ->

  ecomet_user:on_init_state(),
  % We create object. but not mount should return ok %
  TestDB_1 = ecomet:create_object(#{
    <<".name">>=><<"my_db1">>,
    <<".folder">>=>?OID(<<"/root/.databases">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.database">>)
  }),
  TestDB_2 = ecomet:create_object(#{
    <<".name">>=><<"my_db2">>,
    <<".folder">>=>?OID(<<"/root/.databases">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.database">>)
  }),


  ok = ecomet:delete_object(TestDB_1),
  ok = ecomet:delete_object(TestDB_2),


  % We create and mount object, error should occur %

  TestDB_3 = ecomet:create_object(#{
    <<".name">>=><<"my_db3">>,
    <<".folder">>=>?OID(<<"/root/.databases">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.database">>)
  }),
  TestDB_4 = ecomet:create_object(#{
    <<".name">>=><<"my_db4">>,
    <<".folder">>=>?OID(<<"/root/.databases">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.database">>)
  }),

  TestFolder_3=ecomet:create_object(#{
    <<".name">>=><<"test_folder3">>,
    <<".folder">>=>?OID(<<"/root">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.folder">>),
    <<"database">>=>?OID(TestDB_3)
  }),

  TestFolder_4=ecomet:create_object(#{
  <<".name">>=><<"test_folder4">>,
  <<".folder">>=>?OID(<<"/root">>),
  <<".pattern">>=>?OID(<<"/root/.patterns/.folder">>),
  <<"database">>=>?OID(TestDB_4)
  }),

  ?assertError({is_mounted,_}, ecomet:delete_object(TestDB_3)),
  ?assertError({is_mounted,_}, ecomet:delete_object(TestDB_4)),
  ok = ecomet:edit_object(TestFolder_3, #{<<"database">> => none}),
  ok = ecomet:edit_object(TestFolder_4, #{<<"database">> => none}),

  ok = ecomet:delete_object(TestDB_3),
  ok = ecomet:delete_object(TestDB_4),

  ok = ecomet:delete_object(TestFolder_3),
  ok = ecomet:delete_object(TestFolder_4),
  ok.

% Object =  {<<".name">> => {NewName, OldName}, <<"id">> => {NewId, OldId}} or #{} %
% Other fields are not important in this case %
on_edit_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We do not edit Name or ID, should return ok %
  ok = ecomet_db:on_edit(#{ notname => {new, old}, notid => {old, veryold} }),
  ok = ecomet_db:on_edit(#{notname => {1, 2}, notid => {3, 4}}),
  ok = ecomet_db:on_edit(#{}),

  % We try to edit Name or ID, error or smtn like %
  ?assertError(renaming_is_not_allowed, ecomet_db:on_edit(#{ <<".name">> => {new, old},
    <<"id">> => {6, 5.5} })),

  ?assertError(renaming_is_not_allowed, ecomet_db:on_edit(#{ <<".name">> => {new, old},
    id => {old, veryold} })),

  ?assertError(change_id_is_not_allowed, ecomet_db:on_edit(#{ name=> {old, new}, <<"id">> => {1, 2} })),

  meck:unload(ecomet)
.


% Object == #{<<".name">> => {NewName, OldName}} %
% This is Object creation, name field should be %
on_create_test(_Config) ->
  ecomet_user:on_init_state(),

  TestDB = ecomet:create_object(#{
    <<".name">>=><<"my_db">>,
    <<".folder">>=>?OID(<<"/root/.databases">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.database">>)
  }),
  _ID = ecomet_schema:get_db_id(my_db),
  Folder = ecomet:create_object(#{
    <<".name">>=><<"test_folder">>,
    <<".folder">>=>?OID(<<"/root">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.folder">>),
   <<"database">>=>?OID(TestDB)
  }),
  my_db = ecomet_schema:get_mounted_db(?OID(Folder)),
  ok = ecomet:delete_object(Folder),
  ok = ecomet:delete_object(TestDB),

  ok.

