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
-module(ecomet_node_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([all/0, group/0, init_per_suite/1, end_per_suite/1]).

-export([
  check_name_test/1,
  check_id_test/1,
  register_node_test/1,
  unregister_node_test/1,
  on_create_test/1,
  on_edit_test/1,
  on_delete_test/1
]).

all() ->
  [
    check_name_test,
    check_id_test,
    register_node_test,
    unregister_node_test,
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


check_name_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We are not changing name, should return ok %
  ok = ecomet_node:check_name(#{notenanme => surname, <<".notname">> => <<".surname">>}),
  ok = ecomet_node:check_name(#{<<".nickname">> => <<"Sparrow">>, <<"WakeUp">> => <<"Neo">>}),

  % We create new object with valid name, should return ok %
  % TODO. Add valid names
  %_ValidName = <<"iwasbornin1998@I">>,
  %_AnotherVN = <<"iwasbornin1998@">>,
  %ok = ecomet_node:check_name(#{<<".name">> => {<<"Name">>, none}}),
  %ok = ecomet_node:check_name(#{<<".name">> => {<<"www">>, none}}),

  % We create new object with invalid name, error should occur %
  ?assertError(invalid_node_name, ecomet_node:check_name(#{<<".name">> => {<<"dfdfdfwdfdf">>,none} })),
  ?assertError(invalid_node_name, ecomet_node:check_name(#{<<".name">> => {<<"iwasbornin1988">>,none}} )),

  % We are trying to change name, error should occur %
  ?assertError(renaming_is_not_allowed, ecomet_node:check_name(#{ <<".name">> => {<<"NewOne">>, <<"OldOne">>}})),
  ?assertError(renaming_is_not_allowed, ecomet_node:check_name(#{<<".name">> => {<<"Naruto">>, <<"Sasuke">>}})),

  meck:unload(ecomet)
.


check_id_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We do not edit ID, should return OK %
  ok = ecomet_db:check_id(#{<<".name">> => {<<"NewOne">>, <<"OldOne">>}, key => value}),
  ok = ecomet_db:check_id(#{1.5 => {<<"NewOne">>, <<"OldOne">>}, value => key}),
  ok = ecomet_db:check_id(#{1 => 2, 3 => 4, 5 => 6}),

  % We create new Object, should return OK %
  ok = ecomet_db:check_id(#{ <<"id">> => {1, none}, <<".name">> => {<<"Help">>, <<"Me">>} }),
  ok = ecomet_db:check_id(#{ <<"id">> => {{1,2}, none}, <<".name">> => {<<"Hack">>, <<"Me">>} }),
  ok = ecomet_db:check_id(#{ <<"id">> => {<<"ID">>, none}, <<".name">> => {<<"Change">>, <<"Me">>} }),

  % We try to change id, error occur here %
  ?assertError(change_id_is_not_allowed, ecomet_db:check_id(#{ <<"id">> => {1, 2}, <<".name">> => {<<"Change">>, <<"Me">>} })),
  ?assertError(change_id_is_not_allowed,ecomet_db:check_id(#{ <<"id">> => {happy, sad}, <<".name">> => smth, 3 => 5.5 })),
  ?assertError(change_id_is_not_allowed,ecomet_db:check_id(#{ <<"id">> => {<<"qwe">>, <<"asd">>} })),

  meck:unload(ecomet)

.


register_node_test(_Config) ->
  meck:new(ecomet),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field ,Object)} end),
  meck:expect(ecomet, edit_object, fun(_Object, _IDMapper) -> ok end),

  % TODO. Check side effects: set the ID and registering the node in the schema
  ok = ecomet_node:register_node(#{<<".name">> => <<"jSparrow">>, <<"value">> => 123}),
  ok = ecomet_node:register_node(#{<<".name">> => <<"neo">>, age => 18}),
  meck:unload(ecomet)
.


unregister_node_test(_Config) ->

  meck:new(ecomet),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field ,Object)} end),

  % TODO. Check side effects: unregistering the node in the schema

  ok = ecomet_node:unregister_node(#{<<".name">> => <<"jSparrow">>, sex => male}),
  ok = ecomet_node:unregister_node(#{<<".name">> => <<"neo">>, sex => male}),

  meck:unload(ecomet)

.


on_create_test(_Config) ->

  meck:new([ecomet, ecomet_schema], [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field ,Object, none)} end),
  meck:expect(ecomet, edit_object, fun(_Object, _IDMapper) -> ok end),
  meck:expect(ecomet_schema, add_node, fun(_Atom) -> ok end),

  ?assertError(invalid_node_name, ecomet_node:on_create(#{<<".name">> => {<<"invalidname">>, none}})),
  %ok = ecomet_node:on_create(#{<<".name">> => {<<"iwasbornin1998@faceplate.c">>, none}}),

  % TODO. Check side effects (see register_node_test)
  meck:unload([ecomet, ecomet_schema]),
  ok
.


on_edit_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We are not changing name or id, should return ok %
  ok = ecomet_node:on_edit(#{<<".notname">> => {<<"dont">>, <<"edit">>}, <<"notid">> => {bla, bla}}),
  ok = ecomet_node:on_edit(#{<<"hello">> => <<"its_me">>}),

  %We are trying to change name or id, error should occur %
  ?assertError(renaming_is_not_allowed, ecomet_node:on_edit(#{<<".name">> => {<<"NewOne">>, <<"OldOne">>}})),
  ?assertError(change_id_is_not_allowed, ecomet_node:on_edit(#{<<"id">> => {{1, 2}, {3, 4}}})),
  ?assertError(renaming_is_not_allowed, ecomet_node:on_edit(#{<<".name">> => {<<"NewOne">>, <<"OldOne">>}, <<"id">> => {{1, 2}, {3, 4}} })),

  meck:unload(ecomet)
.

on_delete_test(_Config) ->

  meck:new(ecomet),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field ,Object, none)} end),

  % We are trying to delete ready node, error should occur %
  ?assertError(is_active, ecomet_node:on_delete(#{<<"is_ready">> => true, <<".name">> => <<"MyName">>})),

  %We are trying to delete non ready node, should return ok %

  % TODO. Check side effects (see unregister_node_test)
  ok = ecomet_node:on_delete(#{<<"is_ready">> => false, <<".name">> => <<"myName">>}),
  ok = ecomet_node:on_delete(#{ <<".name">> => <<"frodo">> }),

  meck:unload(ecomet)
.

