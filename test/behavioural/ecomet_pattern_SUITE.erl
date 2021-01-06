%%%-------------------------------------------------------------------
%%% @author faceplate
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Dec 2020 14:58
%%%-------------------------------------------------------------------
-module(ecomet_pattern_SUITE).
-author("faceplate").


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
  get_parent_test/1,
  get_parents_test/1,
  set_parents_test/1,
  wrap_transaction_test/1,
  is_empty_test/1,
  on_delete_test/1,
  on_edit_test/1,
  on_create_test/1
]).


all() ->
  [
    check_handler_module_test,
    check_handler_test,
    check_parent_test,
    set_parents_test,
    wrap_transaction_test,
    is_empty_test,
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
  ok = ecomet_pattern:check_handler_module(ecomet_db)

.


% Object == #{behaviour_module => {NewValue, OldValue}} %
% Other fields are not important in this case %
check_handler_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We do not change behaviour_module, should return ok
  ok = ecomet_pattern:check_handler(#{ 1 => 2, {old, new} => {new, old} }),
  ok = ecomet_pattern:check_handler(#{ 1.5 => 4.2, <<"old">> => <<"new">> }),

  % Changing behaviour_module to none, should return ok %
  ok = ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {none, notnone} }),
  ok = ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {none, <<"Smth">>}, 1 => 2 }),

  ?assertError(invalid_module, ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {invalid_module, none}})),
  ?assertError(undefined_on_create, ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {ecomet_query, none}})),

  ok = ecomet_pattern:check_handler(#{ <<"behaviour_module">> => {ecomet_field, none}}),
  meck:unload(ecomet)
.

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

  parents = ecomet_pattern:get_parent(#{<<"parents">> => parents}),

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


wrap_transaction_test(_Config) ->
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
  ?assertError(cannot_change_parent, ecomet_pattern:check_parent(#{ <<"parent_pattern">> => {new, old} })),
  ?assertError(cannot_change_parent, ecomet_pattern:check_parent(#{ <<"parent_pattern">> => {<<"why">>, <<"soHard?">>} })),

  meck:unload(ecomet)
.

is_empty_test(_Config) ->
  ok.

on_create_test(_Config) ->
  ok.

on_edit_test(_Config) ->
  ok.

on_delete_test(_Config) ->
  ok.

