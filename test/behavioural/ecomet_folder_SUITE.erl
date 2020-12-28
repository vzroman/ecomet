%%%-------------------------------------------------------------------
%%% @author faceplate
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Dec 2020 15:12
%%%-------------------------------------------------------------------
-module(ecomet_folder_SUITE).
-author("faceplate").

%% API
-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([all/0, group/0, init_per_suite/1, end_per_suite/1]).

-export([
  check_database_test/1,
  inherit_rights_test/1,
  recursive_rights_test/1,
  apply_recursion_test/1,
  get_content_system_test/1,
  get_db_name_test/1,
  is_empty_test/1,
  on_create_test/1,
  on_edit_test/1,
  on_delete_test/1
]).

all() ->
  [
    check_database_test,
    inherit_rights_test,
    recursive_rights_test,
    apply_recursion_test,
    get_content_system_test,
    get_db_name_test,
    is_empty_test,
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



on_create_test(_Config) ->
  ok
.

on_edit_test(_Config) ->
  ok
.

on_delete_test(_Config) ->
  ok
.

% For testing this function we need to complicate our meck object%
check_database_test(_Config) ->

  meck:new([ecomet, ecomet_db, ecomet_schema, ecomet_query, ecomet_lib]),
  meck:expect(ecomet, field_changes, fun(Object, Field) -> maps:get(Field, Object, none) end),
  meck:expect(ecomet_schema, unmount_db, fun(_FolderID) -> ok end) ,
  meck:expect(ecomet_schema, mount_db, fun(_FolderID, _DB) -> ok end),
  meck:expect(ecomet_db, get_name, fun(_MountedDB) -> ok end),
  meck:expect(ecomet_query, system, fun(DBList, _Fields, _Cond) ->
                                    Field = element(1, _Cond),
                                    Folder = element(3, _Cond),
                                    map_get(Field, Folder)
                                    end),

  % should return none or DB name%
  meck:expect(ecomet_schema, get_mounted_db, fun(FolderID) -> maps:get(<<"db_name">>, FolderID) end),

  meck:expect(ecomet_lib, to_oid, fun(Object) -> maps:get(<<"OID">>, Object) end),


  % We are not changing database field, should return ok %
  ok = ecomet_folder:check_database(#{<<"Chester">> => <<"Bennington">>}),
  ok = ecomet_folder:check_database(#{}),

  % Our complicated meck object %
  #{
    <<"database">> => {<<"MountedDB">>, <<"UnmountedDB">>},
    <<"OID">> => #{<<"db_name">> => <<"myDB">>, <<".folder">> => 0}
  },

  ecomet_folder:check_database(#{<<"database">> => {<<"MountedDB">>, <<"UnmountedDB">>}}),


  meck:unload([ecomet, ecomet_db, ecomet_schema, ecomet_query, ecomet_lib])
.


inherit_rights_test(_Config) ->
  meck:new([ecomet, ecomet_object]),
  meck:expect(ecomet_object, read_field, fun(Object, Field, Default) -> {ok, maps:get(Field, Object, Default)} end),
  meck:expect(ecomet, edit_object, fun(_Object, _Fields) ->

    ok end),


  meck:unload([ecomet, ecomet_object])
.



recursive_rights_test(_Config) ->

  meck:new(ecomet),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field ,Object, none)} end),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),


  % Because <<"recursive_rights">> == false %
  ok = ecomet_folder:recursive_rights(#{<<"recursive_rights">> => false,
                                   <<".contentreadgroups">> => {new, old},
                                   <<".contentwritegroups">> => {new, old}}),
  ok = ecomet_folder:recursive_rights(#{<<"recursive_rights">> => false, <<"dsnt_matter">> => qwe}),

  %               wfww      %
    ok = ecomet_folder:recursive_rights(#{<<"recursive_rights">> => true,
      <<".readgroups">>=> {new, old},
      <<".readgroups">> => {new, old} }),

  ok
.



apply_recursion_test(_Config) ->
  ok.


get_content_system_test(_Config) ->

  ok.



get_db_name_test(_Config) ->

  ok.

is_empty_test(_Config) ->
  ok
.