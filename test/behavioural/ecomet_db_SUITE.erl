%%%-------------------------------------------------------------------
%%% @author zeinetsse
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Dec 2020 17:29
%%%-------------------------------------------------------------------
-module(ecomet_db_SUITE).
-author("zeinetsse").


-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").


%% API
-export([all/0, group/0, init_per_suite/1, end_per_suite/1]).

-export([
  check_id_test/1,
  create_database_test/1,
  check_name_test/1,
  delete_test/1,
  edit_test/1,
  create_test/1
]).


all() ->
  [
    check_id_test,
    create_database_test,
    check_name_test,
    delete_test,
    edit_test,
    create_test
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
  try ecomet_db:check_name(#{<<".name">> => {<<"Invalid-name(?)">>, none}, value => 123, <<"id">> => 100000})
  catch
    error:invalid_name -> invalid_name
  end,
  try ecomet_db:check_name(#{<<".name">> => {<<"1nvaliI) Nam3">>, none}, value => 123, <<"id">> => 100000})
  catch
    error:invalid_name -> invalid_name
  end,
  try ecomet_db:check_name(#{<<".name">> => {<<"Invalid&&Name">>, none}, value => 123, <<"id">> => 100000})
  catch
    error:invalid_name -> invalid_name
  end,
  try ecomet_db:check_name(#{<<".name">> => {<<";)(:%%%">>, none}, value => 123, <<"id">> => 100000})
  catch
    error:invalid_name -> invalid_name
  end,
  try ecomet_db:check_name(#{<<".name">> => {"Seems_to_be_valid)", none}, value => 123, <<"id">> => 100000})
  catch
    error:invalid_name -> invalid_name
  end,



  % We edit name, have to crash or smth like that %
  try ecomet_db:check_name(#{<<".name">> => {<<"Issa">>, <<"Roman">>}, val => [1, 1, {11, 12}]})
  catch
    error:renaming_is_not_allowed -> renaming_is_not_allowed
  end,

  try ecomet_db:check_name(#{<<".name">> => {<<"Galym">>, <<"Abay">>}, value => 123, <<"id">> => <<"qwe">>})
  catch
    error:renaming_is_not_allowed -> renaming_is_not_allowed
  end,

  try ecomet_db:check_name(#{<<".name">> => {<<"Faceplate">>, <<"EComet">>}, <<"id">> => {1, 1}})
  catch
    error:renaming_is_not_allowed -> renaming_is_not_allowed
  end,

  meck:unload(ecomet)
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
  try ecomet_db:check_id(#{ <<"id">> => {1, 2}, <<".name">> => {<<"Change">>, <<"Me">>} })
  catch
    error:change_id_is_not_allowed ->change_id_is_not_allowed
  end,
  try ecomet_db:check_id(#{ <<"id">> => {happy, sad}, <<".name">> => smth, 3 => 5.5 })
  catch
    error:change_id_is_not_allowed ->change_id_is_not_allowed
  end,

  try ecomet_db:check_id(#{ <<"id">> => {<<"qwe">>, <<"asd">>} })
  catch
    error:change_id_is_not_allowed ->change_id_is_not_allowed
  end,

  meck:unload(ecomet)
.

% Just putting %
create_database_test(_Config) ->

  % We are creating new DB, should return ok %
  {ok, _ID1} = ecomet_db:create_database(mydb1),
  {ok, _ID2} = ecomet_db:create_database(mydb2),
  {ok, _ID3} = ecomet_db:create_database(mydb3),

  % We are creating existing DB, error should occur%
  error = ecomet_db:create_database(mydb1),
  error = ecomet_db:create_database(mydb2),
  error = ecomet_db:create_database(mydb3)
.

% Object == {id1, id2} %
delete_test(_Config) ->

  meck:new(ecomet_lib, [no_link]),
  meck:expect(ecomet_lib, to_oid, fun(Object) -> Object end),

  % Object is not mounted, should return ok %
  ok = ecomet_db:on_delete({1, 2}),
  ok = ecomet_db:on_delete({12, 23}),
  ok = ecomet_db:on_delete({324, 34}),

  meck:unload(ecomet_lib),
  % We create object. but not mount should return ok %
  TestDB_1 = ecomet:create_object(#{
    <<".name">>=><<"my_db1">>,
    <<".folder">>=>?OID(<<"/root/.database">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.database">>)
  }),
  TestDB_2 = ecomet:create_object(#{
    <<".name">>=><<"my_db2">>,
    <<".folder">>=>?OID(<<"/root/.database">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.database">>)
  }),


  ok = ecomet_db:on_delete(?OID(TestDB_1)),
  ok = ecomet_db:on_delete(?OID(TestDB_2)),


  % We create and mount object, error should occur %

  _TestFolder_1=ecomet:create_object(#{
    <<".name">>=><<"test_folder">>,
    <<".folder">>=>?OID(<<"/root">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.folder">>),
    <<"database">>=>?OID(TestDB_1)
  }),

  _TestFolder_2=ecomet:create_object(#{
  <<".name">>=><<"test_folder">>,
  <<".folder">>=>?OID(<<"/root">>),
  <<".pattern">>=>?OID(<<"/root/.patterns/.folder">>),
  <<"database">>=>?OID(TestDB_2)
  }),

  try ecomet_db:on_delete(?OID(TestDB_1))
  catch
    error:is_mounted -> is_mounted
  end,

  try ecomet_db:on_delete(?OID(TestDB_2))
  catch
    error:is_mounted -> is_mounted
  end

 % meck:unload(ecomet_lib)
.

% Object =  {<<".name">> => {NewName, OldName}, <<"id">> => {NewId, OldId}} or #{} %
% Other fields are not important in this case %
edit_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We do not edit Name or ID, should return ok %
  ok = ecomet_db:on_edit(#{ notname => {new, old}, notid => {old, veryold} }),
  ok = ecomet_db:on_edit(#{notname => {1, 2}, notid => {3, 4}}),
  ok = ecomet_db:on_edit(#{}),

  % We try to edit Name or ID, error or smtn like %
  try ecomet_db:on_edit(#{ <<".name">> => {new, old}, <<"id">> => {6, 5.5} })
  catch
    error:renaming_is_not_allowed -> renaming_is_not_allowed;
    error:change_id_is_not_allowed -> change_id_is_not_allowed
  end,
  try ecomet_db:on_edit(#{ <<".name">> => {new, old}, id => {old, veryold} })
  catch
    error:renaming_is_not_allowed -> renaming_is_not_allowed;
    error:change_id_is_not_allowed -> change_id_is_not_allowed
  end,

  try ecomet_db:on_edit(#{ name=> {old, new}, <<"id">> => {1, 2} })
  catch
    error:renaming_is_not_allowed -> renaming_is_not_allowed;
    error:change_id_is_not_allowed -> change_id_is_not_allowed
  end,

  meck:unload(ecomet)
.


% Object == #{<<".name">> => {NewName, OldName}} %
% This is Object creation, name field should be %
create_test(_Config) ->


  ecomet_user:on_init_state(),
  AA =  ?OID(<<"/root">>),
  ct:pal("OID  /root/.databases ~p", [AA]),
  TestDB_3 = ecomet:create_object(#{
    <<".name">>=><<"my_db3">>,
    <<".folder">>=>?OID(<<"/root/.database">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.database">>)
  }),
  _ID = ecomet_schema:get_db_id(my_db3),
  Folder = ecomet:create_object(#{
    <<".name">>=><<"test_folder">>,
    <<".folder">>=>?OID(<<"/root">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.folder">>),
   <<"database">>=>?OID(TestDB_3)
  }),
  my_db3 = ecomet_schema:get_mounted_db(?OID(Folder))

.

