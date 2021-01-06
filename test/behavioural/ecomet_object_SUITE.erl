%%%-------------------------------------------------------------------
%%% @author faceplate
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Dec 2020 15:12
%%%-------------------------------------------------------------------
-module(ecomet_object_SUITE).
-author("faceplate").


-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(object, {oid, edit, map, deleted=false, db}).

%% API
-export([all/0, group/0, init_per_suite/1, end_per_suite/1]).

-export([
  on_create_test/1,
  on_edit_test/1,
  on_delete_test/1,
  check_storage_type_test/1,
  check_path_test/1,
  edit_rights_test/1,
  edit_test/1,
  check_db_test/1

]).

all() ->
  [
    check_storage_type_test,
    check_path_test,
    edit_rights_test,
    edit_test,
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

on_create_test(_Config) ->

  ecomet_user:on_init_state(),

  Object = ecomet:create_object(#{
   <<".name">> => <<"Simple">>,
   <<".folder">> => ?OID(<<"/root/.databases">>),
   <<".pattern">> => ?OID(<<"/root/.patterns/.database">>),
    <<".readgroups">> => [],
    <<".writegroups">> => []
  }),

  {ok, _WriteG} = ecomet_object:read_field(Object, <<".writegroups">>),
  {ok, _ReadG} = ecomet_object:read_field(Object, <<".readgroups">>),

  %%[] = WriteG -- ReadG,

  ok = ecomet:delete_object(Object),
  ok
.

on_edit_test(_Config) ->
  meck:new([ecomet_field, ecomet_transaction, ecomet_folder]),
  meck:expect(ecomet_field, get_value, fun(Map, _OID, Field) -> {ok, element(1, maps:get(Field, Map))} end),
  meck:expect(ecomet_transaction, dict_get, fun(_OID_fields, Default) -> Default end),
  meck:expect(ecomet_field, field_changes, fun(Map,_Fields,_OID,Field) -> maps:get(Field, Map, none) end),

  meck:expect(ecomet_folder, find_object_system,
    fun(FolderID, Name) ->
      DB = #{
        {{1, 65000}, <<"Boruto">>} => {4, 7},
        {{}, <<"Keanu">>} => {2, 9},
        {{}, <<"Johnny">>} => {9, 6}
      },
      Value = maps:get({FolderID, Name}, DB, none),
      case Value of
        none -> {error, not_found};
        _ -> {ok, Value}
      end
    end),

  % Our meck object %
 Object1 = #object{
    oid={4, 7},
    map=#{<<".pattern">> => {new, old},
      <<".ts">> => {new, old},
      <<".name">> => {<<"Boruto">>, <<"Naruto">>},
      <<".folder">> => {{1, 70000}, {3, 4}}
    }
  },
  ?assertError(different_database, ecomet_object:on_edit(Object1)),

  Object2 = #object{
    oid={4, 7},
    map=#{<<".pattern">> => {new, old},
      <<".ts">> => {new, old},
      <<".name">> => {<<"Bor/uto">>, <<"Naruto">>},
      <<".folder">> => {{1, 65000}, {3, 4}}
    }
  },

  ?assertError("the '/' symbol is not allowed in names", ecomet_object:on_edit(Object2)),

  Object3 = #object{
    oid={4, 7},
    map=#{<<".pattern">> => {new, old},
      <<".ts">> => {new, old},
      <<".name">> => {<<"Boruto">>, <<"Naruto">>},
      <<".folder">> => {{1, 65000}, {3, 4}}
    }
  },
  ?assertError(can_not_change_pattern, ecomet_object:on_edit(Object3)),

  Object4 = #object{
    oid={4, 7},
    map=#{
      <<".ts">> => {new, old},
      <<".name">> => {<<"Boruto">>, <<"Naruto">>},
      <<".folder">> => {{1, 65000}, {3, 4}}
    }
  },
  ?assertError(can_not_change_ts, ecomet_object:on_edit(Object4)),

  Object5 = #object{
    oid={4, 7},
    map=#{
      <<".name">> => {<<"Boruto">>, <<".Naruto">>},
      <<".folder">> => {{1, 65000}, {3, 4}}
    }
  },
  ?assertError(system_object, ecomet_object:on_edit(Object5)),


  Object6 = #object{
    oid={4, 7},
    map=#{
      <<".name">> => {<<"Boruto">>, <<"Naruto">>},
      <<".folder">> => {{1, 65000}, {3, 4}}
    }
  },
  ok = ecomet_object:on_edit(Object6),

  meck:unload([ecomet_field, ecomet_transaction, ecomet_folder])
.

on_delete_test(_Config) ->
  meck:new(ecomet),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),

  ?assertError(system_object, ecomet_object:on_delete(#{<<".name">> => <<".system">>})),
  ok = ecomet_object:on_delete(#{<<".name">> => <<"notsystem">>}),
  meck:unload(ecomet)
.


check_storage_type_test(_Config) ->

  meck:new([ecomet, ecomet_pattern]),
  meck:expect(ecomet, read_field, fun(Object, Field) ->
                                      {ok, maps:get(Field, element(2, Object))}
                                  end),
  meck:expect(ecomet_pattern, get_storage, fun(Object) -> element(2, Object) end),

  % Create meck object %
  % ?RAM, ?RAMLOCAL, ?RAMDISC, ?DISC%
%%  {
%%    ?RAM,
%%    #{<<".folder">> => {?RAM, <<"nomatter">>}},
%%  },
  % Persistent folders can contain any types of objects %
  ok = ecomet_object:check_storage_type({ ?RAM, #{<<".folder">> => {?DISC, <<"nomatter">>}} }),
  ok = ecomet_object:check_storage_type({ whatever, #{<<".folder">> => {?RAMLOCAL, <<"nomatter">>}} }),

  % Ram only folders cannot contain persistent objects %
  ok = ecomet_object:check_storage_type({ ?RAM, #{<<".folder">> => {?RAM, <<"nomatter">>}} }),
  ?assertError(ram_only_folder, ecomet_object:check_storage_type({ ?DISC, #{<<".folder">> => {?RAM, <<"nomatter">>}} })),

  meck:unload([ecomet, ecomet_pattern]),

  ok.

check_path_test(_Config) ->

  meck:new([ecomet_field, ecomet_transaction, ecomet_folder], [no_link]),
  meck:expect(ecomet_transaction, dict_get, fun(OID, _Default) -> element(1, OID) end),

  meck:expect(ecomet_field, field_changes, fun(Map,_Fields,_OID,Field) -> maps:get(Field, Map, none) end),
  meck:expect(ecomet_folder, find_object_system, fun(FolderID, Name) ->
    Res = lists:member(Name, FolderID),
      Ret = if Res =:= false -> {error, not_found};
        true -> true
      end,
      Ret
    end),

  % Our meck object %
%%  #object{ oid=#{<<".folder">> => [list_of_names], <<".name">> => name },
%%   map=#{<<".folder">> => {new, old}, <<".name">> => {newname, oldname}}
%%   },
  Object1 = #object{ oid=#{},
    map=#{}
  },
  Object2 = #object{ oid=#{<<".folder">> => [], <<".name">> => <<"jus/tname">> },
    map=#{<<".name">> => {<<"newname">>, <<"oldname">>}}
  },
  Object3 = #object{ oid=#{<<".folder">> => [<<"justname">>], <<".name">> => <<"justname">> },
    map=#{<<".folder">> => {<<"newfolder">>, <<"oldfolder">>}}
  },
  Object4 = #object{ oid=#{<<".folder">> => [<<"anothername">>], <<".name">> => <<"justname">> },
    map=#{<<".name">> => {<<"My">>, <<"Name">>}}
  },
  % .name and .folder fields are not changed, should return ok %
  ok = ecomet_object:check_path(Object1),

  % %
  ?assertError("the '/' symbol is not allowed in names", ecomet_object:check_path(Object2)),
  ?assertError({not_unique,_}, ecomet_object:check_path(Object3)),

  ok = ecomet_object:check_path(Object4),
  meck:unload([ecomet_field, ecomet_transaction, ecomet_folder])
.

edit_rights_test(_Config) ->

 meck:new([ecomet_field, ecomet_transaction]),
 meck:expect(ecomet_transaction, dict_get, fun(OID, _Default) -> element(1, OID) end),
 meck:expect(ecomet_field, field_changes, fun(Map,_Fields,_OID,Field) -> maps:get(Field, Map, none) end),
% Our meck object %
%%  #object{ oid=#{<<".folder">> => [list_of_names], <<".name">> => name },
%%   map=#{<<".writegroups">> => {new, old}, <<".readgroups">> => {newname, oldname}}
%%   },


 meck:unload([ecomet_field, ecomet_transaction])
.

edit_test(_Config) ->

%%  % Our meck object %
%%  #object{
%%    oid = {1, 2},
%%    map = #{}
%%  },

  ok.


check_db_test(_Config) ->

  meck:new([ecomet_field, ecomet_transaction]),
  meck:expect(ecomet_transaction, dict_get, fun(OID, _Default) -> element(1, OID) end),
  meck:expect(ecomet_field, field_changes, fun(Map,_Fields,_OID,Field) -> maps:get(Field, Map, none) end),
  % Our meck object %
%%  #object{ oid={1, 2} },
%%   map=#{<<".folder">> => {new, old}, <<".name">> => {newname, oldname}}
%%   },

  ok = ecomet_object:check_db(#object{oid={1, 2}, map=#{<<"notfolder">> => <<"doesntmttr">>}}),

  ok = ecomet_object:check_db(#object{oid={3, 2}, map=#{<<".folder">> => {{1, 65000}, {3, 4} } } }),
  ?assertError(different_database,ecomet_object:check_db(#object{oid={3, 2}, map=#{<<".folder">> => {{1, 70000}, {3, 4} } } })),


  meck:unload([ecomet_field, ecomet_transaction])
.


