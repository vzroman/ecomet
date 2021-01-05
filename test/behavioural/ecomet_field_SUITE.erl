%%%-------------------------------------------------------------------
%%% @author faceplate
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Dec 2020 15:11
%%%-------------------------------------------------------------------
-module(ecomet_field_SUITE).
-author("faceplate").

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
  check_folder_test/1,
  check_storage_test/1,
  check_type_test/1,
  check_index_test/1,
  check_default_test/1,
  to_schema_test/1,
  get_type_test/1,
  check_name_test/1
]).

all() ->
  [
    on_create_test,
    on_edit_test,
    on_delete_test,
    check_name_test,
    check_folder_test,
    check_storage_test,
    check_type_test,
    check_index_test,
    check_default_test,
    to_schema_test,
    get_type_test
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

  meck:new([ecomet, ecomet_pattern]),
  meck:expect(ecomet, field_changes, fun(Object, Field) -> maps:get(Field, Object, none) end),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, element(1, maps:get(Field, Object))} end),
  meck:expect(ecomet_pattern, append_field, fun(_patternID, _Name, _COnfig) -> ok end),
  meck:expect(ecomet_pattern, remove_field, fun(_PatternID, _OldName) -> ok end),
  meck:expect(ecomet_pattern, get_storage, fun(PatternID) -> PatternID end),
  meck:expect(ecomet, edit_object, fun(_Object, _Map) -> ok end),

  Object1 = #{<<".name">> => {<<".faceplate">>, none},
    <<".folder">> => {?DISC, none},
    <<"storage">> => {?RAMDISC, ?DISC},
      <<"type">> => term,
      <<"subtype">> => {},
    <<"index">> => {simple, datetime},
    <<"default">> => {<<"newdef">>, <<"newdef">>}

  },

  ok = ecomet_field:on_create(Object1),

  meck:unload([ecomet, ecomet_pattern])
.


on_edit_test(_Config) ->
    ok.


on_delete_test(_Config) ->

  meck:new([ecomet, ecomet_pattern]),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  meck:expect(ecomet_pattern, remove_field, fun(_PatternID, _Name) -> ok end),
  meck:expect(ecomet_pattern, is_empty, fun(PatternID) -> PatternID end),
  % Deleting object %
  ecomet_field:on_delete(#{<<".folder">> => true, <<".name">> => <<"NazGul">>}),
  % Incorrectly deleting object %
  ?assertError(has_objects, ecomet_field:on_delete(#{<<".folder">> => false, <<".name">> => <<"NazGul">>})),
  ?assertError(has_objects,ecomet_field:on_delete(#{<<".folder">> => sfsf, <<".name">> => <<"NazGul">>})),



  meck:unload([ecomet_pattern, ecomet])


.


check_name_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  meck:new(ecomet_pattern),
  meck:expect(ecomet_pattern, remove_field, fun(_PatternID, _OldName) -> ok end),

  %% #{<<".name">> => {<<"Old">>, <<"New">>}, <<".folder">> => 12},

  % We are not changing name, should return ok %
  ok = ecomet_field:check_name(#{<<".folder">> => 12}, true),
  ok = ecomet_field:check_name(#{<<".notname">> => <<"whysohard">>}, false),

  % We are trying to change name, while is_empty == false, error should occur%
  ?assertError(has_objects, ecomet_field:check_name(#{<<".name">> => {<<"new">>, <<"old">>}}, false)),
  ?assertError(has_objects, ecomet_field:check_name(#{<<".name">> => {<<"help">>, <<"me">>}}, false)),

  % is_empty == true %
  % We are trying to change on incorrect name %
  ?assertError(invalid_name, ecomet_field:check_name(#{<<".name">> => {<<"S_eE-mS_Va119(">>, none}}, true)),
  ?assertError(invalid_name, ecomet_field:check_name(#{<<".name">> => {<<".Not.valid">>, none}}, true)),

  % We change name on valid %
  ok = ecomet_field:check_name(#{<<".name">> => {<<".H-a_R--r9y">>, none}, <<".folder">> => 12}, true),
  ok = ecomet_field:check_name(#{<<".name">> => {<<"._-_-_-_-">>, <<"Alucard">>}, <<".folder">> => 1998}, true),


  meck:unload(ecomet),
  meck:unload(ecomet_pattern)
.


check_folder_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We are not changing pattern, should return ok%
  ok = ecomet_field:check_folder(#{<<".notfolder">> => okey}, true),
  ok = ecomet_field:check_folder(#{<<"Uchiha">> => <<"Itachi">>}, false),

  % We are creating smth(object), should return ok %
  ok = ecomet_field:check_folder(#{<<".folder">> => {<<"New">>, none}}, true),
  ok = ecomet_field:check_folder(#{<<".folder">> => {<<"Roy">>, none}, <<"Linkin">> => <<"Park">>}, false),

  % We trying to change folder or pattern, error should occur %
  ?assertError(cannot_change_pattern, ecomet_field:check_folder(#{<<".folder">> => {<<"Roy">>, <<"MusTang">>}}, false)),
  ?assertError(cannot_change_pattern, ecomet_field:check_folder(#{<<".folder">> => {<<"Sub-Zero">>, <<"Scorpion">>}}, true)),

  meck:unload(ecomet)
.

check_storage_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  meck:new(ecomet_pattern, [no_link]),
  % PatternID == ?RAMLOCAL or ?RAM or none%
  meck:expect(ecomet_pattern, get_storage, fun(PatternID) -> PatternID end),

  % We are not changing storage, should return ok %
  ok = ecomet_field:check_storage(#{<<"singer">> => {<<"avril">>, <<"lavigne">>}}, false),
  ok = ecomet_field:check_storage(#{1 => 2}, true),

  % Object or smth not empty, error should occur %
  ?assertError(has_objects, ecomet_field:check_storage(#{<<"storage">> => {<<"new">>, <<"old">>}}, false)),
  ?assertError(has_objects, ecomet_field:check_storage(#{<<"storage">> => {<<"job">>, <<"difficult">>}}, false)),

  % is_empty == true %
  % Storage == [?RAMLOCAL,?RAM,?RAMDISC,?DISC] or whatever  %
  % NewStorage = RAMDISC; NewStorage = ?DISC %
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<"storage">> => {?RAMDISC, none}, <<".folder">> => ?RAMLOCAL},true)),
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<"storage">> => {?RAMDISC, none}, <<".folder">> => ?RAM}, true)),
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<"storage">> => {?DISC, none}, <<".folder">> => ?RAMLOCAL}, true)),
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<"storage">> => {?DISC, none}, <<".folder">> => ?RAM}, true)),

  ok = ecomet_field:check_storage(#{<<"storage">> => {?RAMDISC, none}, <<".folder">> => <<"LinkinPark">>}, true),
  ok = ecomet_field:check_storage(#{<<"storage">> => {?DISC, none}, <<".folder">> => <<"SecretFolder">>}, true),

  % NewStorage != RAMDISC; NewStorage != ?DISC %
  ok = ecomet_field:check_storage(#{<<"storage">> => {?RAM, <<"FlashCard">>}, <<".folder">> => ?RAMLOCAL}, true),
  ok = ecomet_field:check_storage(#{<<"storage">> => {?RAMLOCAL, <<"Safe">>}, <<".folder">> => <<"MyFolder">>},true),
  ?assertError(invalid_storage_type, ecomet_field:check_storage(#{<<"storage">> => {<<"HDFS">>, <<"SSD">>}, <<"Green">> => <<"Day">>}, true)),
  ?assertError(invalid_storage_type, ecomet_field:check_storage(#{<<"storage">> => {<<"Cloud">>, ?RAM}}, true)),

  meck:unload(ecomet_pattern),
  meck:unload(ecomet)
.

to_schema_test(_Config) ->
  Params1 = #{
    <<"type">> => mytype,
    <<"subtype">> => none,
    <<"index">> => 1,
    <<"required">> => false,
    <<"storage">> => ram,
    <<"default">> => no,
    <<"autoincrement">> => true,
    <<"myfield">> => <<"plsnotcrash">>
  },

  Params2 = #{
    <<"type">> => str,
    <<"required">> => <<"YES">>,
    <<"storage">> => <<"SSD">>,
    <<"default">> => <<"Sleep">>,
    <<"myfield">> => <<"tired">>
  },

  Ans1 = #{
    type => mytype,
    subtype => none,
    index => 1,
    required => false,
    storage => ram,
    default => no,
    autoincrement => true
  },
  Ans1 = ecomet_field:to_schema(Params1),

  Ans2 = #{
    type => str,
    subtype => none,
    index => none,
    required => <<"YES">>,
    storage => <<"SSD">>,
    default => <<"Sleep">>,
    autoincrement => false
  },
  Ans2 = ecomet_field:to_schema(Params2)
.

get_type_test(_Config) ->

  ok.

check_type_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We are not changing type and subtype, should return ok %
  ok = ecomet_field:check_type(#{<<"type">> => none, <<"Johnny">> => <<"Depp">>}, true),
  ok = ecomet_field:check_type(#{<<"subtype">> => none, <<"Cyber">> => <<"Punc">>}, false),
  ok = ecomet_field:check_type(#{}, true),

  % We are trying to change type or subtype, but there already created object with the schema %
  ?assertError(has_objects, ecomet_field:check_type(#{<<"type">> => {<<"New">>, <<"Old">>}, <<"subtype">> => {new, old}}, false)),
  ?assertError(has_objects, ecomet_field:check_type(#{<<"type">> => {<<"New">>, <<"Old">>}}, false)),
  ?assertError(has_objects, ecomet_field:check_type(#{<<"subtype">> => {<<"New">>, <<"Old">>}}, false)),

  % is_empty == true %
  % We are trying to change type or subtype or we have valid types %
  ok = ecomet_field:check_type(#{<<"type">> => term, <<"subtype">> => {new, old}}, true),
  ok = ecomet_field:check_type(#{<<"type">> => list, <<"subtype">> => binary} ,true),
  ok = ecomet_field:check_type(#{<<"type">> => atom}, true),
  ok = ecomet_field:check_type(#{<<"type">> => list, <<"subtype">> => link} ,true),

  % We have unsupported types, error should occur %
  ?assertError(invalid_type, ecomet_field:check_type(#{<<"type">> => qweasd, <<"subtype">> => link} ,true)),
  ?assertError(invalid_type, ecomet_field:check_type(#{<<"type">> => list, <<"subtype">> => gsg} ,true)),

  meck:unload(ecomet)
.


check_index_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),
  meck:expect(ecomet, edit_object, fun(_Object, _Map) -> ok end),

  % We are not changing index, should return ok %
  ok = ecomet_field:check_index(#{<<"index">> => none, <<"Album">> => <<"Meteora">>}, true),
  ok = ecomet_field:check_index(#{<<"Album">> => <<"Meteora">>, <<"Crypto">> => <<"RSA">>}, false),

  % %
  ok = ecomet_field:check_index(#{<<"index">> => {none, {1, 1}}, <<"Album">> => <<"Meteora">>}, true),
  ok = ecomet_field:check_index(#{<<"index">> => {none, 1}, <<"Album">> => <<"Meteora">>}, false),

  %supported types %
%%  [
%%    simple,
%%    '3gram',
%%    datetime
%%  ],
  % New indexes contain only supported types, should return ok%
  ok = ecomet_field:check_index(#{<<"index">> => {[simple, '3gram'], {1, 1}}, <<"Album">> => <<"Meteora">>}, false),
  ok = ecomet_field:check_index(#{<<"index">> => {[datetime, '3gram', datetime], {1, 1}}, <<"Album">> => <<"FolkLore">>},true),
  ok = ecomet_field:check_index(#{<<"index">> => {[datetime, datetime, datetime], {1, 1}}, <<"Album">> => <<"Divide">>}, true),

  % New indexes contain not supported types, error should occur %
  ?assertError(invalid_index_type, ecomet_field:check_index(#{<<"index">> => {[notvalidtype], {1, 1}} }, true)),
  ?assertError(invalid_index_type, ecomet_field:check_index(#{<<"index">> => {[validtype, it ,is, joke], {1, 1}} }, false)),


  meck:unload(ecomet)
.

check_default_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),
  meck:expect(ecomet, edit_object, fun(_Object, _Map) -> ok end),

  % We are not changing default field, should return ok %
  ok = ecomet_field:check_default(#{<<"STALKER">> => <<"SOC">>}, true),
  ok = ecomet_field:check_default(#{},false),

  % We are changing default field to none, should return ok %
  ok = ecomet_field:check_default(#{<<"default">> => {none, <<"notnone">>}}, true),
  ok = ecomet_field:check_default(#{<<"default">> => {none, <<"qwe">>}}, false),

%%  #{
%%    type => string,
%%    subtype => none,
%%    index => none,
%%    required => false,
%%    storage => disc,
%%    default => none,
%%    autoincrement => false
%%  })

  % Some preparation is done here %
  _Object0 = #{<<"default">> => {1, old}, <<"type">> => string, <<"subtype">> => none},
  _Object1 = #{<<"default">> => {<<"123">>, old}, <<"type">> => integer, <<"subtype">> => none},
  %error case
  _Object2 = #{<<"default">> => {new, old}, <<"type">> => float, <<"subtype">> => none},
   % error case
  _Object3 = #{<<"default">> => {qwe, old}, <<"type">> => bool, <<"subtype">> => none},
  _Object4 = #{<<"default">> => {new, old}, <<"type">> => atom, <<"subtype">> => none},
  % error case
  _Object5 = #{<<"default">> => {new, old}, <<"type">> => binary, <<"subtype">> => none},
  _Object6 = #{<<"default">> => {1, old}, <<"type">> => term, <<"subtype">> => none},

  %_Object8 = #{<<"default">> => {new, old}, <<"type">> => list, <<"subtype">> => integer},

  % We are changing default field with valid default_field value, should return ok %
  ok = ecomet_field:check_default(_Object0, false),
  ok = ecomet_field:check_default(_Object1, true),
  ok = ecomet_field:check_default(_Object4, false),
  ok = ecomet_field:check_default(_Object6, true),

  % We are changing default field with invalid default_field value, error should occur %
  ?assertError(invalid_default_value, ecomet_field:check_default(_Object2, true)),
  ?assertError(invalid_default_value, ecomet_field:check_default(_Object3, false)),
  ?assertError(invalid_default_value, ecomet_field:check_default(_Object5, true)),

  meck:unload(ecomet)

.