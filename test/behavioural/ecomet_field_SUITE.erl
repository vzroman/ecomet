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

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([all/0, group/0, init_per_suite/1, end_per_suite/1]).

-export([
  check_folder_test/1,
  check_storage_test/1,
  check_type_test/1,
  check_index_test/1,
  check_default_test/1,
  to_schema_test/1,
  check_name_test/1,
  on_create_test/1,
  on_edit_test/1,
  on_delete_test/1
]).

all() ->
  [
    check_name_test,
    check_folder_test,
    check_storage_test,
    check_type_test,
    check_index_test,
    check_default_test,
    to_schema_test,
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
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  meck:new(ecomet_pattern),
  meck:expect(ecomet_pattern, remove_field, fun(_PatternID, _OldName) -> ok end),

  %% #{<<".name">> => {<<"Old">>, <<"New">>}, <<".folder">> => 12},

  % We are not changing name, should return ok %
  ok = ecomet_field:check_name(#{<<".folder">> => 12}, true),
  ok = ecomet_field:check_name(#{<<".notname">> => <<"whysohard">>}, false),

  % We are trying to change name, while is_empty == false, error should occur%
  ?assertError(has_objects, ecomet_field:check_name(#{<<".name">> => {<<"new">>, <<"old">>}}, _IsEmpty = false)),
  ?assertError(has_objects, ecomet_field:check_name(#{<<".name">> => {<<"help">>, <<"me">>}}, _IsEmpty = false)),

  % is_empty == true %
  % We are trying to change on incorrect name %
  ?assertError(invalid_name, ecomet_field:check_name(#{<<".name">> => {<<"S_eE-mS_Va119(">>, none}}, true)),
  ?assertError(invalid_name, ecomet_field:check_name(#{<<".name">> => {<<".Not.valid">>, none}}, true)),

  % We change name on valid %
  ok = ecomet_field:check_name(#{<<".name">> => {<<".H-a_R--r9y">>, none}, <<".folder">> => 12}, true),
  ok = ecomet_field:check_name(#{<<".name">> => {<<"._-_-_-_-">>, <<"Alucard">>}, <<".folder">> => 1998}, true),


  meck:unload(ecomet),
  meck:unload(ecomet_pattern),

  ok.


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
  ok = ecomet_field:check_storage(#{<<"singer">> => {<<"avril">>, <<"lavigne">>}}, _IsEmpty0=false),
  ok = ecomet_field:check_storage(#{1 => 2}, _IsEmpty1=true),

  % Object or smth not empty, error should occur %
  ?assertError(has_objects, ecomet_field:check_storage(#{<<"storage">> => {<<"new">>, <<"old">>}}, _IsEmpty0=false)),
  ?assertError(has_objects, ecomet_field:check_storage(#{<<"storage">> => {<<"job">>, <<"difficult">>}}, _IsEmpty0=false)),

  % is_empty == true %
  % Storage == [?RAMLOCAL,?RAM,?RAMDISC,?DISC] or whatever  %
  % NewStorage = RAMDISC; NewStorage = ?DISC %
  % TODO. Add a comment about using the folder with mecking ecomet_pattern:get_storage
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<"storage">> => {?RAMDISC, none}, <<".folder">> => ?RAMLOCAL},true)),
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<"storage">> => {?RAMDISC, none}, <<".folder">> => ?RAM}, true)),
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<"storage">> => {?DISC, none}, <<".folder">> => ?RAMLOCAL}, true)),
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<"storage">> => {?DISC, none}, <<".folder">> => ?RAM}, true)),

  ok = ecomet_field:check_storage(#{<<"storage">> => {?RAMDISC, none}, <<".folder">> => ?RAMDISC}, true),
  ok = ecomet_field:check_storage(#{<<"storage">> => {?DISC, none}, <<".folder">> => ?DISC}, true),

  % NewStorage != RAMDISC; NewStorage != ?DISC %
  % TODO. Use expected values for storage types
  ok = ecomet_field:check_storage(#{<<"storage">> => {?RAM, <<"FlashCard">>}, <<".folder">> => ?RAMLOCAL}, true),
  ok = ecomet_field:check_storage(#{<<"storage">> => {?RAMLOCAL, <<"Safe">>}, <<".folder">> => <<"MyFolder">>},true),
  ?assertError(invalid_storage_type, ecomet_field:check_storage(#{<<"storage">> => {<<"HDFS">>, <<"SSD">>}, <<"Green">> => <<"Day">>}, true)),
  ?assertError(invalid_storage_type, ecomet_field:check_storage(#{<<"storage">> => {<<"Cloud">>, ?RAM}}, true)),

  meck:unload(ecomet_pattern),
  meck:unload(ecomet)
.

to_schema_test(_Config) ->
  % TODO. Use valid parameters and values
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
  Ans2 = ecomet_field:to_schema(Params2),

  ok.


check_type_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  % We are not changing type and subtype, should return ok %
  ok = ecomet_field:check_type(#{ <<"Johnny">> => <<"Depp">>}, _IsEmpty1=true),
  ok = ecomet_field:check_type(#{<<"Cyber">> => <<"Punc">>}, _IsEmpty0=false),
  ok = ecomet_field:check_type(#{}, _IsEmpty=true),

  % We are trying to change type or subtype, but there already created object with the schema %
  ?assertError(has_objects, ecomet_field:check_type(#{<<"type">> => {<<"New">>, <<"Old">>}, <<"subtype">> => {new, old}}, _IsEmpty0=false)),
  ?assertError(has_objects, ecomet_field:check_type(#{<<"type">> => {<<"New">>, <<"Old">>}}, _IsEmpty0=false)),
  ?assertError(has_objects, ecomet_field:check_type(#{<<"subtype">> => {<<"New">>, <<"Old">>}}, _IsEmpty0=false)),

  % is_empty == true %
  % We are trying to change type or subtype or we have valid types %
  % If the type is not a list then it doesn't matter what is a subtype
  ok = ecomet_field:check_type(#{<<"type">> => term, <<"subtype">> => {new, old}}, _IsEmpty1=true),
  ok = ecomet_field:check_type(#{<<"type">> => list, <<"subtype">> => binary} ,_IsEmpty1=true),
  ok = ecomet_field:check_type(#{<<"type">> => atom}, _IsEmpty=true),
  ok = ecomet_field:check_type(#{<<"type">> => list, <<"subtype">> => link} ,_IsEmpty1=true),

  % We have unsupported types, error should occur %
  ?assertError(invalid_type, ecomet_field:check_type(#{<<"type">> => qweasd, <<"subtype">> => link} ,_IsEmpty1=true)),
  ?assertError(invalid_type, ecomet_field:check_type(#{<<"type">> => list, <<"subtype">> => gsg} ,_IsEmpty1=true)),

  meck:unload(ecomet),

  ok.


check_index_test(_Config) ->

  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),
  meck:expect(ecomet, edit_object, fun(_Object, _Map) -> ok end),

  % We are not changing index, should return ok %
  ok = ecomet_field:check_index(#{ <<"Album">> => <<"Meteora">>}, _IsEmpty1=true),
  ok = ecomet_field:check_index(#{<<"Album">> => <<"Meteora">>, <<"Crypto">> => <<"RSA">>}, _IsEmpty0=false),

  % %
  ok = ecomet_field:check_index(#{<<"index">> => {none, [simple]}, <<"Album">> => <<"Meteora">>}, _IsEmpty1=true),
  ok = ecomet_field:check_index(#{<<"index">> => {none, ['3gram']}, <<"Album">> => <<"Meteora">>}, _IsEmpty0=false),

  %supported types %
%%  [
%%    simple,
%%    '3gram',
%%    datetime
%%  ],
  % New indexes contain only supported types, should return ok%
  ok = ecomet_field:check_index(#{<<"index">> => {[simple, '3gram'], [datetime]}, <<"Album">> => <<"Meteora">>}, false),
  ok = ecomet_field:check_index(#{<<"index">> => {[datetime, '3gram', datetime], none}, <<"Album">> => <<"FolkLore">>},true),
  ok = ecomet_field:check_index(#{<<"index">> => {[datetime, datetime, datetime], none}, <<"Album">> => <<"Divide">>}, true),

  % New indexes contain not supported types, error should occur %
  ?assertError(invalid_index_type, ecomet_field:check_index(#{<<"index">> => {[notvalidtype], [simple]} }, true)),
  ?assertError(invalid_index_type, ecomet_field:check_index(#{<<"index">> => {[validtype, it ,is, joke], [datetime]} }, false)),


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
  ok = ecomet_field:check_default(#{
    <<"default">> => {1, old},
    <<"type">> => string,
    <<"subtype">> => none
  }, _IsEmpty=false),

  ok = ecomet_field:check_default(_Object1, true),
  ok = ecomet_field:check_default(_Object4, false),
  ok = ecomet_field:check_default(_Object6, true),

  % We are changing default field with invalid default_field value, error should occur %
  ?assertError(invalid_default_value, ecomet_field:check_default(_Object2, true)),
  ?assertError(invalid_default_value, ecomet_field:check_default(_Object3, false)),
  ?assertError(invalid_default_value, ecomet_field:check_default(_Object5, true)),

  % TODO. The check_default converts the new value according to the field type. Add the check for the side effect

  meck:unload(ecomet)

.

on_create_test(_Config) ->

  % TODO. Comment the code, add negative tests, do the create via true object create

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
    <<"index">> => {[simple], none},
    <<"default">> => {<<"newdef">>, <<"newdef">>}
  },

  ok = ecomet_field:on_create(Object1),

  meck:unload([ecomet, ecomet_pattern]),

  ok.


on_edit_test(_Config) ->
  % TODO. Comment the code, add negative tests

  meck:new([ecomet, ecomet_pattern]),
  meck:expect(ecomet, field_changes, fun(Object, Field) -> maps:get(Field, Object, none) end),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, element(1, maps:get(Field, Object, {none}))} end),
  meck:expect(ecomet_pattern, append_field, fun(_patternID, _Name, _COnfig) -> ok end),
  meck:expect(ecomet_pattern, remove_field, fun(_PatternID, _OldName) -> ok end),
  meck:expect(ecomet_pattern, get_storage, fun(PatternID) -> PatternID end),
  meck:expect(ecomet, edit_object, fun(_Object, _Map) -> ok end),
  meck:expect(ecomet_pattern, is_empty, fun(_PatternID) -> get(is_empty) end),

  put(is_empty, true),

  Object1 = #{<<".name">> => {<<".faceplate">>, none},
    <<"storage">> => {?RAMDISC, ?DISC},
    <<"type">> => term,
    <<"subtype">> => {},
    <<"index">> => {[simple], datetime},
    <<"default">> => {<<"newdef">>, <<"newdef">>}

  },

  ok = ecomet_field:on_edit(Object1),

  put(is_empty, false),

  Object2 = #{
  },

  ok = ecomet_field:on_edit(Object2),

  meck:unload([ecomet, ecomet_pattern])
.


on_delete_test(_Config) ->

  % TODO. Comment the code, add negative tests
  meck:new([ecomet, ecomet_pattern]),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  meck:expect(ecomet_pattern, remove_field, fun(_PatternID, _Name) -> ok end),
  meck:expect(ecomet_pattern, is_empty, fun(PatternID) -> PatternID end),
  % Deleting object %
  ecomet_field:on_delete(#{<<".folder">> => true, <<".name">> => <<"NazGul">>}),
  % Incorrectly deleting object %
  ?assertError(has_objects, ecomet_field:on_delete(#{<<".folder">> => false, <<".name">> => <<"NazGul">>})),
  ?assertError(has_objects,ecomet_field:on_delete(#{<<".folder">> => sfsf, <<".name">> => <<"NazGul">>})),



  meck:unload([ecomet_pattern, ecomet]),

  ok.