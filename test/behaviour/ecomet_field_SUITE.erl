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
  IsEmptyTrue = true,
  IsEmptyFalse = false,
  % We are not changing name, should return false %
  false = ecomet_field:check_name(#{<<".folder">> => {2, 1}}, IsEmptyTrue),
  false = ecomet_field:check_name(#{<<".notname">> => <<"whysohard">>}, IsEmptyFalse),

  % We are trying to change name, while is_empty == false, error should occur%
  ?assertError(has_objects, ecomet_field:check_name(#{<<".name">> => {<<"new">>, <<"old">>}}, IsEmptyFalse)),
  ?assertError(has_objects, ecomet_field:check_name(#{<<".name">> => {<<"help">>, <<"me">>}}, IsEmptyFalse)),

  % is_empty == true %
  % We are trying to change on incorrect name %
  ?assertError(invalid_name, ecomet_field:check_name(#{<<".name">> => {<<"S_eE-mS_Va119(">>, none}}, IsEmptyTrue)),
  ?assertError(invalid_name, ecomet_field:check_name(#{<<".name">> => {<<".Not.valid">>, none}}, IsEmptyTrue)),

  % We change name on valid %
  true = ecomet_field:check_name(#{<<".name">> => {<<".H-a_R--r9y">>, none}, <<".folder">> => 12}, IsEmptyTrue),
  true = ecomet_field:check_name(#{<<".name">> => {<<"._-_-_-_-">>, <<"Alucard">>}, <<".folder">> => 1998}, IsEmptyTrue),


  meck:unload(ecomet),
  meck:unload(ecomet_pattern),

  ok.


check_folder_test(_Config) ->
  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  IsEmptyTrue = true,
  IsEmptyFalse = false,
  % We are not changing pattern, should return false%
  false = ecomet_field:check_folder(#{<<".notfolder">> => okey}, IsEmptyTrue),
  false = ecomet_field:check_folder(#{<<"Uchiha">> => <<"Itachi">>}, IsEmptyFalse),

  % We are creating smth(object), should return true %
  true = ecomet_field:check_folder(#{<<".folder">> => {{1, 2}, none}}, IsEmptyTrue),
  true = ecomet_field:check_folder(#{<<".folder">> => {{4, 3}, none}, <<"Linkin">> => <<"Park">>}, IsEmptyFalse),

  % We trying to change folder or pattern, error should occur %
  ?assertError(cannot_change_pattern, ecomet_field:check_folder(#{<<".folder">> => {{1, 2}, {3, 4}}}, IsEmptyFalse)),
  ?assertError(cannot_change_pattern, ecomet_field:check_folder(#{<<".folder">> => {{2, 5}, {5, 6}}}, IsEmptyTrue)),

  meck:unload(ecomet)
.

check_storage_test(_Config) ->
  meck:new([ecomet, ecomet_pattern], [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  % PatternID == ?RAMLOCAL or ?RAM or none%
  meck:expect(ecomet_pattern, get_storage, fun(PatternID) -> PatternID end),

  IsEmptyTrue = true,
  IsEmptyFalse = false,
  % We are not changing storage, should return false %
  false = ecomet_field:check_storage(#{<<"singer">> => {<<"avril">>, <<"lavigne">>}}, IsEmptyFalse),
  false = ecomet_field:check_storage(#{1 => 2}, IsEmptyTrue),

  % Object or smth not empty, error should occur %
  ?assertError(has_objects, ecomet_field:check_storage(#{<<".storage">> => {?DISC, none}}, IsEmptyFalse)),
  ?assertError(has_objects, ecomet_field:check_storage(#{<<".storage">> => {?RAMLOCAL, ?RAM}}, IsEmptyFalse)),

  % Mecking ecomet_pattern:get_storage
  % {ok,PatternID} = ecomet:read_field(Object,<<".folder">>),
  % PatternStorage = ecomet_pattern:get_storage(PatternID),
  % In our case <<.folder>> contain PatternStorage type and ecomet_pattern:get_storage() just return it,
  % It is done for testing purposes

  % is_empty == true %
  % Storage == [?RAMLOCAL,?RAM,?RAMDISC,?DISC] or whatever  %
  % NewStorage = RAMDISC or NewStorage = ?DISC %
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<".storage">> => {?RAMDISC, none}, <<".folder">> => ?RAMLOCAL},IsEmptyTrue)),
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<".storage">> => {?RAMDISC, none}, <<".folder">> => ?RAM}, IsEmptyTrue)),
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<".storage">> => {?DISC, none}, <<".folder">> => ?RAMLOCAL}, IsEmptyTrue)),
  ?assertError(memory_only_pattern, ecomet_field:check_storage(#{<<".storage">> => {?DISC, none}, <<".folder">> => ?RAM}, IsEmptyTrue)),

  true = ecomet_field:check_storage(#{<<".storage">> => {?RAMDISC, none}, <<".folder">> => ?RAMDISC}, IsEmptyTrue),
  true = ecomet_field:check_storage(#{<<".storage">> => {?DISC, none}, <<".folder">> => ?DISC}, IsEmptyTrue),

  % NewStorage != RAMDISC and NewStorage != ?DISC %
  true = ecomet_field:check_storage(#{<<".storage">> => {?RAM, ?RAMLOCAL}, <<".folder">> => ?RAMLOCAL}, IsEmptyTrue),
  true = ecomet_field:check_storage(#{<<".storage">> => {?RAMLOCAL, ?DISC}, <<".folder">> => <<"MyFolder">>},IsEmptyTrue),
  ?assertError(invalid_storage_type, ecomet_field:check_storage(#{<<".storage">> => {<<"HDFS">>, <<"SSD">>}, <<"Green">> => <<"Day">>}, IsEmptyTrue)),
  ?assertError(invalid_storage_type, ecomet_field:check_storage(#{<<".storage">> => {<<"Cloud">>, ?RAM}}, IsEmptyTrue)),

  meck:unload([ecomet, ecomet_pattern]),
  ok
.

to_schema_test(_Config) ->
  Params1 = #{
    <<".type">> => integer,
    <<".subtype">> => none,
    <<".index">> => none,
    <<".required">> => false,
    <<".storage">> => ram,
    <<".default">> => none,
    <<".autoincrement">> => true,
    <<"myfield">> => <<"plsnotcrash">>
  },

  Params2 = #{
    <<".type">> => link,
    <<".required">> => true,
    <<".storage">> => disc,
    <<".default">> => none,
    <<"myfield">> => <<"tired">>
  },

  Ans1 = #{
    type => integer,
    subtype => none,
    index => none,
    required => false,
    storage => ram,
    default => none,
    autoincrement => true
  },
  Ans1 = ecomet_field:to_schema(Params1),

  Ans2 = #{
    type => link,
    subtype => none,
    index => none,
    required => true,
    storage => disc,
    default => none,
    autoincrement => false
  },
  Ans2 = ecomet_field:to_schema(Params2),

  ok.


check_type_test(_Config) ->
  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),

  IsEmptyTrue = true,
  IsEmptyFalse = false,
  % We are not changing type and subtype, should return false %
  false = ecomet_field:check_type(#{ <<"Johnny">> => <<"Depp">>}, IsEmptyTrue),
  false = ecomet_field:check_type(#{<<"Cyber">> => <<"Punc">>}, IsEmptyFalse),
  false = ecomet_field:check_type(#{}, IsEmptyTrue),

  % We are trying to change type or subtype, but there already created object with the schema %
  ?assertError(has_objects, ecomet_field:check_type(#{<<".type">> => {bool, atom}, <<".subtype">> => {new, old}}, IsEmptyFalse)),
  ?assertError(has_objects, ecomet_field:check_type(#{<<".type">> => {atom, term}}, IsEmptyFalse)),
  ?assertError(has_objects, ecomet_field:check_type(#{<<".subtype">> => {float, bool}}, IsEmptyFalse)),

  % is_empty == true %
  % We are trying to change type or subtype or we have valid types %
  % If the type is not a list then it doesn't matter what is a subtype
  true = ecomet_field:check_type(#{<<".type">> => term, <<".subtype">> => {new, old}}, IsEmptyTrue),
  true = ecomet_field:check_type(#{<<".type">> => list, <<".subtype">> => binary} ,IsEmptyTrue),
  true = ecomet_field:check_type(#{<<".type">> => atom}, IsEmptyTrue),
  true = ecomet_field:check_type(#{<<".type">> => list, <<".subtype">> => link} ,IsEmptyTrue),

  % We have unsupported types, error should occur %
  ?assertError(invalid_type, ecomet_field:check_type(#{<<".type">> => qweasd, <<".subtype">> => link} ,IsEmptyTrue)),
  ?assertError(invalid_type, ecomet_field:check_type(#{<<".type">> => list, <<".subtype">> => gsg} ,IsEmptyTrue)),

  meck:unload(ecomet),

  ok.


check_index_test(_Config) ->
  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),
  meck:expect(ecomet, edit_object, fun(_Object, _Map) -> ok end),

  IsEmptyTrue = true,
  IsEmptyFalse = false,
  % We are not changing index, should return false %
  false = ecomet_field:check_index(#{ <<"Album">> => <<"Meteora">>}, IsEmptyTrue),
  false = ecomet_field:check_index(#{<<"Album">> => <<"Meteora">>, <<"Crypto">> => <<"RSA">>}, IsEmptyFalse),

  % %
  true = ecomet_field:check_index(#{<<".index">> => {none, [simple]}, <<"Album">> => <<"Meteora">>}, IsEmptyTrue),
  true = ecomet_field:check_index(#{<<".index">> => {none, ['3gram']}, <<"Album">> => <<"Meteora">>}, IsEmptyFalse),

  %supported types %
%%  [
%%    simple,
%%    '3gram',
%%    datetime
%%  ],
  % New indexes contain only supported types, should return true%
  true = ecomet_field:check_index(#{<<".index">> => {[simple, '3gram'], [datetime]}, <<"Album">> => <<"Meteora">>}, IsEmptyFalse),
  true = ecomet_field:check_index(#{<<".index">> => {[datetime, '3gram', datetime], none}, <<"Album">> => <<"FolkLore">>},IsEmptyTrue),
  true = ecomet_field:check_index(#{<<".index">> => {[datetime, datetime, datetime], none}, <<"Album">> => <<"Divide">>}, IsEmptyTrue),

  % New indexes contain not supported types, error should occur %
  ?assertError(invalid_index_type, ecomet_field:check_index(#{<<".index">> => {[notvalidtype], [simple]} }, IsEmptyTrue)),
  ?assertError(invalid_index_type, ecomet_field:check_index(#{<<".index">> => {[validtype, it ,is, joke], [datetime]} }, IsEmptyFalse)),


  meck:unload(ecomet)
.

check_default_test(_Config) ->
  meck:new(ecomet, [no_link]),
  meck:expect(ecomet, field_changes, fun(Object, Key) -> maps:get(Key, Object, none) end),
  meck:expect(ecomet, edit_object, fun(_Object, Map) -> put(default, maps:get(<<".default">>, Map)), ok end),

  IsEmptyTrue = true,
  IsEmptyFalse = false,
  % We are not changing default field, should return false %
  false = ecomet_field:check_default(#{<<"STALKER">> => <<"SOC">>}, IsEmptyTrue),
  false = ecomet_field:check_default(#{}, IsEmptyFalse),

  % We are changing default field to none, should return true %
  true = ecomet_field:check_default(#{<<".default">> => {none, <<"notnone">>}}, IsEmptyTrue),
  true = ecomet_field:check_default(#{<<".default">> => {none, <<"qwe">>}}, IsEmptyFalse),

%%  #{
%%    type => string,
%%    subtype => none,
%%    index => none,
%%    required => false,
%%    storage => disc,
%%    default => none,
%%    autoincrement => false
%%  })

  % We are changing default field with valid default_field value, should return ok %
  true = ecomet_field:check_default(#{
    <<".default">> => {1, old},
    <<".type">> => string,
    <<".subtype">> => none
  }, IsEmptyFalse),
  <<"1">> = get(default),

  true = ecomet_field:check_default(#{
    <<".default">> => {<<"123">>, old},
    <<".type">> => integer,
    <<".subtype">> => none
  }, IsEmptyTrue),
  123 = get(default),

  true = ecomet_field:check_default(#{
    <<".default">> => {new, old},
    <<".type">> => atom,
    <<".subtype">> => none
  }, IsEmptyFalse),
  new = get(default),

  true = ecomet_field:check_default(#{
    <<".default">> => {1, old},
    <<".type">> => term,
    <<".subtype">> => none
  }, IsEmptyTrue),
  1 = get(default),

  % We are changing default field with invalid default_field value, error should occur %
  ?assertError(invalid_default_value, ecomet_field:check_default(#{
    <<".default">> => {new, old},
    <<".type">> => float,
    <<".subtype">> => none
  }, IsEmptyTrue)),

  ?assertError(invalid_default_value, ecomet_field:check_default(#{
    <<".default">> => {qwe, old},
    <<".type">> => bool,
    <<".subtype">> => none
  }, IsEmptyFalse)),

  ?assertError(invalid_default_value, ecomet_field:check_default(#{
    <<".default">> => {new, old},
    <<".type">> => binary,
    <<".subtype">> => none
  }, IsEmptyTrue)),

  meck:unload(ecomet)

.

on_create_test(_Config) ->

  meck:new([ecomet, ecomet_pattern]),
  meck:expect(ecomet, field_changes, fun(Object, Field) -> maps:get(Field, Object, none) end),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, element(1, maps:get(Field, Object))} end),
  meck:expect(ecomet_pattern, append_field, fun(_patternID, _Name, _COnfig) -> ok end),
  meck:expect(ecomet_pattern, remove_field, fun(_PatternID, _OldName) -> ok end),
  meck:expect(ecomet_pattern, get_storage, fun(PatternID) -> PatternID end),
  meck:expect(ecomet, edit_object, fun(_Object, _Map) -> ok end),

  % <<.name>> => valid name %
  % <<".folder">> => PatternStorage DISC%
  % <<".storage">> => %
  % <<".type">>  => valid type %
  % <<".subtype">> => does not need, because <<".type">> != list %
  % <<".index">> => valid index%
  % <<".default">> => valid default value, accordingly to type %
  Object1 = #{<<".name">> => {<<".faceplate">>, none},
    <<".folder">> => {?DISC, none},
    <<".storage">> => {?RAMDISC, ?DISC},
    <<".type">> => term,
    <<".subtype">> => none,
    <<".index">> => {[simple], none},
    <<".default">> => {<<"newdef">>, none}
  },
  ok = ecomet_field:on_create(Object1),

  % <<.name>> => valid name %
  % <<".folder">> => PatternStorage RAMLOCAL%
  % <<".storage">> => We cannot change storage type to RAMDISC
  % while PatternStorage is RAMLOCAL %
  % <<".type">>  => valid type %
  % <<".subtype">> => does not need, because <<".type">> != list %
  % <<".index">> => valid index%
  % <<".default">> => valid default value, accordingly to type %
  Object2 = #{<<".name">> => {<<".google">>, none},
    <<".folder">> => {?RAMLOCAL, none},
    <<".storage">> => {?RAMDISC, none},
    <<".type">> => string,
    <<".subtype">> => none,
    <<".index">> => {[simple, '3gram'], none},
    <<".default">> => {123, none}
  },
  ?assertError(memory_only_pattern, ecomet_field:on_create(Object2)),

  % <<.name>> => valid name %
  % <<".folder">> => PatternStorage RAMDISC%
  % <<".storage">> => %
  % <<".type">>  => valid type %
  % <<".subtype">> => does not need, because <<".type">> != list %
  % <<".index">> => invalid index%
  % <<".default">> => valid default value, accordingly to type %
  Object3 = #{<<".name">> => {<<".yahoo">>, none},
    <<".folder">> => {?RAMDISC, none},
    <<".storage">> => {?DISC, none},
    <<".type">> => integer,
    <<".subtype">> => none,
    <<".index">> => {[notsimple, datetime], none},
    <<".default">> => {<<"123">>, none}
  },
  ?assertError(invalid_index_type, ecomet_field:on_create(Object3)),
  meck:unload([ecomet, ecomet_pattern]),

  ok.


on_edit_test(_Config) ->

  meck:new([ecomet, ecomet_pattern]),
  meck:expect(ecomet, field_changes, fun(Object, Field) -> maps:get(Field, Object, none) end),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, element(1, maps:get(Field, Object, {none}))} end),
  meck:expect(ecomet_pattern, append_field, fun(_patternID, _Name, _COnfig) -> ok end),
  meck:expect(ecomet_pattern, remove_field, fun(_PatternID, _OldName) -> ok end),
  meck:expect(ecomet_pattern, get_storage, fun(PatternID) -> PatternID end),
  meck:expect(ecomet, edit_object, fun(_Object, _Map) -> ok end),
  meck:expect(ecomet_pattern, is_empty, fun(_PatternID) -> get(is_empty) end),

  % in this case testing is similar to on_create_test %
  put(is_empty, true),
  Object1 = #{<<".name">> => {<<".faceplate">>, <<".zeinet">>},
    <<".folder">> => {?RAMLOCAL, none},
    <<".storage">> => {?RAM, ?DISC},
    <<".type">> => term,
    <<".subtype">> => none,
    <<".index">> => {[simple], datetime},
    <<".default">> => {<<"newdef">>, <<"newdef">>}
  },
  ok = ecomet_field:on_edit(Object1),

  % In this case we have non empty object, so we cannot change it%
  put(is_empty, false),
  Object2 = #{<<".name">> => {<<".faceplate">>, <<".zeinet">>},
    <<".folder">> => {?DISC, none},
    <<".storage">> => {?RAMDISC, ?DISC},
    <<".type">> => string,
    <<".subtype">> => none,
    <<".index">> => {[datetime], datetime},
    <<".default">> => {<<"newdef">>, <<"newdef">>}
  },
  ?assertError(has_objects, ecomet_field:on_edit(Object2)),

  meck:unload([ecomet, ecomet_pattern])
.


on_delete_test(_Config) ->

  meck:new([ecomet, ecomet_pattern]),
  meck:expect(ecomet, open,
    fun(_PatternID, _None) ->
     case get(open) of
      false -> ?ERROR(object_deleted);
      true -> ok
     end
    end),

  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object)} end),
  meck:expect(ecomet_pattern, remove_field, fun(_PatternID, _Name) -> ok end),
  meck:expect(ecomet_pattern, is_empty, fun(PatternID) -> PatternID end),

  % Mecking: Some checks are performed only when a field is deleted but not its
  % pattern. Whe the pattern is deleted the contained fields are deleted without
  % these additional checks. During delete operation to check if it is the containing
  % pattern delete operation the code tries to open the pattern, we meck this procedure
  % to make a difference between the two cases
  % Deleting object %
  put(open, false),
  ok = ecomet_field:on_delete(#{<<".folder">> => true, <<".name">> => <<"NazGul">>}),
  % Incorrectly deleting object %
  put(open, true),
  ?assertError(has_objects, ecomet_field:on_delete(#{<<".folder">> => false, <<".name">> => <<"NazGul">>})),
  ?assertError(has_objects,ecomet_field:on_delete(#{<<".folder">> => sfsf, <<".name">> => <<"NazGul">>})),


  meck:unload([ecomet_pattern, ecomet]),

  ok.