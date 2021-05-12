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
-module(ecomet_folder_SUITE).


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
  rights_changes_test/1,
  apply_rights_test/1,
  apply_recursion_test/1,
  on_create_test/1,
  on_edit_test/1,
  on_delete_test/1
]).

all() ->
  [
    check_database_test,
    inherit_rights_test,
    recursive_rights_test,
    rights_changes_test,
    apply_rights_test,
    apply_recursion_test,
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


check_database_test(_Config) ->
  ecomet_user:on_init_state(),
  MyDB = ecomet:create_object(#{
    <<".name">> => <<"HelpMeDB">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.database">>)
  }),


  Folder1 = ecomet:create_object(#{
    <<".name">> => <<"Folder1">>,
    <<".folder">> => ?OID(<<"/root">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".database">> => ?OID(MyDB)
  }),

  Folder2 = ecomet:create_object(#{
    <<".name">> => <<"SaveMeFolder">>,
    <<".folder">> => ?OID(Folder1),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),
  % We are trying to change database(unmount old and mount new),
  % but unmount only allowed when folder is empty
  ?assertError({contains_objects, _}, ecomet:edit_object(Folder1, #{<<".database">> => none})),
  ecomet:delete_object(Folder2),
  ecomet:edit_object(Folder1, #{<<".database">> => none}),
  none = ecomet_schema:get_mounted_db(?OID(Folder1)),
  ecomet:delete_object(Folder1),
  ecomet:delete_object(MyDB),

  ok
.


inherit_rights_test(_Config) ->
  meck:new([ecomet]),
  meck:expect(ecomet, read_fields, fun(Object, Fields) ->
    maps:map(fun(Key, Default) ->
      maps:get(Key, Object, Default)
    end, Fields)
  end),
  meck:expect(ecomet, edit_object, fun(_Object, Fields) -> put(edited, Fields), ok end),

  % Object does not have {<<.readgroups>>, <<.writegroups>>,
  % <<.contentreadgroups>>, <<.contentwritegroups>>} fields %
  ecomet_folder:inherit_rights(#{}),
  #{
    <<".contentreadgroups">> := none,
    <<".contentwritegroups">> := none
  } = get(edited),

  % Object has  {<<.readgroups>>, <<.writegroups>>} fields,
  % but does not have {<<.contentreadgroups>>, <<.contentwritegroups>>} fields %
  ecomet_folder:inherit_rights(#{
    <<".readgroups">> => [{1, 2}],
    <<".writegroups">> => [{2, 3}]
  }),
  #{
    <<".contentreadgroups">> := [{1, 2}],
    <<".contentwritegroups">> := [{2, 3}]
  } = get(edited),

  % Object has  {<<.contentreadgroups>>, <<.contentwritegroups>>} fields,
  % but does not have {<<.readgroups>>, <<.writegroups>>} fields %
  ecomet_folder:inherit_rights(#{
    <<".contentreadgroups">> => [{10, 10}],
    <<".contentwritegroups">> => [{20, 20}]
  }),
  #{
    <<".contentreadgroups">> := [{10, 10}],
    <<".contentwritegroups">> := [{20, 20}]
  } = get(edited),

  % Object has  {<<.contentreadgroups>>, <<.contentwritegroups>>,
  % <<.readgroups>>, <<.writegroups>>} fields %
  ecomet_folder:inherit_rights(#{
    <<".readgroups">> => [{1, 2}],
    <<".writegroups">> => [{2, 3}],
    <<".contentreadgroups">> => [{10, 10}],
    <<".contentwritegroups">> => [{20, 20}]
  }),
  #{
    <<".contentreadgroups">> := [{10, 10}],
    <<".contentwritegroups">> := [{20, 20}]
  } = get(edited),

  meck:unload([ecomet]),
  ok
.


recursive_rights_test(_Config) ->
  ok
.

rights_changes_test(_Config) ->
  meck:new(ecomet),
  meck:expect(ecomet, field_changes, fun(Object, Field) -> maps:get(Field, Object, none) end),
%%  #{
%%    <<".readgroups">> => {[]},
%%    <<".writegroups">> => []
%%  },
  none = ecomet_folder:rights_changes(#{ <<".writegroups">> => {[{1, 2}], none}}, <<".readgroups">>),
  none = ecomet_folder:rights_changes(#{<<".readgroups">> => {none, none}}, <<".readgroups">>),
  {[{1, 5}, {3, 4}],[]} = ecomet_folder:rights_changes(#{
    <<".readgroups">> => {[{3, 4}, {1, 5}, {3, 4}], none}}, <<".readgroups">>),

  {[], [{1, 5}, {3, 4}]} = ecomet_folder:rights_changes(#{
    <<".readgroups">> => {none, [{3, 4}, {1, 5}, {3, 4}]}}, <<".readgroups">>),
  meck:unload(ecomet),
  ok.

apply_rights_test(_Config) ->
  meck:new([ecomet]),
  meck:expect(ecomet, read_field, fun(Object, Field) -> {ok, maps:get(Field, Object, none)} end),
  meck:expect(ecomet, edit_object, fun(_Object, Fields) -> put(edit, Fields), ok end),
  Object = #{
    <<"exclude_patterns">> => [{2, 5}, {1, 3}, {2, 5}]
  },
  Changes = #{
    <<"exclude_patterns">> => {[{1, 2}, {3, 4}], [{5, 6}, {7, 8}]},
    <<"only_patterns">> => {[{10, 20}, {20, 30}], [{30, 40}, {40, 50}]}
  },
  Additional = [{<<".database">>, {5, 5}}],
  ecomet_folder:apply_rights(Object, Changes, Additional),

  Expected =#{
    <<"exclude_patterns">> => [{1, 2}, {1, 3}, {2, 5}, {3, 4}],
    <<"only_patterns">> =>[{10, 20}, {20, 30}],
    <<".database">> => {5, 5}
  },

  Expected = get(edit),
  meck:unload([ecomet]),
  ok.



apply_recursion_test(_Config) ->
  ok.



on_create_test(_Config) ->
  ecomet_user:on_init_state(),
  MyUselessDB = ecomet:create_object(#{
    <<".name">> => <<"VeryUseless">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.database">>)
  }),

  NotSoUseless = ecomet:create_object(#{
    <<".name">> => <<"NotSoUseless">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.database">>)
  }),

  Folder1 = ecomet:create_object(#{
    <<".name">> => <<"Chester">>,
    <<".folder">> => ?OID(<<"/root">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".database">> => ?OID(MyUselessDB)
  }),

  %ct:pal("db for /root/Chester ~p",[ecomet_schema:get_mounted_folder(<<"/root/Chester/some">>)]),
  'VeryUseless' = ecomet_schema:get_mounted_db(?OID(Folder1)),

  Folder2 = ecomet:create_object(#{
    <<".name">> => <<"LightBringer">>,
    <<".folder">> => ?OID(Folder1),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),

  %ecomet:delete_object(Folder2),
  ?assertError({contains_objects, _}, ecomet:edit_object(Folder1, #{<<".database">> => ?OID(NotSoUseless)})) ,
  ecomet:delete_object(Folder2),

  ecomet:delete_object(Folder1),
  ecomet:delete_object(MyUselessDB),
  ecomet:delete_object(NotSoUseless),
  ok
.

on_edit_test(_Config) ->
  ecomet_user:on_init_state(),
  MyUselessDB = ecomet:create_object(#{
    <<".name">> => <<"VeryUseless">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.database">>)
  }),

  NotSoUseless = ecomet:create_object(#{
    <<".name">> => <<"NotSoUseless">>,
    <<".folder">> => ?OID(<<"/root/.databases">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.database">>)
  }),

  Folder1 = ecomet:create_object(#{
    <<".name">> => <<"Chester">>,
    <<".folder">> => ?OID(<<"/root">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".database">> => ?OID(MyUselessDB)
  }),

  %ct:pal("db for /root/Chester ~p",[ecomet_schema:get_mounted_folder(<<"/root/Chester/some">>)]),
  'VeryUseless' = ecomet_schema:get_mounted_db(?OID(Folder1)),

  Folder2 = ecomet:create_object(#{
    <<".name">> => <<"LightBringer">>,
    <<".folder">> => ?OID(Folder1),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),
  % Folder1 contains Folder2, so we cannot unmount 'MyUselessDB'
  % while Folder1 is not empty
  ?assertError({contains_objects, _}, ecomet:edit_object(Folder1, #{<<".database">> => ?OID(NotSoUseless)})) ,

  ecomet:delete_object(Folder2),
  % Folder2 deleted and here Folder1 is empty
  % So we can change <<database>> field %
  ecomet:edit_object(Folder1, #{<<".database">> => ?OID(NotSoUseless)}),
  'NotSoUseless' = ecomet_schema:get_mounted_db(?OID(Folder1)),

  ecomet:delete_object(Folder1),
  ecomet:delete_object(MyUselessDB),
  ecomet:delete_object(NotSoUseless),

  ok
.

% Folder1 --> |
%             |--> Folder2
%             |--> Folder3 --> |
%                              | --> Folder4
on_delete_test(_Config) ->

  ecomet_user:on_init_state(),
  Folder1 = ecomet:create_object(#{
    <<".name">> => <<"Chester">>,
    <<".folder">> => ?OID(<<"/root">>),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),

  ct:pal("db for /root/Chester ~p",[ecomet_schema:get_mounted_folder(<<"/root/Chester">>)]),

  _Folder2 = ecomet:create_object(#{
    <<".name">> => <<"LightBringer">>,
    <<".folder">> => ?OID(Folder1),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),

  Folder3 = ecomet:create_object(#{
    <<".name">> => <<"Arthas">>,
    <<".folder">> => ?OID(Folder1),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),

  _Folder4 = ecomet:create_object(#{
    <<".name">> => <<"Illidan">>,
    <<".folder">> => ?OID(Folder3),
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
  }),
  ct:pal("After create"),
  ?OID(<<"/root/Chester">>),
  ?OID(<<"/root/Chester/LightBringer">>),
  ?OID(<<"/root/Chester/Arthas">>),
  ?OID(<<"/root/Chester/Arthas/Illidan">>),
  ct:pal("After OID"),
%  ecomet:delete_object(Folder2),
  % We delete Folder1
  % Because Folder1 contain other Folders, other Folders will be deleted with Folder1%
  ecomet:delete_object(Folder1),
  ?assertError({badmatch,{error,invalid_path}}, ?OID(<<"/root/Chester">>)),
  ?assertError({badmatch,{error,invalid_path}}, ?OID(<<"/root/Chester/LightBringer">>)),
  ?assertError({badmatch,{error,invalid_path}},  ?OID(<<"/root/Chester/Arthas">>)),
  ?assertError({badmatch,{error,invalid_path}}, ?OID(<<"/root/Chester/Arthas/Illidan">>)),
  ok
.