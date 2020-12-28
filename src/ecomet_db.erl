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
-module(ecomet_db).

-include("ecomet.hrl").
%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  is_local/1,
  get_search_node/2,
  get_databases/0,
  get_name/1,
  get_by_name/1
]).


%%===========================================================================
%% Ecomet object behaviour
%%===========================================================================
-export([
  on_create/1,
  on_edit/1,
  on_delete/1
]).

%%===========================================================================
%% This functions we need to export ONLY for testing them
%%===========================================================================
-ifdef(TEST).
-export([
  check_id/1,
  check_name/1,
  create_database/1,
  remove_database/1
]).

-endif.




%%=================================================================
%%	SERVICE API
%%=================================================================
get_name(DB)->
  {ok,Name}=ecomet:read_field(?OBJECT(DB),<<".name">>),
  binary_to_atom(Name,utf8).

is_local(_Name)->
  % TODO
  true.

get_search_node(_Name,_Exclude)->
  % TODO
  node().

get_databases()->
  ecomet_schema:get_registered_databases().

get_by_name(Name) when is_binary(Name)->
  case ecomet_query:system([?ROOT],[<<".oid">>],{'AND',[
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.database">>)},
    {<<".name">>,'=',Name }
  ]}) of
    [OID]->{ok,OID};
    _->{error,not_found}
  end.

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  check_name(Object),
  %-------------Create a new database-------------------------
  { ok, Name } = ecomet:read_field(Object,<<".name">>),
  NameAtom = binary_to_atom(Name,utf8),
  ?LOGINFO("creating a new database ~p",[NameAtom]),
  % We need to create the backend out of a transaction, otherwise it throws 'nested_transaction'
  ecomet:on_commit(fun()->
    case create_database(NameAtom) of
      {ok,ID}->
        ?LOGINFO("database ~p created successfully, id = ~p",[NameAtom,ID]),
        ok = ecomet:edit_object(Object,#{<<"id">>=>ID});
      _->
        ok = ecomet:delete_object(Object)
    end
  end),
  ok.

on_edit(Object)->
  check_name(Object),
  check_id(Object),
  ok.

on_delete(Object)->
  case ecomet_folder:find_mount_points(?OID(Object)) of
    []->
      {ok,Name}=ecomet:read_field(Object,<<".name">>),
      NameAtom = binary_to_atom(Name,utf8),
      ecomet:on_commit(fun()->
        remove_database(NameAtom)
      end),
      ok;
    _->?ERROR(is_mounted)
  end.

check_name(Object)->
  case ecomet:field_changes(Object,<<".name">>) of
    none->ok;
    { Name, none }->
      case re:run(Name,"^(\\w+)$") of
        {match,_}->ok;
        _->
          ?ERROR(invalid_name)
      end;
    {_New, _Old}->
      ?ERROR(renaming_is_not_allowed)
  end.

check_id(Object)->
  case ecomet:field_changes(Object,<<"id">>) of
    none->ok;
    { _New, none }->
      % This is the creation
      ok;
    {_New, _Old}->
      ?ERROR(change_id_is_not_allowed)
  end.

create_database(Name)->
  try
    ?LOGINFO("creating backend for ~p",[Name]),
    ok = ecomet_backend:create_db(Name),
    ?LOGINFO("backend for ~p database is created successfully",[Name]),

    ?LOGINFO("registering the ~p database in the schema",[Name]),
    case ecomet_schema:add_db(Name) of
      {ok,ID}->{ok,ID};
      {error,Error}->
        try ecomet_backend:remove_db(Name) catch
          _:CleanError->
            ?LOGERROR("error on removing ~p database, error ~p",[Name,CleanError])
        end,
        throw(Error)
    end
  catch
    _:CreateError:Stack->
      ?LOGERROR("unable to create the ~p database, error ~p, stack ~p",[
        Name,
        CreateError,
        Stack
      ]),
      error
  end.

remove_database(Name)->
  ?LOGINFO("unregistering the ~p database",[Name]),
  case ecomet_schema:remove_db(Name) of
    ok->ok;
    {error,Error}->
      ?LOGERROR("error unregistering a database ~p, error ~p",[Name,Error])
  end,
  try ecomet_backend:remove_db(Name) catch
    _:CleanError->
      ?LOGERROR("error on removing ~p database, error ~p",[Name,CleanError])
  end,
  ok.