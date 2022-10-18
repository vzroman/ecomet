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
-module(ecomet_transaction).

-include("ecomet.hrl").

%% API
-export([
  internal/1,
  start/0,
  on_commit/1,
  get_type/0,
  commit/0,
  rollback/0
]).


-export([
  read/5,
  write/6,
  delete/5,
  on_abort/5,
  changes/4,
  log/2,
  dict_put/1,
  dict_get/1,dict_get/2,
  dict_remove/1
]).

-define(transaction,?MODULE).
-record(state,{ type, on_commit, log, dict, owner }).

internal(Fun)->
  case get(?transaction) of
    undefined->
      put(?transaction, #state{ type = internal, on_commit = [], log = #{}, dict = #{}, owner = self() }),
      Result = ecomet_db:transaction(fun()->
        Res = Fun(),
        State = #state{log = Log,owner = Owner} = erase(?transaction),
        OrderedLog = ordsets:from_list([{Queue,Value} || {Queue, Value} <- maps:values(Log)]),
        CommitLog = ecomet_object:commit([Value || {_,Value} <- OrderedLog]),
        {Res, State#state{ log = [Commit#{ self => Owner } || Commit <- CommitLog] }}
      end),
      case Result of
        {ok,{ FunRes, State }}->
          tcommit( State ),
          FunRes;
        _->
          abort
      end;
    #state{ type = internal } = State->
      Result = ecomet_db:transaction(Fun),
      case Result of
        {abort,_}->
          put(?transaction, State);
        _->
          ok
      end,
      Result;
    #state{ type = External } when is_pid( External )->
      External ! {internal, self(), Fun},
      receive
        {result, External, {abort,_}=Error}-> throw(Error);
        {result, External, Result}-> Result
      end
  end.

% Start external transaction
start()->
  case get(?transaction) of
    undefined->
      Self = self(),
      External = ecomet_user:spawn_session(fun()-> external( Self ) end),
      put(?transaction,#state{ type = External });
    _->
      throw(nested_transaction)
  end.

% Commit transaction
commit()->
  case get(?transaction) of
    #state{ type = External } when is_pid(External)->
      External ! {commit, self()},
      receive
        {result, External, Result}->
          erase(?transaction),
          Result
      end;
    #state{type = internal}->
      throw( internal_transaction );
    _->
      throw(no_transaction)
  end.

% Transaction rollback, erase all changes
rollback()->
  case get(?transaction) of
    #state{ type = External } when is_pid( External )->
      External ! {rollback, self()},
      receive
        {result, External, _Result}->
          erase(?transaction),
          ok
      end;
    #state{ type = internal }->
      throw( internal_transaction );
    _->
      throw(no_transaction)
  end.

% Get type of the current transaction
get_type()->
  case get(?transaction) of
    undefined->none;
    #state{ type = internal }-> internal;
    #state{ type = External } when is_pid(External)-> external
  end.

% Add fun to execute if transaction committed
on_commit(Fun) when is_function(Fun,0)->
  case get(?transaction) of
    #state{type = internal, on_commit = OnCommit } = State->
      put(?transaction,State#state{ on_commit = [Fun|OnCommit] });
    #state{ type = External} when is_pid(External)->
      External ! {on_commit, self(), Fun};
    _->
      throw(no_transaction)
  end;
on_commit(_Invalid)->
  throw( badarg ).

read(DB, Storage, Type, Key, Lock)->
  case get(?transaction) of
    #state{ type = internal }->
      ecomet_db:read( DB, Storage, Type, Key, Lock );
    #state{ type = External } when is_pid(External)->
      External ! {read, self(), DB, Storage, Type, Key, Lock},
      receive
        {result, External, {abort,_}=Error}-> throw(Error);
        {result, External, Result}-> Result
      end;
    _->
      throw(no_transaction)
  end.

write(DB, Storage, Type, Key, Value, Lock)->
  case get(?transaction) of
    #state{ type = internal }->
      ecomet_db:write( DB, Storage, Type, Key, Value, Lock );
    #state{ type = External } when is_pid(External)->
      External ! {write, self(), DB, Storage, Type, Key, Value, Lock};
    _->
      throw(no_transaction)
  end.

delete(DB, Storage, Type, Key, Lock)->
  case get(?transaction) of
    #state{ type = internal }->
      ecomet_db:delete( DB, Storage, Type, Key, Lock );
    #state{ type = External } when is_pid(External)->
      External ! {delete, self(), DB, Storage, Type, Key, Lock};
    _->
      throw(no_transaction)
  end.

on_abort( DB, Storage, Type, Key, Value )->
  case get(?transaction) of
    #state{ type = internal }->
      ecomet_db:on_abort( DB, Storage, Type, Key, Value );
    #state{ type = External } when is_pid(External)->
      External ! {on_abort, self(), DB, Storage, Type, Key, Value};
    _->
      throw(no_transaction)
  end.

changes( DB, Storage, Type, Key )->
  case get(?transaction) of
    #state{ type = internal }->
      ecomet_db:changes( DB, Storage, Type, Key );
    #state{ type = External } when is_pid(External)->
      External ! {changes, self(), DB, Storage, Type, Key},
      receive
        {result, External, {abort,_}=Error}-> throw(Error);
        {result, External, Result}-> Result
      end;
    _->
      throw(no_transaction)
  end.

log(OID, Value)->
  case get(?transaction) of
    #state{ type = internal, log = Log } = State->
      Queue =
        case Log of
          #{ OID, {_Queue, _} }->
            _Queue;
          _->
            maps:size( Log )
        end,
      put(?transaction, State#state{ log = Log#{ OID =>{Queue,Value} }});
    #state{ type = External } when is_pid( External )->
      External ! {log, self(), OID, Value};
    _->
      throw( no_transaction )
  end.


dict_put(KeyValueList) when is_list(KeyValueList)->
  dict_put(maps:from_list(KeyValueList));
dict_put(KeyValueMap) when is_map(KeyValueMap)->
  case get(?transaction) of
    #state{ type = internal, dict = Dict0 } = State->
      Dict=maps:merge(Dict0,KeyValueMap),
      put(?transaction, State#state{ dict = Dict});
    #state{ type = External } when is_pid( External )->
      External ! {dict_put, self(), KeyValueMap};
    _->
      throw( no_transaction )
  end.

dict_get(Key)->
  case get(?transaction) of
    #state{ type = internal, dict = Dict }->
      maps:get(Key,Dict);
    #state{ type = External } when is_pid( External )->
      External ! {dict_get, self(), Key},
      receive
        {result, External, {abort,_}=Error}-> throw(Error);
        {result, External, Result}-> Result
      end;
    _->
      throw( no_transaction )
  end.

dict_get(Key,Default)->
  case get(?transaction) of
    #state{ type = internal, dict = Dict }->
      maps:get(Key,Dict, Default);
    #state{ type = External } when is_pid( External )->
      External ! {dict_get, self(), Key, Default},
      receive
        {result, External, {abort,_}=Error}-> throw(Error);
        {result, External, Result}-> Result
      end;
    _->
      Default
  end.

dict_remove(KeyList) when is_list(KeyList)->
  case get(?transaction) of
    #state{ type = internal, dict = Dict0 }=State->
      Dict=maps:without(KeyList,Dict0),
      put(?transaction, State#state{ dict = Dict});
    #state{ type = External } when is_pid( External )->
      External ! {dict_remove, self(), KeyList};
    _->
      ok
  end.

%%-----------------------------------------------------------------------
%% External transaction
%%-----------------------------------------------------------------------
external( Owner )->
  link( Owner ),

  Result = internal(fun()->
    State = get(?transaction),
    put(?transaction,State#state{owner = Owner}),
    external_loop( Owner )
  end),

  unlink(Owner),
  catch Owner ! {result, self(), Result}.


external_loop( Owner )->
  receive
    {internal, Owner, Fun}->
      Result = ecomet_db:transaction( Fun ),
      Owner ! {result, self(), Result},
      external_loop( Owner );
    {on_commit, Owner, Fun}->
      on_commit( Fun ),
      external_loop( Owner );
    {read, Owner, DB, Storage, Type, Key, Lock}->
      Result = read(DB, Storage, Type, Key, Lock ),
      Owner ! {result, self(), Result},
      external_loop( Owner );
    {write, Owner, DB, Storage, Type, Key, Value, Lock}->
      ok = write(DB, Storage, Type, Key, Value, Lock),
      external_loop( Owner );
    {delete, Owner, DB, Storage, Type, Key, Lock}->
      ok = delete(DB, Storage, Type, Key, Lock),
      external_loop( Owner );
    {on_abort, Owner, DB, Storage, Type, Key, Value}->
      ok = on_abort(DB, Storage, Type, Key, Value),
      external_loop( Owner );
    {changes, Owner, DB, Storage, Type, Key}->
      Result = changes(DB, Storage, Type, Key ),
      Owner ! {result, self(), Result},
      external_loop( Owner );
    {log, Owner, OID, Value}->
      log( OID, Value ),
      external_loop( Owner );
    {dict_put, Owner, KeyValueMap}->
      dict_put( KeyValueMap ),
      external_loop( Owner );
    {dict_get, Owner, Key}->
      Owner ! {result,self(), dict_get( Key )},
      external_loop( Owner );
    {dict_get, Owner, Key, Default}->
      Owner ! {result,self(), dict_get( Key, Default )},
      external_loop( Owner );
    {dict_remove, Owner, KeyList}->
      dict_remove( KeyList ),
      external_loop( Owner );
    {commit, Owner}->
      ok;
    {rollback, Owner}->
      throw( rollback );
    _Unexpected->
      external_loop( Owner )
  end.

%%-----------------------------------------------------------------------
%% Internal helpers
%%-----------------------------------------------------------------------
tcommit( #state{ log = Log, on_commit = OnCommit })->
  catch ecomet_subscription:commit( Log ),
  [ catch F() || F <- lists:reverse(OnCommit) ].

