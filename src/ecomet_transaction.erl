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

-define(transaction,?MODULE).
-record(state,{ type, on_commit, log }).

internal(Fun)->
  case get(?transaction) of
    undefined->
      put(?transaction, #state{ type = internal, on_commit = [], log = #{} }),
      Result = ecomet_db:transaction(Fun),
      State = erase(?transaction),
      case Result of
        {ok,_}-> commit( State );
        _-> ignore
      end,
      Result;
    #state{ type = internal } = State->
      Result = ecomet_db:transaction(Fun),
      case Result of
        {abort,_}->
          put(?transaction, State);
        _->
          ignore
      end,
      Result;
    #state{ type = External } when is_pid( External )->
      External ! {internal, self(), Fun},
      receive
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
    #state{type = External} when is_pid(External)->
      External ! {external, self()};
    _->
      throw(nested_external_transaction)
  end.

% Commit transaction
commit()->
  case get(?transaction) of
    #state{ type = External } when is_pid(External)->
      External ! {commit, self()},
      receive
        {commit, External}->
          erase(?transaction),
          ok;
        {abort, External, Reason}->
          erase(?transaction),
          {abort, Reason};
        {nested, External, Result}->
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
        {abort, External}->
          erase(?transaction),
          ok;
        {nested, External}->
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
      External ! {write, self(), DB, Storage, Type, Key, Value, Lock},
      receive
        {result, External, Result}-> Result
      end;
    _->
      throw(no_transaction)
  end.

delete(DB, Storage, Type, Key, Lock)->
  case get(?transaction) of
    #state{ type = internal }->
      ecomet_db:delete( DB, Storage, Type, Key, Lock );
    #state{ type = External } when is_pid(External)->
      External ! {delete, self(), DB, Storage, Type, Key, Lock},
      receive
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

%%-----------------------------------------------------------------------
%% External transaction
%%-----------------------------------------------------------------------
external( Owner )->
  todo.

%%-----------------------------------------------------------------------
%% Internal helpers
%%-----------------------------------------------------------------------
tcommit()->
  case get(?TKEY) of
    undefined->?ERROR(no_transaction);
    % Root transaction
    #state{parent=none,log=Log,droplog = DropLog,dict=Dict,oncommit=OnCommits }->
      Commits = lists:reverse(lists:subtract(Log,DropLog)),
      CommitLog = run_commit(Commits,Dict,[]),
      erase(?TKEY),
      {CommitLog,OnCommits};
    #state{ dirty=IsDirty, parent = #state{ dirty = IsDirty } }=State->
      % If the parent and child have the same dirty we merge commits
      put(?TKEY,merge_commit(State)),
      {[],[]};
    #state{parent=Parent,log=Log,droplog = DropLog,dict=Dict,oncommit=OnCommits }->
      % The parent and the child have different levels of dirty
      % We perform the commit
      Commits = lists:reverse(lists:subtract(Log,DropLog)),
      CommitLog = run_commit(Commits,Dict,[]),
      put(?TKEY,Parent),
      {CommitLog,OnCommits}
  end.

% Commit changes to storage
run_commit([OID|Rest],Dict,LogList)->
  run_commit(Rest,Dict,[ecomet_object:commit(OID,Dict)|LogList]);
run_commit([],_Dict,Log)->lists:reverse(Log).

release_locks([{_Key,Lock}|Rest])->
  release_stick_lock(Lock),
  release_locks(Rest);
release_locks([])->ok.

% Merge results of the transaction to the parent
merge_commit(#state{
  locks=Locks,
  dict=Dict,
  log=Log,
  droplog=DropLog,
  oncommit=OnCommits,
  parent=Parent
})->
  ClearedLog=lists:subtract(Log,DropLog),
  MergedLog=merge_log(ClearedLog,Parent#state.log),
  Parent#state{
    locks=Locks,
    dict=Dict,
    log=MergedLog,
    droplog=lists:subtract(Parent#state.droplog,ClearedLog),
    oncommit=[OnCommits|Parent#state.oncommit]
  }.

merge_log([OID|Rest],Parentlog)->
  Merged=
    case lists:member(OID,Parentlog) of
      false->[OID|Parentlog];
      true->Parentlog
    end,
  merge_log(Rest,Merged);
merge_log([],Parentlog)->Parentlog.

on_commit(Log,OnCommits)->
  run_notifications([L#ecomet_log{self = self()} || L <- Log]),
  run_oncommits(lists:reverse(OnCommits)).

% Run notifications
run_notifications(Logs)->
  ecomet_router:on_commit(Logs).

% Execute fun
run_oncommits([F|Rest]) when is_function(F)->
  try F() catch _:_->ok end,
  run_oncommits(Rest);
% Execute brunch (result of sub transaction)
run_oncommits([Branch|Rest]) when is_list(Branch)->
  run_oncommits(lists:reverse(Branch)),
  run_oncommits(Rest);
run_oncommits([])->ok.