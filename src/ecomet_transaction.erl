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
  internal_sync/1,
  dirty/1,
  start/0,
  lock_key/4,
  lock/3,
  release_lock/1,
  find_lock/1,
  dict_get/1,dict_get/2,
  dict_put/1,
  dict_remove/1,
  queue_commit/1,
  apply_commit/1,
  on_commit/1,
  get_type/0,
  commit/0,
  rollback/0
]).

-define(TKEY,eCoMeT_tRaNsAcTiOn).
-define(LOCKTIMEOUT,10000).
-define(LOCKKEY(DB,Storage,Type,Key),{DB,Storage,Type,Key}).

-record(state,{locks,dict,log,droplog,parent,oncommit,type,dirty}).
-record(lock,{value,level,pid}).

% Run fun within transaction
internal(Fun)->
  State = tstart(internal),
  case ecomet_backend:transaction(fun()->
    clean(State),
    Result=Fun(),
    {Log,OnCommits}=tcommit(),
    {Result,Log,OnCommits}
  end) of
    {ok,{Result,Log,OnCommits}}->
      on_commit(Log,OnCommits),
      {ok,Result};
    {error,Error}->
      rollback(),
      {error,Error}
  end.

internal_sync(Fun)->
  State = tstart(internal),
  case ecomet_backend:sync_transaction(fun()->
    clean(State),
    Result=Fun(),
    {Log,OnCommits}=tcommit(),
    {Result,Log,OnCommits}
  end) of
    {ok,{Result,Log,OnCommits}}->
      on_commit(Log,OnCommits),
      {ok,Result};
    {error,Error}->
      rollback(),
      {error,Error}
  end.

dirty(Fun)->
  tstart( internal, _Dirty=true ),
  try
    Result=Fun(),
    {Log,OnCommits}=tcommit(),
    on_commit(Log,OnCommits),
    {ok,Result}
  catch
    _:Error->
      rollback(),
      {error,Error}
  end.

% Start external transaction
start()->tstart(external).

% Start transaction
tstart(Type)->
  tstart(Type, _Dirty=false).
tstart(Type,Dirty)->
  State=
    case get(?TKEY) of
      % It is root transaction
      undefined->
        #state{
          locks= #{},
          dict= #{},
          log=[],
          droplog=[],
          parent=none,
          oncommit=[],
          type=Type,
          dirty = Dirty
        };
      % Subtransaction
      Parent->
        % Variants:
        SubType=
          case {Parent#state.type,Type} of
            % parent is external. Sub run as external, even if it is claimed as internal
            {external,_}->external;
            % parent is internal, sub is internal. Run as internal
            {internal,internal}->internal;
            % parent is internal, sub is external. Error
            {internal,external}->?ERROR(external_transaction_within_internal)
          end,
        #state{
          locks=Parent#state.locks,
          dict= Parent#state.dict,
          log=[],
          droplog=[],
          parent=Parent,
          oncommit=[],
          type=SubType,
          dirty=Dirty
        }
    end,
  put(?TKEY,State),
  State.

% Commit transaction
commit()->
  case get(?TKEY) of
    undefined->?ERROR(no_transaction);
    #state{type = internal}->?ERROR(internal_transaction);
    State when State#state.type==external->
      case ecomet_backend:transaction(fun()->tcommit() end) of
        {ok,{Log,OnCommits}}->
          release_locks(maps:to_list(State#state.locks)),
          on_commit(Log,OnCommits);
        {error,Error}->?ERROR(Error)
      end
  end.

% Transaction rollback, erase all changes
rollback()->
  case erase(?TKEY) of
    undefined->?ERROR(no_transaction);
    % Main transaction
    #state{parent=none,type=Type,locks=Locks}->
      case Type of
        internal->ok;
        external->release_locks(maps:to_list(Locks))
      end;
    #state{parent=Parent,locks=Locks}->
      % By default we hold all locks until main transaction finish
      put(?TKEY,Parent#state{locks=Locks})
  end,
  ok.

clean(State)->
  put(?TKEY,State).

% Get type of current transaction
get_type()->
  case get(?TKEY) of
    undefined->none;
    #state{ dirty = true }-> dirty;
    #state{ type = Type }-> Type
  end.

lock_key(DB,Storage,Type,Key)->
  ?LOCKKEY(DB,Storage,Type,Key).

lock(?LOCKKEY(_,_,_,_)=LockKey,Level,Timeout) when is_integer(Timeout)->
  case get(?TKEY) of
    undefined->?ERROR(no_transaction);
    #state{locks=Locks,type=TType}=State->
      case add_lock(LockKey,Level,Locks,TType,Timeout) of
        {ok,Lock}->
          put(?TKEY,State#state{locks=Locks#{LockKey=>Lock}}),
          {ok,Lock#lock.value};
        {error,Error}->{error,Error}
      end
  end;
lock(?LOCKKEY(_,_,_,_)=LockKey,Level,_Timeout)->
  lock(LockKey,Level,?LOCKTIMEOUT).


add_lock(Key,LockLevel,Locks,TType,Timeout)->
  case maps:find(Key,Locks) of
    error->get_lock(TType,Key,LockLevel,Timeout);
    {ok,Lock}->
      case {LockLevel,Lock#lock.level} of
        % No need to upgrade
        {read,_}->{ok,Lock};
        {write,write}->{ok,Lock};
        % Upgrade needed
        {write,read}->upgrade_lock(TType,Key,LockLevel,Lock,Timeout)
      end
  end.

% Acquire lock for internal transaction
get_lock(internal,?LOCKKEY(DB,Storage,Type,Key),LockLevel,_Timeout)->
  Value= ecomet_backend:read(DB,Storage,Type,Key,LockLevel),
  {ok,#lock{value=Value,level=LockLevel,pid=internal}};
% External transaction uses stick locks
get_lock(external,Key,LockLevel,Timeout)->
  Self=self(),
  PID=spawn(fun()->stick_lock(Self,Key,LockLevel) end),
  wait_stick_lock(PID,Timeout).
wait_stick_lock(PID,Timeout)->
  receive
    {ecomet_stick_lock,Lock} when Lock#lock.pid==PID->{ok,Lock}
  after
    Timeout->
      exit(PID,timeout),
      {error,timeout}
  end.

upgrade_lock(internal,Key,LockLevel,_Lock,Timeout)->
  get_lock(internal,Key,LockLevel,Timeout);
upgrade_lock(external,_Key,LockLevel,#lock{pid=PID},Timeout)->
  PID!{upgrade,LockLevel},
  wait_stick_lock(PID,Timeout).

% Executed in context of linked process
stick_lock(Holder,Key,LockLevel)->
  ecomet_backend:transaction(fun()->
    {ok,Lock}=get_lock(internal,Key,LockLevel,timeout_not_used),
    Holder!{ecomet_stick_lock,Lock#lock{pid=self()}},
    wait_release(Key,Holder)
  end).
wait_release(Key,Holder)->
  link(Holder),
  process_flag(trap_exit,true),
  receive
  % Finish process
    release->ok;
  % Parent finished
    {'EXIT',Holder,Reason}->exit(Reason);
    {upgrade,NewLockLevel}->
      % Use unlink because this process can be killed by timeout
      process_flag(trap_exit,false),
      unlink(Holder),
      {ok,Lock}=get_lock(internal,Key,NewLockLevel,timeout_not_used),
      Holder!{ecomet_stick_lock,Lock#lock{pid=self()}},
      wait_release(Key,Holder)
  end.

release_lock(Key)->
  case get(?TKEY) of
    undefined->?ERROR(no_transaction);
    % Internal transaction holds locks until commit all parents
    #state{type=internal}->ok;
    State->
      case maps:find(Key,State#state.locks) of
        {ok,Lock}->release_stick_lock(Lock),
          put(?TKEY,State#state{locks=maps:remove(Key,State#state.locks)}),
          ok;
        error->ok
      end
  end.
release_stick_lock(#lock{pid=PID})->
  PID!release.

find_lock(Key)->
  case get(?TKEY) of
    undefined->none;
    #state{locks=Locks}->
      case maps:find(Key,Locks) of
        error->none;
        {ok,#lock{value=Value,level=LockLevel}}->{Value,LockLevel}
      end
  end.


dict_get(Key)->
  case get(?TKEY) of
    undefined->?ERROR(no_transaction);
    #state{dict=Dict}->maps:get(Key,Dict)
  end.
dict_get(Key,Default)->
  case get(?TKEY) of
    undefined->Default;
    #state{dict=Dict}->maps:get(Key,Dict,Default)
  end.

dict_put(KeyValueList) when is_list(KeyValueList)->
  dict_put(maps:from_list(KeyValueList));
dict_put(KeyValueMap) when is_map(KeyValueMap)->
  case get(?TKEY) of
    undefined->?ERROR(no_transaction);
    State->
      Dict=maps:merge(State#state.dict,KeyValueMap),
      put(?TKEY,State#state{dict=Dict})
  end.

dict_remove(KeyList)->
  case get(?TKEY) of
    undefined->ok;
    State->
      Dict=maps:without(KeyList,State#state.dict),
      put(?TKEY,State#state{dict=Dict})
  end.

% Put oid to commit queue
queue_commit(OID)->
  case get(?TKEY) of
    undefined->?ERROR(no_transaction);
    State->
      NewState=
        case lists:member(OID,State#state.log) of
          false->State#state{log=[OID|State#state.log],droplog=[OID|State#state.droplog]};
          true->State
        end,
      put(?TKEY,NewState)
  end,
  ok.

apply_commit(OID)->
  case get(?TKEY) of
    undefined->?ERROR(no_transaction);
    State->
      DropLog=lists:delete(OID,State#state.droplog),
      put(?TKEY,State#state{droplog=DropLog})
  end,
  ok.

% Add fun to execute if transaction committed
on_commit(Fun) when is_function(Fun,0)->
  case get(?TKEY) of
    undefined->?ERROR(no_transaction);
    State->
      OnCommits=[Fun|State#state.oncommit],
      put(?TKEY,State#state{oncommit=OnCommits})
  end,
  ok;
on_commit(_Invalid)->
  ?ERROR(not_function).

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
  run_notifications(Log),
  run_oncommits(lists:reverse(OnCommits)).

% Run notifications
run_notifications([Log|Rest])->
  ecomet_query:on_commit(Log),
  run_notifications(Rest);
run_notifications([])->ok.

% Execute fun
run_oncommits([F|Rest]) when is_function(F)->
  try F() catch _:_->ok end,
  run_oncommits(Rest);
% Execute brunch (result of sub transaction)
run_oncommits([Branch|Rest]) when is_list(Branch)->
  run_oncommits(lists:reverse(Branch)),
  run_oncommits(Rest);
run_oncommits([])->ok.