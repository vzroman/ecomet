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
-module(ecomet_session).

-include("ecomet.hrl").

-behaviour(gen_server).

-define(TIMEOUT,300000).
-define(MB,1048576).

%%=================================================================
%%	Service API
%%=================================================================
-export([
  on_init/0,
  %-------Subscriptions---------------
  register_subscription/3,
  remove_subscription/1,

  get_active_users/0,
  get_sessions/0,get_sessions/1
]).

%%=================================================================
%%	OTP
%%=================================================================
-export([
  start_link/2,
  stop/2,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(STOP_TIMEOUT,5000).
-define(SESSIONS,ecomet_sessions).

-record(session,{id, pid, ts, user, user_id, info }).
-record(user,{id, sessions_count }).
-record(state,{ subs, user_id, user, owner }).

%%=================================================================
%%	Service API
%%=================================================================
on_init()->
  % Prepare the storage for sessions
  ets:new(?SESSIONS,[named_table,public,ordered_set,{keypos, #session.id}]),

  % Initialize subscriptions
  ecomet_subscription:on_init(),

  ok.

register_subscription(Id, Params, Timeout)->
  CallTimeout =
    if
      is_integer(Timeout) -> Timeout;
      true -> ?TIMEOUT
    end,
  case ecomet_user:get_session() of
    {ok,PID}->
      gen_server:call(PID,{register_subscription,Id,Params},CallTimeout);
    {error,Error}->
      ?ERROR(Error)
  end.

remove_subscription(ID)->
  case ecomet_user:get_session() of
    {ok,PID}->
      gen_server:cast(PID,{remove_subscription,ID});
    {error,Error}->
      ?ERROR(Error)
  end.

get_active_users()->
  get_active_users(ets:next(?SESSIONS,{})).
get_active_users(UserId) when is_tuple(UserId)->
  case ets:lookup(?SESSIONS,UserId) of
    [#user{sessions_count = Count}]-> [{UserId,Count} | get_active_users(ets:next(?SESSIONS,UserId))];
    _->get_active_users(ets:next(?SESSIONS,UserId))
  end;
get_active_users(_)->
  [].

get_sessions()->
  get_sessions( ets:match_object(?SESSIONS,#session{id = '_',pid='_', ts='_', user='_', user_id='_', info='_'})).
get_sessions(UserId) when is_tuple(UserId)->
  get_sessions( ets:match_object(?SESSIONS,#session{id = '_',pid='_', ts='_', user='_', user_id=UserId, info='_'}));
get_sessions(User) when is_binary(User)->
  get_sessions( ets:match_object(?SESSIONS,#session{id = '_',pid='_', ts='_', user=User, user_id='_', info='_'}));
get_sessions(Sessions) when is_list(Sessions)->
  [#{
    id => Id,
    user => User,
    user_id => UserId,
    pid => PID,
    ts => TS,
    subscriptions => ecomet_subscription:get_subscriptions( Id ),
    info => Info
  } || #session{
    id = Id,
    user = User,
    user_id = UserId,
    pid = PID,
    ts = TS,
    info = Info
  } <- Sessions].


%%=================================================================
%%	OTP
%%=================================================================
start_link(User,Info)->

  #{
    <<".name">> := Name,
    <<"memory_limit">> := UserMemoryLimit
  } = ecomet:read_fields( User, [<<".name">>,<<"memory_limit">>] ),

  % Limit the user process by memory
  MemoryLimit=
    case Info of
      #{memory_limit := false}->
        false;
      #{memory_limit := Limit} when is_integer(Limit)->
        Limit;
      _->
        if
          is_integer(UserMemoryLimit) -> UserMemoryLimit;
          true -> ?ENV(process_memory_limit, ?PROCESS_MEMORY_LIMIT)
        end
    end,

  % Set memory limit for the user process
  set_memory_limit( MemoryLimit ),

  gen_server:start_link(?MODULE, [Name,?OID(User),Info,self()], []).


stop(Session,Reason)->
  case is_process_alive(Session) of
    true->gen_server:stop(Session,Reason,?STOP_TIMEOUT);
    _->ok
  end.

set_memory_limit( Limit ) when is_integer( Limit )->
  WordSize = erlang:system_info(wordsize),
  process_flag(max_heap_size, #{
    size => (Limit * ?MB) div WordSize,
    kill => true,
    error_logger => true
  });
set_memory_limit( _NoLimit )->
  ok.

init([Name,UserId,Info,Owner])->

  % We need to trap_exit to run the disconnect process before exit
  process_flag(trap_exit,true),

  % Register session
  true = ets:insert(?SESSIONS,#session{
    id = self(),
    ts = ecomet_lib:ts(),
    user = Name,
    user_id = UserId,
    pid = Owner,
    info = Info
  }),

  ets:update_counter(?SESSIONS,UserId,1,#user{sessions_count = 0}),

  State = #state{
    user_id = UserId,
    user = Name,
    owner = Owner,
    subs = #{}
  },

  ?LOGINFO("starting a session for user ~p",[Name]),

  {ok,State}.


handle_call({register_subscription, Id, Params}, _From, #state{
  owner = Owner,
  user = Name ,
  subs = Subs
} = State) ->

  case ecomet_subscription:start_link( Id, Owner, Params ) of
    {ok,PID}->
      ?LOGDEBUG("register subscription ~p for user ~ts, PID ~p",[Id,Name,PID]),
      {reply,{ok,PID},State#state{subs = Subs#{Id=>PID}}};
    {error,Error} ->
      ?LOGDEBUG("unable to register subscription ~p for user ~ts, error ~p",[Id,Name,Error]),
      {reply, {error,Error}, State#state{subs = Subs}}
  end;

handle_call(Request, From, State) ->
  ?LOGWARNING("ecomet session got an unexpected call resquest ~p from ~p , state ~p",[Request,From, State]),
  {noreply,State}.

handle_cast({remove_subscription, Id}, #state{
  user = User,
  subs = Subs
} = State) ->

  case Subs of
    #{ Id := PID }->
      ecomet_subscription:stop( PID ),
      ?LOGDEBUG("remove subscription ~p for user ~ts",[ Id,User ]),
      {reply,ok,State#state{subs = maps:remove(Id,Subs)}};
    _->
      {reply, {error,not_exists},State}
  end;
handle_cast(Request,State)->
  ?LOGWARNING("ecomet session got an unexpected cast resquest ~p, state ~p",[Request, State]),
  {noreply,State}.

handle_info(Message,State)->
  ?LOGWARNING("ecomet_session got an unexpected message ~p, state ~p",[Message,State]),
  {noreply,State}.

terminate(Reason,#state{
  user_id = UserId,
  user = Name,
  owner = Owner,
  subs = Subs
})->
  ?LOGINFO("terminating session for user ~ts, reason ~p",[Name ,Reason]),

  % Deactivate all the subscriptions
  [ ecomet_subscription:stop(PID) || {_,PID} <- maps:to_list(Subs)],

  % Unregister session
  ets:delete(?SESSIONS, self()),

  ets:update_counter(?SESSIONS,UserId,-1,#user{sessions_count = 1}),

  % If the session is closed by the normal reason
  % then unlink the owner process before exit to avoid
  % sending the 'exit' to the client's process
  case Reason of
    normal->unlink(Owner);
    shutdown->unlink(Owner);
    {shutdown,_}->unlink(Owner);
    _->ok
  end,

  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.






