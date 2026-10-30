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

-define(MB,1048576).

%%=================================================================
%%	Service API
%%=================================================================
-export([
  on_init/0,
  start/2,
  stop/2,
  get_active_users/0,
  get_sessions/0,get_sessions/1
]).

%%=================================================================
%%	OTP
%%=================================================================
-export([
  start_link/0,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SESSIONS,ecomet_sessions).

-record(session,{id, pid, ts, user, user_id, info, ref }).
-record(user,{id, name, sessions_count }).

%%=================================================================
%%	Service API
%%=================================================================
-spec on_init() -> ok.
on_init()->
  {ok,_PID} = start_link(),
  ok.

-spec start(term(), map()) -> ok.
start(User,Info)->

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

  gen_server:cast(?MODULE,{add_session,self(),Name,?OID(User),Info}).

-spec stop(pid(), term()) -> ok.
stop(PID,Reason)->
  gen_server:cast(?MODULE,{remove_session,PID,Reason}).

-spec get_active_users() -> [{tuple(), binary(), pos_integer()}].
get_active_users()->
  get_active_users(ets:next(?SESSIONS,{})).
get_active_users(UserId) when is_tuple(UserId)->
  case ets:lookup(?SESSIONS,UserId) of
    [#user{name = Name,sessions_count = Count}] when Count>0-> [{UserId,Name,Count} | get_active_users(ets:next(?SESSIONS,UserId))];
    _->get_active_users(ets:next(?SESSIONS,UserId))
  end;
get_active_users(_)->
  [].

-spec get_sessions() -> [map()].
get_sessions()->
  get_sessions( ets:match_object(?SESSIONS,#session{_ = '_'})).

% Return public session maps for a user id or name; the list clause formats rows.
get_sessions(UserId) when is_tuple(UserId)->
  get_sessions( ets:match_object(?SESSIONS,#session{user_id = UserId, _ = '_'}));
get_sessions(User) when is_binary(User)->
  get_sessions( ets:match_object(?SESSIONS,#session{user = User, _ = '_'}));
get_sessions(Sessions) when is_list(Sessions)->
  [#{
    id => Id,
    user => User,
    user_id => UserId,
    pid => PID,
    ts => TS,
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
-spec start_link() -> {ok, pid()} | {error, term()} | ignore.
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE,[],[]).

-spec init([]) -> {ok, nostate}.
init([])->
  erlang:process_flag(message_queue_data, off_heap),
  % The monitor owns both tables and is the sole writer of session rows.
  ets:new(?SESSIONS,[named_table,protected,ordered_set,{keypos, #session.id}]),
  ets:new(?ECOMET_SESSION_TOKENS,[named_table,public,set]),
  {ok,nostate}.

-spec handle_call(term(), gen_server:from(), nostate) ->
  {reply, {error, {unexpected_call, term()}}, nostate}.
handle_call(Request, From, State) ->
  ?LOGWARNING("ecomet session got an unexpected call request ~p from ~p, state ~p",
    [Request,From,State]),
  {reply,{error,{unexpected_call,Request}},State}.

-spec handle_cast(term(), nostate) -> {noreply, nostate}.
handle_cast({add_session,PID,Name,UserId,Info},State)->
  add_session(PID,Name,UserId,Info),
  {noreply,State};
handle_cast({remove_session,PID,Reason},State)->
  case ets:take(?SESSIONS,PID) of
    [#session{ref = Ref}=Session]->
      erlang:demonitor(Ref,[flush]),
      close_session(Session,Reason);
    []->ok
  end,
  {noreply,State};
handle_cast(Request,State)->
  ?LOGWARNING("ecomet session got an unexpected cast request ~p, state ~p",
    [Request,State]),
  {noreply,State}.

-spec handle_info(term(), nostate) -> {noreply, nostate}.
handle_info({'DOWN', _Ref, process, PID, Reason},State)->
  % A dead pid cannot own a live session, regardless of the monitor reference.
  case ets:take(?SESSIONS,PID) of
    [Session]->close_session(Session,Reason);
    []->ok
  end,
  {noreply,State};
handle_info(Message,State)->
  ?LOGWARNING("ecomet_session got an unexpected message ~p, state ~p",[Message,State]),
  {noreply,State}.

-spec terminate(term(), nostate) -> ok.
terminate(_Reason,_State)->
  ok.

-spec code_change(term(), nostate, term()) -> {ok, nostate}.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%=================================================================
%%	Session bookkeeping
%%=================================================================
add_session(PID,Name,UserId,Info)->
  Ref =
    case ets:take(?SESSIONS,PID) of
      [#session{user_id = OldUserId, ref = OldRef}]->
        ets:update_counter(?SESSIONS,OldUserId,{#user.sessions_count,-1}),
        OldRef;
      []->erlang:monitor(process,PID)
    end,

  true = ets:insert(?SESSIONS,#session{
    id = PID,
    pid = PID,
    ts = ecomet_lib:ts(),
    user = Name,
    user_id = UserId,
    info = Info,
    ref = Ref
  }),

  ets:update_counter(?SESSIONS,UserId,{#user.sessions_count,1},
    #user{name = Name,sessions_count = 0}),

  ?LOGINFO("starting a session for user ~p",[Name]),
  ok.

close_session(#session{user_id = UserId, user = Name},Reason)->
  ets:update_counter(?SESSIONS,UserId,{#user.sessions_count,-1}),
  ?LOGINFO("closing session for user ~ts, reason ~p",[Name,Reason]),
  ok.

%%=================================================================
%%	Caller memory limit
%%=================================================================
set_memory_limit( Limit ) when is_integer( Limit )->
  WordSize = erlang:system_info(wordsize),
  process_flag(max_heap_size, #{
    size => (Limit * ?MB) div WordSize,
    kill => true,
    error_logger => true
  });
set_memory_limit( _NoLimit )->
  ok.
