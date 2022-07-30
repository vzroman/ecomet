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
  on_start_node/1,
  %-------Subscriptions---------------
  register_subscription/2,
  run_subscription/2,
  on_subscription/2,
  remove_subscription/1
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

-record(state,{ instance, pattern, subs, user, owner, memory_limit }).

%%=================================================================
%%	Service API
%%=================================================================
on_start_node(Node)->

  NotClosed = ecomet_query:system([?ROOT],[<<".oid">>],{'AND',[
    {<<".pattern">>,':=',?OID(<<"/root/.patterns/.session">>)},
    {<<"node">>,'=',Node},
    {<<"close">>,'=',-1}
  ]}),

  [ try
      ecomet_query:delete([?ROOT],{<<".folder">>,'=',OID}),
      Object=ecomet:open(OID,none),
      ecomet:edit_object(Object,#{<<"close">> => ecomet_lib:ts()})
    catch
      _:Error->
        ?LOGERROR("error closing session ~p, error ~p",[OID,Error])
    end|| OID <- NotClosed],
  ok.

register_subscription(Params, Timeout)->
  CallTimeout =
    if
      is_integer(Timeout) -> Timeout;
      true -> ?TIMEOUT
    end,
  case ecomet_user:get_session() of
    {ok,PID}->
      gen_server:call(PID,{register_subscription,Params},CallTimeout);
    {error,Error}->
      ?ERROR(Error)
  end.

run_subscription(PID, Match)->
  PID ! {run_subscription,Match}.

on_subscription(PID,Log)->
  PID ! {on_subscription,Log}.

remove_subscription(ID)->
  case ecomet_user:get_session() of
    {ok,PID}->
      gen_server:call(PID,{remove_subscription,ID});
    {error,Error}->
      ?ERROR(Error)
  end.

%%=================================================================
%%	OTP
%%=================================================================
start_link(Context,Info)->

  % Limit the user process by memory
  MemoryLimit=
    case Info of
      #{memory_limit := false}->
        false;
      #{memory_limit := Limit} when is_integer(Limit)->
        Limit;
      _->
        {ok,UID} = ecomet_user:get_user(),
        case ecomet:read_field(?OBJECT(UID), <<"memory_limit">> ) of
          {ok, Limit} when is_integer( Limit )->
            Limit;
          _ ->
            ?ENV(process_memory_limit, ?PROCESS_MEMORY_LIMIT)
        end
    end,

  % Set memory limit for the user process
  set_memory_limit( MemoryLimit ),

  gen_server:start_link(?MODULE, [Context,Info,self(),MemoryLimit], []).


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

init([Context,Info,Owner,MemoryLimit])->

  % Obtain the user context
  Context(),

  % Set memory limit for the session process
  set_memory_limit( MemoryLimit ),

  % We need to trap_exit to run the disconnect process before dieing
  process_flag(trap_exit,true),

  % Register the session
  {ok,UserID} = ecomet_user:get_user(),
  {ok,Name} = ecomet:read_field(?OBJECT(UserID),<<".name">>),
  Session = ecomet:create_object(#{
    <<".folder">> => UserID,
    <<".pattern">> => ?OID(<<"/root/.patterns/.session">>),
    <<"close">> => -1, % The time of closing. -1 means still open
    <<"node">> => node(),
    <<"PID">> => self(),
    <<"info">> => Info
  }),

  State = #state{
    instance = Session,
    pattern = ?OID(<<"/root/.patterns/.subscription">>),
    subs = #{},
    user = Name,
    owner = Owner,
    memory_limit = MemoryLimit
  },

  ?LOGINFO("starting a session for user ~p",[Name]),

  {ok,State}.


handle_call({register_subscription,Params}, _From, #state{
  user = User ,
  subs = Subs,
  pattern = PatternID,
  instance = Instance,
  memory_limit = MemoryLimit
} = State) ->

  ID = maps:get(<<".name">>,Params),
  Self = self(),
  PID = spawn_link(fun()->
    % Set memory limit for the subscription handling process
    set_memory_limit( MemoryLimit ),
    % Enter the wait loop
    wait_for_run(Self, [])
  end),

  ?LOGDEBUG("register subscription ~ts for user ~ts, PID ~p",[ ID,User,PID ]),
  ecomet:create_object(Params#{
    <<".folder">>=>?OID(Instance),
    <<".pattern">>=>PatternID,
    <<"PID">>=> PID
  }),

  {reply,PID,State#state{subs = Subs#{ID=>PID}}};


handle_call({remove_subscription, ID}, _From, #state{
  user = User,
  subs = Subs,
  pattern = PatternID,
  instance = Instance
} = State) ->

  ?LOGDEBUG("remove subscription ~ts for user ~p",[ ID,User ]),
  ecomet_query:delete([?ROOT],{'AND',[
    {<<".pattern">>,':=',PatternID},
    {<<".folder">>,'=',?OID(Instance)},
    {<<".name">>,'=',ID}
  ]}),

  case Subs of
    #{ID := PID} -> PID ! {stop, self()};
    _ -> ignore
  end,

  {reply,ok,State#state{subs = maps:remove(ID,Subs)}};

handle_call(Request, From, State) ->
  ?LOGWARNING("ecomet session got an unexpected call resquest ~p from ~p , state ~p",[Request,From, State]),
  {noreply,State}.

handle_cast(Request,State)->
  ?LOGWARNING("ecomet session got an unexpected cast resquest ~p, state ~p",[Request, State]),
  {noreply,State}.

handle_info(Message,State)->
  ?LOGWARNING("ecomet_session got an unexpected message ~p, state ~p",[Message,State]),
  {noreply,State}.

terminate(Reason,#state{
  instance = Session,
  pattern = PatternID,
  user = Name,
  owner = Owner,
  subs = Subs
})->
  ?LOGINFO("terminating session for user ~p, reason ~p",[Name ,Reason]),
  ecomet_query:delete([?ROOT],{'AND',[
    {<<".pattern">>,':=',PatternID},
    {<<".folder">>,'=',?OID(Session)}
  ]}),
  ok = ecomet:edit_object(Session,#{ <<"close">> => ecomet_lib:ts() }),

  % Deactivate all the subscriptions
  Self = self(),
  [ PID ! {stop, Self} || {_,PID} <- maps:to_list(Subs)],

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

%%------------------------------------------------------------
%%  Subscription process
%%------------------------------------------------------------

% The subscription is not started yet, stockpile the logs and wait for message to start
wait_for_run(Owner, Buffer)->
  receive
    {on_subscription,Log}->
      wait_for_run(Owner, [Log|Buffer]);
    {run_subscription,Match}->
      % Activate the subscription
      % Run the buffer
      [ Match(Log) || Log <- lists:reverse(Buffer) ],
      subscription_loop(Owner, Match);
    {stop, Owner}->
      % The exit command
      unlink(Owner);
    Unexpected->
      ?LOGWARNING("unexpected message ~p",[Unexpected]),
      wait_for_run(Owner, Buffer)
  end.

% The subscription is active, wait for log events.
subscription_loop(Owner, Match)->
  receive
    {on_subscription,Log}->
      try Match(Log) catch
        _:Error->?LOGERROR("subscription matching error, error ~p",[Error])
      end,
      subscription_loop(Owner, Match);
    {stop, Owner}->
      % The exit command
      unlink(Owner);
    Unexpected->
      ?LOGWARNING("unexpected message ~p",[Unexpected]),
      subscription_loop(Owner, Match)
  end.























