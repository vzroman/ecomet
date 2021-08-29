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

-define(TIMEOUT,30000).
-define(MB,1048576).

%%=================================================================
%%	Service API
%%=================================================================
-export([
  on_start_node/1,
  %-------Subscriptions---------------
  register_subscription/1,
  run_subscription/2,
  on_subscription/3,
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

-record(state,{ instance, pattern, subs, user, owner }).

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

register_subscription(Params)->
  case ecomet_user:get_session() of
    {ok,PID}->
      gen_server:call(PID,{register_subscription,Params},?TIMEOUT);
    {error,Error}->
      ?ERROR(Error)
  end.

run_subscription(ID,Match)->
  case ecomet_user:get_session() of
    {ok,PID}->
      gen_server:call(PID,{run_subscription,ID,Match},infinity);
    {error,Error}->
      ?ERROR(Error)
  end.

on_subscription(PID,ID,Log)->
  gen_server:cast(PID,{on_subscription,ID,Log}).

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
        ?ENV(process_memory_limit, ?PROCESS_MEMORY_LIMIT)
    end,

  if
    is_integer(MemoryLimit) ->
      WordSize = erlang:system_info(wordsize),
      process_flag(max_heap_size, #{
        size => (MemoryLimit div WordSize) * ?MB,
        kill => true,
        error_logger => true
      });
    true ->
      ignore
  end,

  gen_server:start_link(?MODULE, [Context,Info,self()], []).

stop(Session,Reason)->
  case is_process_alive(Session) of
    true->gen_server:stop(Session,Reason,?STOP_TIMEOUT);
    _->ok
  end.

init([Context,Info,Owner])->

  % Obtain the user context
  Context(),

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
    owner = Owner
  },

  ?LOGINFO("starting a session for user ~p",[Name]),

  {ok,State}.


handle_call({register_subscription,Params}, _From, #state{
  user = User ,
  subs = Subs,
  pattern = PatternID,
  instance = Instance
} = State) ->

  ID = maps:get(<<".name">>,Params),
  ?LOGDEBUG("register subscription ~ts for user ~ts",[ ID,User ]),
  ecomet:create_object(Params#{
    <<".folder">>=>?OID(Instance),
    <<".pattern">>=>PatternID,
    <<"PID">>=>self()
  }),

  {reply,ok,State#state{subs = Subs#{ID=>[]}}};

handle_call({run_subscription,ID,Match}, _From, #state{
  user = User ,
  subs = Subs
} = State) ->

  % Run the buffer
  [ Match(Log) ||Log <- lists:reverse(maps:get(ID,Subs)) ],

  ?LOGDEBUG("run subscription ~ts for user ~ts",[ ID,User ]),

  {reply,ok,State#state{subs = Subs#{ID=>Match}}};

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

  {reply,ok,State#state{subs = maps:remove(ID,Subs)}};

handle_call(Request, From, State) ->
  ?LOGWARNING("ecomet session got an unexpected call resquest ~p from ~p , state ~p",[Request,From, State]),
  {noreply,State}.


handle_cast({on_subscription,ID,Log},#state{subs = Subs,user = User}=State)->
  Subs1=
    case Subs of
      #{ID:=Match} when is_function(Match)->
        % The subscription is activated, run log matching
        try Match(Log) catch
          _:Error->?LOGERROR("subscription ~ts matching error, user ~ts, error ~p",[ID,User,Error])
        end,
        Subs;
      #{ID:=Buffer} when is_list(Buffer)->
        % The subscription is not started yet, stockpile the logs
        Subs#{ID=>[Log|Buffer]};
      _->
        ?LOGWARNING("received on_subscription event for not registered subscription ~ts, user ~ts",[ID,User]),
        Subs
    end,
  {noreply,State#state{subs = Subs1}};

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
  owner = Owner
})->
  ?LOGINFO("terminating session for user ~p, reason ~p",[Name ,Reason]),
  ecomet_query:delete([?ROOT],{'AND',[
    {<<".pattern">>,':=',PatternID},
    {<<".folder">>,'=',?OID(Session)}
  ]}),
  ok = ecomet:edit_object(Session,#{ <<"close">> => ecomet_lib:ts() }),

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
