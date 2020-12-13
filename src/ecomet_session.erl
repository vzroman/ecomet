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

%%=================================================================
%%	Service API
%%=================================================================
-export([
  on_start_node/1
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

-record(state,{instance,pattern,subs,user}).

%%=================================================================
%%	OTP
%%=================================================================
on_start_node(Node)->
  ecomet_query:set([?ROOT],#{<<"close">> => ecomet_lib:ts()},{'AND',[
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.session">>)},
    {<<"node">>,'=',Node},
    {<<"close">>,'=',-1}
  ]}),
  ok.

%%=================================================================
%%	OTP
%%=================================================================
start_link(Context,Info)->
  gen_server:start_link(?MODULE, [Context,Info], []).

stop(Session,Reason)->
  gen_server:stop(Session,Reason,?STOP_TIMEOUT).

init([Context,Info])->
  % Obtain the user context
  Context(),

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
    user = Name
  },

  ?LOGINFO("starting a session for user ~p",[Name]),

  {ok,State}.

handle_call(Request, From, State) ->
  ?LOGWARNING("ecomet session got an unexpected call resquest ~p from ~p , state ~p",[Request,From, State]),
  {noreply,State}.


handle_cast(#ecomet_log{}=_Log,State)->
  % TODO
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
handle_info(Message,State)->
  ?LOGWARNING("ecomet_session got an unexpected message ~p, state ~p",[Message,State]),
  {noreply,State}.

terminate(Reason,#state{instance = Session,user = Name})->
  ?LOGINFO("terminating session for user ~p, reason ~p",[Name ,Reason]),
  ok = ecomet:edit_object(Session,#{ <<"close">> => ecomet_lib:ts() }),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
