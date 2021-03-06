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
-module(ecomet_ws).

-include("ecomet.hrl").

-behaviour(cowboy_websocket).

-export([
  init/2,
  websocket_init/1,
  websocket_handle/2,
  websocket_info/2,
  terminate/3
]).

init(Req, Opts) ->
  { cowboy_websocket, Req, Opts,#{idle_timeout => 3600000} }.

websocket_init(State) ->
  {ok, State }.

websocket_handle({Type, Msg}, State) when Type=:=text;Type=:=binary ->
  Response = ecomet_json:on_request(Msg),
  {reply,{text,Response},State};
websocket_handle(Ping, State)
  when {ping, <<"PING">>}=:=Ping;ping=:=Ping ->
  {reply,{pong,<<"PONG">>},State};
websocket_handle(_, State) ->
  {ok,State}.

websocket_info( ?SUBSCRIPTION(ID,Action,OID,Fields),State)->
  Message = ecomet_json:on_subscription(ID,Action,OID,Fields),
  {reply,{text,Message},State};
websocket_info(_,State)->
  {ok,State}.

terminate(_Reason, _Req, _State) ->
  ok.
