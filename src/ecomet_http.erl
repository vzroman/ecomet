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

-module(ecomet_http).

-behaviour(cowboy_handler).

-include("ecomet.hrl").

%%=================================================================
%%	Listeners API
%%=================================================================
-export([
  listener/2
]).

%%=================================================================
%%	Cowboy behaviour
%%=================================================================
-export([
  init/2,
  terminate/3
]).

-define(DEFAULT_STOP_TIMEOUT,60000). % 1 min.

listener(http,Params)->
  #{
    id=>http_listener,
    start=>{ cowboy ,start_clear ,[
      http_listener,
      Params,
      #{env => #{dispatch=> dispatch_rules()} }
    ]},
    restart=>permanent,
    shutdown=>?ENV(stop_timeout, ?DEFAULT_STOP_TIMEOUT),
    type=>worker,
    modules=>[cowboy]
  };


listener(https,Params)->
  #{
    id=>https_listener,
    start=>{ cowboy ,start_tls ,[
      https_listener,
      Params,
      #{env => #{dispatch=> dispatch_rules() } }
    ]},
    restart=>permanent,
    shutdown=>?ENV(stop_timeout, ?DEFAULT_STOP_TIMEOUT),
    type=>worker,
    modules=>[cowboy]
  }.


dispatch_rules() ->
  cowboy_router:compile([{'_',[
    {"/", ecomet_http, []},
    {"/ui/[...]", cowboy_static, {priv_dir, ecomet, "UI"}},
    {"/websocket", ecomet_ws, []},
    {"/favicon.ico",  cowboy_static, {priv_file, ecomet, "UI/favicon.ico"}}
  ]}]).

%%=================================================================
%%	Cowboy behaviour
%%=================================================================
init(Req0, Opts) ->
  Req = cowboy_req:reply(301, #{
    <<"location">>=> <<"ui/index.html">>
  }, Req0),
  {ok, Req, Opts}.

terminate(_Reason, _Req, _State) ->
  ok.