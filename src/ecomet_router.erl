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
-module(ecomet_router).

-include("ecomet.hrl").

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  on_init/1,
  on_commit/1
]).

on_init( PoolSize )->

  [ persistent_term:put({?MODULE, I }, spawn_link( fun worker_loop/0 ) ) || I <- lists:seq(1, PoolSize) ],

  % Register the ServiceID for the subscriptions
  persistent_term:put({?MODULE,pool_size}, PoolSize).

worker_loop()->
  receive
    #ecomet_log{} = Log->
      try
        ecomet_query:on_commit( Log )
      catch
        _:Error:Stack->
          ?LOGERROR("log error ~p, ~p, ~p",[ Log, Error, Stack ])
      end,
      worker_loop();
    Unexpected->
      ?LOGWARNING("unexpected message ~p", [Unexpected])
  end.

on_commit( Log )->
  case persistent_term:get({?MODULE,pool_size},none) of
    PoolSize when is_integer(PoolSize)->
      I = erlang:phash(Log, PoolSize),
      Worker = persistent_term:get({?MODULE,I}),
      Worker ! Log;
    _->
      % Not initialized yet
      ignore
  end.