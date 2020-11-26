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
-module(ecomet_db).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  is_local/1,
  get_search_node/2,
  get_databases/0
]).

%%=================================================================
%%	SERVICE API
%%=================================================================
is_local(_Name)->
  % TODO
  true.

get_search_node(_Name,_Exclude)->
  % TODO
  node().

get_databases()->
  ecomet_schema:get_registered_databases().