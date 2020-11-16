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
-module(ecomet_backend).

-include("ecomet.hrl").
%%=================================================================
%%	Service API
%%=================================================================
-export([
  create_db/1,
  remove_db/1,
  get_storages/0
]).

-define(NAME(N,T),list_to_atom("ecomet_"++atom_to_list(N)++"_"++atom_to_list(T))).

-define(DB,[
  { ?RAMLOCAL, #{ type => ?RAM, local => true } },
  { ?RAM, #{ type => ?RAM } },
  { ?RAMDISC, #{ type => ?RAMDISC } },
  { ?DISC, #{ type => ?DISC } }
]).

%%=================================================================
%%	Service API
%%=================================================================
create_db(Name)->
  [ dlss:add_storage(?NAME(Name,Type), DLSSType, O) || { Type, #{ type:=DLSSType } = O } <- ?DB],
  ok.

remove_db(Name)->
  [ dlss:remove_storage( ?NAME(Name,Type) ) || { Type, _ } <- ?DB ],
  ok.

get_storages()->
  dlss:get_storages().