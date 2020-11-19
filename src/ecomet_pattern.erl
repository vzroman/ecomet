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
-module(ecomet_pattern).

%%=================================================================
%%	Service API
%%=================================================================
-export([
  get_map/1,
  edit_map/2
]).

%%=================================================================
%%	Service API
%%=================================================================
get_map(PatternID)->
  case ecomet_schema:get_pattern(PatternID) of
    Value when is_map(Value)->Value;
    _->#{}
  end.

edit_map(PatternID,Map)->
  ecomet_schema:set_pattern(PatternID,Map).