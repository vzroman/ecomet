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
-module(ecomet_lib).

%%=================================================================
%%	Common utilities
%%=================================================================
-export([
  parse_dt/1,
  dt_to_string/1,dt_to_string/2,
  log_ts/0
]).

dt_to_string(DT)->
  dt_to_string(DT,millisecond).
dt_to_string(DT,Unit)->
  unicode:characters_to_binary(calendar:system_time_to_rfc3339(DT,[{unit,Unit},{offset,"Z"}])).

parse_dt(DT) when is_binary(DT)->
  parse_dt(unicode:characters_to_list(DT));
parse_dt(DT) when is_list(DT)->
  calendar:rfc3339_to_system_time(DT,[{unit,millisecond}]).

log_ts()->
  erlang:system_time(nano_seconds).