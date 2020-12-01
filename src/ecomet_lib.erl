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

-include("ecomet.hrl").
%%=================================================================
%%	Common utilities
%%=================================================================
-export([
  parse_dt/1,
  dt_to_string/1,dt_to_string/2,
  log_ts/0,
  to_object/1,to_object/2,to_object/3,to_object_system/1,
  pipe/2,
  module_exists/1
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

to_object(ID)->
  to_object(ID,none,none).
to_object(ID,Lock)->
  to_object(ID,Lock,none).
to_object(<<"/root",_/binary>> =Path,Lock,Timeout)->
  {ok,OID}=ecomet:path2oid(Path),
  ecomet:open(OID,Lock,Timeout);
to_object(ID, Lock,Timeout)->
  case ecomet:is_object(ID) of
    true when Lock=:=none->
      ID;
    true when Lock=/=none->
      % The object is already opened, but we still may need to upgrade the lock
      ecomet:open(?OID(ID),Lock,Timeout);
    _->
      % The ID is supposed to be an OID
      ecomet:open(ID,Lock,Timeout)
  end.
to_object_system(<<"/root",_/binary>> =Path)->
  {ok,OID}=ecomet:path2oid(Path),
  ecomet_object:construct(OID);
to_object_system(ID)->
  case ecomet:is_object(ID) of
    true->ID;
    _->
      ecomet_object:construct(ID)
  end.

pipe(Pipe,Acc)->
  pipe(Pipe,Acc,1).
pipe([H|T],Acc,Step)->
  case try H(Acc) catch _:Exception->{error,Exception} end of
    {ok,Acc1}->pipe(T,Acc1,Step+1);
    ok->pipe(T,Acc,Step+1);
    error->{error,Step,undefined};
    {error,Error}->{error,Step,Error};
    Unexpected->{error,Step,{unexpected,Unexpected}}
  end;
pipe([],Acc,_Step)->
  {ok,Acc}.

%% Utility for checking if the module is available
module_exists(Module)->
  case code:is_loaded(Module) of
    {file,_}->true;
    _->
      case code:load_file(Module) of
        {module,_}->true;
        {error,_}->false
      end
  end.