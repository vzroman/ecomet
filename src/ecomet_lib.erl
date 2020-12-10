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
  to_oid/1,
  pipe/2,
  module_exists/1,
  guid/0
]).

dt_to_string(DT)->
  dt_to_string(DT,millisecond).
dt_to_string(DT,Unit)->
  unicode:characters_to_binary(calendar:system_time_to_rfc3339(DT,[{unit,Unit},{offset,"Z"}])).

parse_dt(DT) when is_integer(DT)-> DT;
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
  ecomet_object:open(OID,Lock,Timeout);
to_object(ID, Lock,Timeout)->
  case ecomet_object:is_object(ID) of
    true when Lock=:=none->
      ID;
    _->
      % The object is already opened, but we still may need to upgrade the lock
      ecomet_object:open(?OID(ID),Lock,Timeout)
  end.
to_object_system(<<"/root",_/binary>> =Path)->
  {ok,OID}=ecomet:path2oid(Path),
  ecomet_object:construct(OID);
to_object_system(ID)->
  case ecomet_object:is_object(ID) of
    true->ID;
    _->
      ecomet_object:construct(ID)
  end.

to_oid(<<"/root",_/binary>> =Path)->
  {ok,OID}=ecomet:path2oid(Path),
  OID;
to_oid(ID)->
  case ecomet_object:is_oid(ID) of
    true->ID;
    _->ecomet:get_oid(?OBJECT(ID))
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

guid() ->
  lists:foldl(fun (I, Acc) ->
    B = rand:uniform(256) - 1,
    S =
      if
        I == 7 ->
          B1 = B band 16#0f bor 16#40,
          io_lib:format("~2.16.0b", [B1]);   %% The last 0 in 2.16.0 means fill with leading 0 if necessay
        I == 9 -> %% multiplexed variant type (2 bits)
          B1 = B band 16#3f bor 16#80,
          io_lib:format("~2.16.0b", [B1]);
        I == 4; I == 6; I == 8; I == 10 ->
          io_lib:format("~2.16.0b-", [B]);
        true ->
          io_lib:format("~2.16.0b", [B])
      end,
    Acc ++ S
  end,[],lists:seq(1, 16)).