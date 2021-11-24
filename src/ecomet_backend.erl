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
  get_storage_name/3,
  get_storages/0,
  get_segments/1,get_segments/3,
  get_root_segment/1,get_root_segment/3,
  get_segment_info/1,
  get_segment_params/1,
  get_segment_size/1,
  get_storage_type/1,
  add_segment_copy/2, remove_segment_copy/2,
  is_local_storage/1,
  get_supported_types/0
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  transaction/1, sync_transaction/1,
  read/4,read/5,dirty_read/4,dirty_read/5,
  write/5,write/6,dirty_write/5,
  delete/4,delete/5,dirty_delete/4
]).

-define(NAME(N,S,T),list_to_atom("ecomet_"++atom_to_list(N)++"_"++atom_to_list(S)++"_"++atom_to_list(T))).

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
  [ dlss:add_storage(?NAME(Name,?DATA,Type), DLSSType, O) || { Type, #{ type:=DLSSType } = O } <- ?DB],
  [ dlss:add_storage(?NAME(Name,?INDEX,Type), DLSSType, O) || { Type, #{ type:=DLSSType } = O } <- ?DB],
  ok.

remove_db(Name)->
  [ dlss:remove_storage( ?NAME(Name,?DATA,Type) ) || { Type, _ } <- ?DB ],
  [ dlss:remove_storage( ?NAME(Name,?INDEX,Type) ) || { Type, _ } <- ?DB ],
  ok.

get_storage_name(Name,Storage,Type)->
  ?NAME(Name,Storage,Type).

get_storages()->
  [ S || S <- dlss:get_storages(), string:prefix(atom_to_list(S),"ecomet_")=/=nomatch ].

get_segments(Storage)->
  dlss:get_segments( Storage ).
get_segments(Name,Storage,Type)->
  get_segments(?NAME(Name,Storage,Type)).

get_root_segment(Storage)->
  dlss:get_storage_root( Storage ).
get_root_segment(Name,Storage,Type)->
  get_root_segment(?NAME(Name,Storage,Type)).

get_segment_info(Segment)->
  dlss:get_segment_info(Segment).

get_segment_params(Segment)->
  dlss:get_segment_info(Segment).

get_segment_size(Segment)->
  dlss:get_segment_size(Segment).

get_storage_type(Storage)->
  case dlss:get_storage_type(Storage) of
    ram->
      case is_local_storage(Storage) of
        true->?RAMLOCAL;
        _->?RAM
      end;
    Other->Other
  end.

add_segment_copy(Segment,Node)->
  dlss:add_segment_copy(Segment,Node).

remove_segment_copy(Segment,Node)->
  dlss:remove_segment_copy(Segment,Node).

is_local_storage(Storage)->
  dlss:is_local_storage(Storage).

get_supported_types()->
  ?STORAGE_TYPES.

%%=================================================================
%%	Data API
%%=================================================================
transaction(Fun)->
  dlss:transaction(Fun).

sync_transaction(Fun)->
  dlss:sync_transaction(Fun).

%%------------------------------------------------------------------
%%	Read
%%------------------------------------------------------------------
read(DB,Storage,Type,Key)->
  read(DB,Storage,Type,Key,read).
read(DB,Storage,Type,Key,Lock)->
  dlss:read(?NAME(DB,Storage,Type), Key, Lock).

dirty_read(DB,Storage,Type,Key)->
  dlss:dirty_read( ?NAME(DB,Storage,Type), Key ).
dirty_read(DB,Storage,Type,Key,_Cache)->
  % TODO. Implement the cache
  dirty_read(DB,Storage,Type,Key).

%%------------------------------------------------------------------
%%	Write
%%------------------------------------------------------------------
write(DB,Storage,Type,Key,Value)->
  write(DB,Storage,Type,Key,Value,none).
write(DB,Storage,Type,Key,Value,Lock)->
  dlss:write( ?NAME(DB,Storage,Type),Key,Value,Lock).

dirty_write(DB,Storage,Type,Key,Value)->
  dlss:dirty_write(?NAME(DB,Storage,Type),Key,Value).

%%------------------------------------------------------------------
%%	Delete
%%------------------------------------------------------------------
delete(DB,Storage,Type,Key)->
  delete(DB,Storage,Type,Key,none).
delete(DB,Storage,Type,Key,Lock)->
  dlss:delete(?NAME(DB,Storage,Type),Key,Lock).

dirty_delete(DB,Storage,Type,Key)->
  dlss:dirty_delete(?NAME(DB,Storage,Type),Key).


