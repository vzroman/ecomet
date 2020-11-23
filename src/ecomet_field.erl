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
-module(ecomet_field).

%%=================================================================
%%	Service API
%%=================================================================
-export([
  build_new/2,
  build_description/1,
  map_add/3
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  get_value/3,get_value/4
]).

-define(DEFAULT_DESCRIPTION,#{
  type => string,
  subtype => none,
  index => none,
  required => false,
  storage => disc,
  default => none,
  autoincrement => false
}).

%%=================================================================
%%	Service API
%%=================================================================
build_description(Params)->
  maps:merge(?DEFAULT_DESCRIPTION,Params).

map_add(Map,Name,Field)->
  % Add the new description to the map
  Map1 = Map#{ Name => Field },
  % Update indexes meta-info if the field has indexes
  case Field of
    #{ index:=Indexes, storage:=Storage } when Indexes=/=none->
      % Storage types that impart in the indexing
      StorageList=maps:get(index_storage,Map,[]),
      % Remove the field from the storage types (if exists)
      StorageList1=
        [{Type,Fields--[Name]}||{Type,Fields} <- StorageList],
      % Current config of the field storage type
      StorageFields=proplists:get_value(Storage,StorageList1,[]),
      % Add the field to the storage type
      StorageList2 = [{Storage,[Name|StorageFields]}|proplists:delete(Storage,StorageList1)],
      % Update the index configuration to the schema
      Map1#{ index_storage => StorageList2};
    _->
      Map1
  end.

%%=================================================================
%%	Data API
%%=================================================================
get_value(Map,OID,Name,Default)->
  case get_value(Map,OID,Name) of
    {ok,none}->Default;
    {ok,Value}->Value
  end.
% Get field actual value
get_value(Map,OID,Name)->
  % Load storage
  case get_storage(Map,Name) of
    {error,Error}->
      {error,Error};
    {ok,StorageType}->
      case ecomet_object:load_storage(OID,StorageType) of
        % Storage is empty
        none->{ok,none};
        Storage->{ok,maps:get(Name,Storage,none)}
      end
  end.

% Get type of storage for the field
get_storage(Map,Name)->
  case Map of
    #{Name := #{storage :=Storage }}-> {ok,Storage};
    _->{error,undefined_field}
  end.

% Build fields structure on object creation
build_new(Map,NewFields)->
  ProjectAsList=
    maps:fold(fun(Name,FieldDesc,Fields)->
      % FieldsMap contain service info under atom keys
      if is_atom(Name)->Fields; true->
        Value=
          case maps:find(Name,NewFields) of
            error->auto_value(FieldDesc);
            {ok,DefinedValue}->DefinedValue
          end,
        check_value(FieldDesc,Value),
        [{Name,Value}|Fields]
      end
    end,[],Map),
  maps:from_list(ProjectAsList).


% Get auto value for field
auto_value(#{default:=Default,autoincrement:=Increment})->
  case Default of
    none->
      case Increment of
        true->
          integer_to_binary((erlang:unique_integer([positive,monotonic]) bsl 16)+ ecomet_node:get_unique_id());
        false->none
      end;
    _->Default
  end.