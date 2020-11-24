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

-include("ecomet.hrl").

%%=================================================================
%%	Service API
%%=================================================================
-export([
  build_new/2,
  build_description/1,
  map_add/3,
  field_names/1,
  index_storages/1,
  fields_storages/1
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  get_value/3,get_value/4,
  lookup_storage/3,
  field_changes/4
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

% Get type of storage for the field
get_storage(Map,Name)->
  case Map of
    #{Name := #{storage :=Storage }}-> {ok,Storage};
    _->{error,undefined_field}
  end.

% Build fields structure on object creation
build_new(Map,NewFields)->
  Fields=
    [begin
       Value =
         case NewFields of
           #{ Name:= Defined } -> Defined;
           _->
             Key = { maps:get(<<".pattern">>,NewFields), Name},
             auto_value(Config,Key)
         end,
       check_value(Config,Value),
       { Name ,Value }
     end || {Name,Config} <- maps:to_list(Map),is_binary(Name)],
  maps:from_list(Fields).


% Get auto value for field
auto_value(#{default:=Default,autoincrement:=Increment},Key)->
  case Default of
    none->
      case Increment of
        true->
          ID = ecomet_schema:local_increment(Key),
          ID bsl 16 + ecomet_node:get_unique_id();
        false->none
      end;
    _->Default
  end.

% Check field value
check_value(#{required:=true},none)->
  ?ERROR(requierd_field);
check_value(Config,Value)->
  Type = get_type(Config),
  case ecomet_types:check_value(Type,Value) of
    ok -> ok;
    _->?ERROR(invalid_value)
  end.

% Get list of all fields in the object
field_names(Map)->
  [ Name || Name <- maps:keys(Map), is_binary(Name) ].

get_type(#{type:=list,subtype:=SubType})->
  {list,SubType};
get_type(#{type:=Type})->
  Type.

% Return list of storages, that contain indexes for the fields
index_storages(Map)->
  Storages=maps:get(index_storage,Map),
  {StorageList,_}=lists:unzip(Storages),
  StorageList.

% Return list of fields storages for object
fields_storages(Map)->
  Types =
    [Type||{Name,#{storage:=Type}}<-maps:to_list(Map),is_binary(Name)],
  ordsets:from_list(Types).

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

% Direct dirty lookup in storage
lookup_storage(Type,OID,FieldName)->
  case ecomet_object:load_storage(OID,Type) of
    none->none;
    Storage->maps:get(FieldName,Storage,none)
  end.

% Get changes for the field. {NewValue, OldValue} or 'none' is returned
field_changes(Map,Project,OID,Name)->
  case Project of
    % Field is in the project. It's changed or it is object creating
    #{Name:=New}->
      % Compare with the old value
      {ok,Storage}=get_storage(Map,Name),
      case lookup_storage(Storage,OID,Name) of
        New->none;
        Old->{New,Old}
      end;
    error->none
  end.



