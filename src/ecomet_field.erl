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
  merge/3,
  build_description/1,
  map_add/3,
  field_names/1,
  get_type/2,
  index_storages/1,
  get_storage/2,
  get_index/2,
  fields_storages/1,
  save_changes/4,
  delete_object/2
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
           #{ <<".pattern">>:= PatternID }->
             Key = { PatternID, Name},
             auto_value(Config,Key)
         end,
       check_value(Config,Value),
       { Name ,Value }
     end || {Name,Config} <- maps:to_list(Map),is_binary(Name)],
  maps:from_list(Fields).

% Merge new values on object edit
merge(Map,Project,NewFields)->
  maps:fold(fun(Name,Value,OutProject)->
    case Map of
      #{Name := Config}->
        check_value(Config,Value),
        OutProject#{Name=>Value};
      _->?ERROR(undefined_field)
    end
  end,Project,NewFields).

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
get_type(Map,Name)->
  case Map of
    #{ Name:= Config } -> {ok, get_type(Config)};
    _->{error,undefined_field}
  end.

% Return list of storages, that contain indexes for the fields
index_storages(Map)->
  Storages=maps:get(index_storage,Map),
  {StorageList,_}=lists:unzip(Storages),
  StorageList.

% Get field indexes
get_index(Map,Name)->
  case Map of
    #{Name := #{index := Index } }->{ ok, Index };
    _->{error,undefined_field}
  end.

% Return list of fields storages for object
fields_storages(Map)->
  Types =
    [Type||{Name,#{storage:=Type}}<-maps:to_list(Map),is_binary(Name)],
  ordsets:from_list(Types).

% Save field changes to storage
save_changes(Map,Project,Loaded,OID)->
  % 1. Group project by storages
  Changed=get_changes(maps:to_list(Project),Map,#{}),
  % 2. Merge existing values into changed storages
  {Merged,ChangedFields}=merge_storages(maps:to_list(Changed),Loaded,OID,{#{},[]}),
  % 3. Save storages
  dump_storages(maps:to_list(Merged),OID),
  ChangedFields.

% Build storage changes from project
get_changes([{Name,Value}|Rest],Map,Storages)->
  {ok,StorageType}=get_storage(Map,Name),
  Storage=maps:get(StorageType,Storages,#{}),
  get_changes(Rest,Map,Storages#{StorageType=>Storage#{Name=>Value}});
get_changes([],_Map,Result)->Result.

% Merge unchanged values into changed storages
merge_storages([{Storage,Fields}|Rest],Loaded,OID,{Merged,Changes})->
  OldFields=
    case Loaded of
      #{ Storage := none }-> #{};
      #{ Storage := StorageFields } -> StorageFields;
      _->
        case ecomet_object:load_storage(OID,Storage) of
          none->#{};
          Loaded->Loaded
        end
    end,

  StorageChanges=
    maps:fold(fun(Field,Value,ChangesList)->
      case maps:find(Field,OldFields) of
        % Value not changed
        {ok,Value}->ChangesList;
        % Really changed
        _->[{Field,Value}|ChangesList]
      end
    end,[],Fields),

  StorageResult=
    case StorageChanges of
      % No real changes to storage, no need to save
      []->Merged;
      _->
        MergedFields=maps:merge(OldFields,Fields),
        ClearedFields=maps:filter(fun(_,Value)-> Value/=none end,MergedFields),
        case {maps:size(OldFields),maps:size(ClearedFields)} of
          % Storage did not exist and nothing to save now
          {0,0}->Merged;
          % We here, if:
          % 1. storage had not existed before, but we have data to write now
          % 2. storage is updated
          % 3. storage had existed before, but all data is cleared (empty will be deleted on dump step)
          _->Merged#{Storage=>ClearedFields}
        end
    end,
  merge_storages(Rest,Loaded,OID,{StorageResult,StorageChanges++Changes});
merge_storages([],_Loaded,_OID,Result)->Result.

% Dump fields storages
dump_storages([{Type,Fields}|Rest],OID)->
  case maps:size(Fields)>0 of
    true->ecomet_object:save_storage(OID,Type,fields,Fields);
    false->ecomet_object:delete_storage(OID,Type,fields)
  end,
  dump_storages(Rest,OID);
dump_storages([],_OID)->ok.

delete_object(Map,OID)->
  % TODO. Each storage is to lookup for the database name by OID, we can optimize it
  [ ecomet_object:delete_storage(OID,Type,fields) || Type <- fields_storages(Map) ],
  ok.

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



