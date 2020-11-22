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
%% ====================================================================
%% API for object fields (alphabetical order)
%% ====================================================================
-export([
  build_new/2,
  copy_to_pattern/2,
  delete_object/2,
  field_changes/4,
  field_names/1,
  fields_storages/1,
  get_default/2,
  get_index/2,
  get_storage/1, get_storage/2,
  get_type/2,
  get_value/3, get_value/4,
  index_storages/1,
  is_required/2,
  lookup_storage/3,
  merge/3,
  save_changes/4
]).

-ifdef(TEST).
-export([
	test_build_map/1,
	test_build_description/1,
	map_add/3,
	map_delete/2,
	get_changes/3,
	merge_storages/4,
+dump_storages/2
	]).
-endif.

-define(STORAGELEVEL(Storage), string:str([ramlocal, ram, ramdisc, disc], [Storage])).

-define(IS_SYSTEM(FieldName), lists:member(FieldName, [
	<<".name">>,
	<<".folder">>,
	<<".readgroups">>,
	<<".writegroups">>,
	<<".ts">>,
	<<".deleted">>])
).

% @edoc metadata describing object in a format of map where
% each key is field_key() and corresponding value is #field{} or
% key is service_key() and corresponding value is any service info
-type description() :: map().

% @edoc ecomet object denotes map where earch key is field_key()
% and corresponding value is field_value() of any() type
-type object() :: map().

% @edoc object identifier
-type oid() :: {non_neg_integer(), non_neg_integer()}.

% @edoc value of field is valid erlang term
-type field_value() :: term().

% @edoc service key is sort of internal key and is atom
-type service_key() :: atom().

% @edoc field key of any field in DB is binary
-type field_key() :: binary().

% @edoc storage type of particular field for certain ecomet object 
-type storage_type() :: ramdisc | disc | ram. 

% @edoc field description
-type field() :: {service_key() | field_key(), field_value()}.

% @edoc field type
-type field_type() :: string | {list, string}.

% @edoc storage name
-type storage() :: atom().

% @edoc internal presentation of field specification:
% list of #field{} defines ecomet object
-record(field, {type, subtype, index, required, storage, default, autoincrement}).

%%===========================================================================
%% API for object fields
%%===========================================================================
% @edoc Build fields structure on object creation
-spec build_new(DescMap :: description(), FieldList :: [field()]) -> object().

build_new(DescMap, FieldList)->
  NewFields = maps:from_list(FieldList),
	maps:from_list(maps:fold(
    fun
      (Name, _FieldDesc, FieldsAcc) when is_atom(Name) ->
        % FieldsMap contain service info under atom keys
        FieldsAcc;
      (Name, FieldDesc, FieldsAcc) ->
        Default = auto_value(FieldDesc),
		    Value = maps:get(Name, NewFields, Default),
		    check_value(FieldDesc, Value),
		    [{Name, Value} | FieldsAcc]
	  end,
    [],
    DescMap
  )).

%% @edoc Helper. Copy field to pattern
copy_to_pattern(Field, PatternID)->
	{ok, FieldStorage} = ecomet:read_field(Field, <<"storage">>),
	{ok, PatternStorage} = ecomet_pattern:get_storage(PatternID),
	EditFields = case ?STORAGELEVEL(PatternStorage) of
		PatternLevel when PatternLevel > 2 -> % Object is persistent
			case ?STORAGELEVEL(FieldStorage) of
				FieldLevel when FieldLevel > 2 -> % Field is persistent too
          [{<<".folder">>,PatternID}];
				_-> % Field is temporary
					{ok, FieldName} = ecomet:read_field(Field, <<".name">>),
					case ?IS_SYSTEM(FieldName) of
						% Field is not system. Persistent objects can contain temporary fields
						false ->
              [{<<".folder">>, PatternID}];
						% Fields is system. It must be as persistent as object
						true ->
              [{<<".folder">>, PatternID}, {<<"storage">>, PatternStorage}]
					end
			end;
			% Object is temporary. Field must be as temporary and local as object
			_ ->
        [{<<".folder">>, PatternID}, {<<"storage">>, PatternStorage}]
		end,
	ecomet_object:copy(Field, EditFields).

% @edoc Delete object
-spec delete_object(DescMap :: description(), OID :: oid()) -> [].

delete_object(DescMap, OID)->
	StorageList = maps:fold(
    fun
      (FieldName, _FieldDesc, StoragesAcc) when is_atom(FieldName) ->
        StoragesAcc;
      (FieldName, FieldDesc, StoragesAcc) when is_atom(FieldName) ->
		    [get_storage(FieldDesc) | StoragesAcc]
		end,
	  [],
    DescMap
  ),
  %@TODO foreach over ordset??
	lists:foreach(
    fun(Type)-> ecomet_object:delete_storage(OID, Type, fields) end,
    ordsets:from_list(StorageList)
  ).

% @edoc Get changes for the field
-spec field_changes(description(), map(), oid(), field_key())
-> 
    none | {New :: field_value(), Old :: field_value()}.

field_changes(DescMap, Project, OID, FieldName)->
	case maps:find(FieldName, Project) of
		{ok, New} -> % Field is in the project. It's changed or it is object creating
			% Compare with old value
			{ok, Storage} = get_storage(DescMap, FieldName),
			case lookup_storage(Storage, OID, FieldName) of
				New->
          none;
				Old->
          {New, Old}
			end;
		error->
      none
	end.

% @edoc Get list of all fields in the object
-spec field_names(DescMap :: description()) -> [field_key()].

field_names(DescMap)->
	maps:fold(
    fun
      (FieldName, _Map, FieldsAcc) when is_atom(FieldName) ->
        FieldsAcc;
      (FieldName, _Map, FieldsAcc) ->
        [FieldName | FieldsAcc]
	  end,
    [],
    DescMap
  ).

% @edoc Return list of fields storages for object
-spec fields_storages(DescMap :: description()) -> ordsets:new().

fields_storages(DescMap)->
	ordsets:from_list(
    maps:fold(
      fun
        (FieldName, _Desc, StoragesAcc) when is_atom(FieldName) ->
			    StoragesAcc;
			  (_FieldName, Desc, StoragesAcc) ->
          [Desc#field.storage | StoragesAcc]
	    end,
      [],
      DescMap)
    ).

% @edoc Get default value for the field
-spec get_default(DescMap :: description(), FieldName :: field_key()) ->
    {ok, any()} | {error, undefined_field}.

get_default(DescMap, FieldName)->
	case maps:find(FieldName, DescMap) of
		{ok, #field{default=Default}} ->
      {ok, Default};
		error ->
      {error, undefined_field}
	end.

% @edoc Get field indexes
-spec get_index(DescMap :: description(), FieldName :: field_key()) ->
    {ok, any()} | {error, undefined_field}.

get_index(DescMap, FieldName)->
	case maps:find(FieldName, DescMap) of
		{ok, #field{index=Index}} ->
      {ok, Index};
		error->
      {error, undefined_field}
	end.

% @edoc Get type of storage for the field
% TODO We need field storage while compiling query. How to obtain it?
-spec get_storage(FieldDescription :: #field{}) -> storage_type().

get_storage(#field{storage=Storage}) ->
  Storage;
get_storage(FieldID)->
	lookup_storage(ramdisc, FieldID, <<"storage">>).

% @edoc Get type of storage for the field
-spec get_storage(
    DescMap   :: description(),
    FieldName :: field_key()
) ->
  {ok, Storage :: storage_type()} | {error, undefined_field}.

get_storage(DescMap, FieldName) ->
	case maps:find(FieldName, DescMap) of
		{ok, #field{storage=Storage}} ->
      {ok, Storage};
		error ->
      {error, undefined_field}
	end.

% @edoc Get field type
-spec get_type(
    DescMap   :: description(),
    FieldName :: field_key()
) ->
  {ok, Type :: field_type()} | {error, undefined_field}.

get_type(DescMap, FieldName)->
	case maps:find(FieldName, DescMap) of
		{ok, Desc}->
			{ok, get_type(Desc)};
		error ->
      {error, undefined_field}
	end.

get_type(#field{type = list, subtype = SubType}) ->
	{list, SubType};
get_type(#field{type = Type}) ->
	Type.

% @doc Get field type with default
-spec get_value(
    DescMap   :: description(),
    OID       :: oid(),
    FieldName :: field_key(),
    Default   :: field_value()
) ->
  {ok, Value :: field_value()} | {error, Reason :: any()}.

get_value(DescMap, OID, FieldName, Default)->
	case get_value(DescMap, OID, FieldName) of
		{ok, none} ->
      Default;
		{ok, Value} ->
      Value
	end.

% @edoc Get field type
-spec get_value(
    DescMap   :: description(),
    OID       :: oid(),
    FieldName :: field_key()
) ->
  {ok, Value :: field_value()} | {error, Reason :: any()}.

get_value(DescMap, OID, FieldName)->
	case get_storage(DescMap, FieldName) of
    {error, _} = Err ->
      Err;
		{ok, StorageType} ->
	    case ecomet_object:load_storage(OID, StorageType) of
		    none -> % Storage is empty
          {ok,none}; 
		    Storage ->
          {ok, maps:get(FieldName, Storage, none)}
	    end
  end.

% @edoc Return list of storages, that contain indexes for the fields
-spec index_storages(DescMap :: description()) -> [storage_type()].

index_storages(DescMap) ->
	Storages = maps:get(index_storage, DescMap),
	{StorageList,_} = lists:unzip(Storages),
	StorageList.

% @edoc Check if field is required
-spec is_required(
    DescMap   :: description(),
    FieldName :: field_key()
) ->
  {ok, boolean()} | {error, undefined_field}.

is_required(DescMap, FieldName)->
	case maps:find(FieldName, DescMap) of
		{ok, #field{required=IsRequired}} ->
      {ok, IsRequired};
		error->
      {error, undefined_field}
	end.

% @doc Direct dirty lookup in storage
-spec lookup_storage(
    Type      :: storage_type(),
    OID       :: oid(),
    FieldName :: field_key()
) ->
  Value :: none | field_value().

lookup_storage(Type, OID, FieldName)->
	case ecomet_object:load_storage(OID, Type) of
		none ->
      none;
		Storage ->
      maps:get(FieldName, Storage, none)
	end.

% @edoc Merge new values on object edit
-spec merge(
    DescMap   :: description(),
    Project   :: map(),
    NewFields :: [{field_key(), field_value()}]
) ->
  map().

merge(DescMap, Project, NewFields)->
	lists:foldl(
    fun({FieldName, Value}, ProjectAcc) ->
      FieldSpec = maps:find(FieldName, DescMap),
		  {ok, Desc} = ?assertNotMatch(error, FieldSpec, {undefined_field, FieldName}),
		  check_value(Desc, Value),
		  ProjectAcc#{FieldName => Value}
	  end,
    Project,
    NewFields
  ).

% @edoc Save field changes to storage
-spec save_changes(
    DescMap   :: description(),
    Project   :: [any()],
    PreloadedStorages :: [storage()],
    OID       :: oid()
) ->
  map().

save_changes(DescMap, Project, PreloadedStorages, OID)->
	% 1. Group project by storages
	Changed = maps:to_list(get_changes(maps:to_list(Project), DescMap, #{})),
	% 2. Merge existing values into changed storages
	{Merged, ChangedFields} = merge_storages(Changed, PreloadedStorages, OID, {#{}, []}),
	% 3. Save storages
	dump_storages(maps:to_list(Merged), OID),
	ChangedFields.

%%=============================================================================
%% TEST functions
%%=============================================================================
% @edoc Dump fields storages
dump_storages([], _OID) ->
  ok;
dump_storages([{Type, Fields}| Rest], OID)->
	case maps:size(Fields) > 0 of
		true->
      ecomet_object:save_storage(OID, Type, fields, Fields);
		false->
      ecomet_object:delete_storage(OID, Type, fields)
	end,
	dump_storages(Rest, OID).


% @edoc Build storage changes from projectors (field names and its values)
% returns projector map of aggregated per storage type 
-spec get_changes(
    [{FieldName :: field_key(), Value :: field_value()}],
    DescMap :: description(),
    InputAcc :: map()
) -> OutputAcc :: map().

get_changes([], _DescMap, Result) ->
  Result;
get_changes([{FieldName, Value}|Rest], DescMap, Storages) ->
	{ok, StorageType} = get_storage(DescMap, FieldName),
	Storage = maps:get(StorageType, Storages, #{}),
	get_changes(Rest, DescMap, Storages#{StorageType => Storage#{FieldName => Value}}).

% @edoc Merge unchanged values into changed storages
merge_storages([], _PreloadedStorages, _OID, Result) ->
  Result;
merge_storages([{Storage,Fields}|Rest],PreloadedStorages,OID,{MergedStorages,ChangedFields})->
	OldFields=
	case maps:find(Storage,PreloadedStorages) of
		{ok,none}->#{};
		{ok,PreLoaded}->PreLoaded;
		error->
			case ecomet_object:load_storage(OID,Storage) of
				none->#{};
				Loaded->Loaded
			end
	end,
	ChangedStorageFields=
	maps:fold(fun(Field,Value,ChangesList)->
		case maps:find(Field,OldFields) of
			% Value not changed
			{ok,Value}->ChangesList;
			% Really changed
			_->[{Field,Value}|ChangesList]
		end
	end,[],Fields),
	StorageResult=
	case ChangedStorageFields of
		% No real changes to storage, no need to save
		[]->MergedStorages;
		_->
			MergedFields=maps:merge(OldFields,Fields),
			ClearedFields=maps:filter(fun(_,Value)-> Value/=none end,MergedFields),
			case {maps:size(OldFields),maps:size(ClearedFields)} of
				% Storage did not exist and nothing to save now
				{0,0}->MergedStorages;
				% We here, if:
				% 1. storage had not existed before, but we have data to write now
				% 2. storage is updated
				% 3. storage had existed before, but all data is cleared (empty will be deleted on dump step)
				_->MergedStorages#{Storage=>ClearedFields}
			end
	end,
	merge_storages(Rest,PreloadedStorages,OID,{StorageResult,ChangedStorageFields++ChangedFields}).

%%=============================================================================
%% Helpers
%%=============================================================================
auto_value(#field{default = none, autoincrement = true}) ->
  erlang:integer_to_binary((erlang:unique_integer([positive,monotonic]) bsl 16) + ecomet_node:get_unique_id());
auto_value(#field{default = none, autoincrement = false}) ->
  none;
auto_value(#field{default = Default}) ->
	Default.

% Check field value or throw an error
check_value(#field{required=Required} = Desc, Value)->
	?assertNotMatch({true,none}, {Required,Value}, requierd_field),
	Type = get_type(Desc),
	?assertMatch(ok, ecomet_types:check_value(Type, Value), invalid_value).
