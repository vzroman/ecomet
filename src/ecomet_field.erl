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

%% ====================================================================
%% API for object fields (alphabetical order)
%% ====================================================================
-export([
  build_new/2,
  get_value/3, get_value/4,
  copy_to_pattern/2,
  delete_object/2,
  field_changes/4,
  field_names/1,
  fields_storages/1,
  get_default/2,
  get_index/2,
  get_storage/1, get_storage/2,
  get_type/2,
  index_storages/1,
  is_required/2,
  lookup_storage/3,
  merge/3,
  save_changes/4
]).

% @todo describe
-type description() :: map().

% @todo describe
-type object() :: map().

% @doc Object identifier
-type oid() :: {non_neg_integer(), non_neg_integer()}.

% @doc Value of field is valid erlang term
-type field_value() :: term().

% @doc Internal key is atom
-type internal_key() :: atom().

% @doc External key is binary
-type external_key() :: binary().

% @doc Field specification
-type field() :: { internal_key() | external_key(), field_value()}.

% @todo describe
-record(field, {type, subtype, index, required, storage, default, autoincrement}).

%%===========================================================================
%% API for object fields
%%===========================================================================
% @doc Build fields structure on object creation
-spec build_new(Map :: description(), FieldList :: [field()]) -> object().

build_new(Map, FieldList)->
  NewFields = maps:from_list(FieldList),
	maps:from_list(maps:fold(
    fun
      (Name, _FieldDesc, FieldsAcc) when is_atom(Name) ->
        % FieldsMap contain service info under atom keys
        FieldsAcc;
      (Name, FieldDesc, FieldsAcc) ->
        Default = auto_value(FieldDesc),
		    Value = maps:get(Name, NewFields, Default),
		    try_value(FieldDesc, Value),
		    [{Name, Value} | FieldsAcc]
	  end,
    [],
    Map
  )).

% @doc Get field type with default
-spec get_value(
    Map     :: description(),
    OID     :: oid(),
    Key     :: external_key(),
    Default :: field_value()
) ->
  {ok, Value :: field_value()} | {error, Reason :: any()}.

get_value(Map, OID, Name, Default)->
	case get_value(Map, OID, Name) of
		{ok, none} ->
      Default;
		{ok, Value} ->
      Value
	end.

% @doc Get field type
-spec get_value(
    Map     :: description(),
    OID     :: oid(),
    Key     :: external_key()
) ->
  {ok, Value :: field_value()} | {error, Reason :: any()}.

get_value(Map, OID, Key)->
	% Load storage
	case mnemonic_oid4storage(OID) of
    {error, _} = Err ->
      Err;
		{ok, Storage} ->
	    case dlss:read(Storage, Key) of
		    not_found -> % Storage is empty
          {ok, none};
		    Value ->
          {ok, Value}
	    end
  end.

auto_value(#field{default = none, autoincrement = true}) ->
  erlang:integer_to_binary((erlang:unique_integer([positive,monotonic]) bsl 16) + ecomet_node:get_unique_id());
auto_value(#field{default = none, autoincrement = false}) ->
  none;
auto_value(#field{default = Default}) ->
	Default.

get_type(Map, Name)->
	case maps:find(Name, Map) of
		{ok, Desc}->
			{ok, get_type(Desc)};
		error ->
      {error, undefined_field}
	end.

get_type(#field{type = list, subtype = SubType}) ->
	{list, SubType};
get_type(#field{type = Type}) ->
	Type.

% Check field value or throw an error
try_value(#field{required = true}, none) ->
  erlang:error(required_field);
try_value(Desc, Value) ->
  case ecomet_types:check_value(get_type(Desc), Value) of
    ok ->
      ok;
    _ ->
      erlang:error(invalid_value)
  end.
