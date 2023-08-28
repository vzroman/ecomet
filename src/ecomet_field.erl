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
  get_type/2,
  get_storage/2,
  get_index/2,
  fields_storages/1,
  is_required/2
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  get_value/3,get_value/4,
  lookup_storage/3,
  field_changes/4
]).

%%=================================================================
%%	Schema API
%%=================================================================
-export([
  build_description/1,
  to_schema/1,
  from_schema/1,
  inherit/2,
  check_parent/2
]).
%%===========================================================================
%% Ecomet object behaviour
%%===========================================================================
-export([
  on_create/1,
  on_edit/1,
  on_delete/1
]).

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).
-export([
  check_name/2,
  check_folder/2,
  check_storage/2,
  check_type/2,
  check_index/2,
  check_default/2
]).
-endif.

-define(DEFAULT_DESCRIPTION,#{
  type => string,
  subtype => none,
  index => none,
  required => false,
  storage => disc,
  default => none,
  autoincrement => false
}).

-define(EXTENT_CRITICAL,[
  type,
  subtype,
  required
]).

%%=================================================================
%%	Service API
%%=================================================================
to_schema(Params) when is_map(Params)->
  maps:map(fun(K,Default)->
    maps:get( atom_to_binary(K,utf8), Params, Default )
  end,?DEFAULT_DESCRIPTION);
to_schema(ID)->
  Config = ecomet:read_fields(?OBJECT(ID),from_schema(?DEFAULT_DESCRIPTION)),
  to_schema(Config).

from_schema(Params)->
  maps:fold(fun(K, V ,Acc)->
    Acc#{ atom_to_binary(K,utf8) => V }
  end,#{},Params ).

build_description(Params)->
  % TODO. Should be deprecated
  maps:map(fun(K,Default)->
    maps:get(K,Params,Default)
  end,?DEFAULT_DESCRIPTION).

inherit(Child,Parent)->
  % Update extent critical params.
  % IMPORTANT! We don't check the extent existence, because it is performed by the field behaviour
  Child0 = maps:merge(Child,maps:with(?EXTENT_CRITICAL,Parent)),

  % Add new indexes. The index can be added but not removed
  ParentIndex=
    case Parent of
      #{index := Index1} when is_list(Index1) ->Index1;
      _ -> []
    end,
  ChildIndex =
    case Child of
      #{index := Index0} when is_list(Index0)-> Index0;
      _->[]
    end,
  Index=
    case ordsets:from_list(ChildIndex ++ ParentIndex) of
      []->none;
      NewIndex->NewIndex
    end,

  Child0#{index => Index}.

check_parent(Child,Parent)->

  % Children cannot change extent critical parameters defined in the parent
  case maps:to_list(maps:with(?EXTENT_CRITICAL,Child)) -- maps:to_list(maps:with(?EXTENT_CRITICAL,Parent)) of
    []->ok;
    _->?ERROR(parent_field)
  end,

  % Children cannot remove indexes defined in the parent
  ChildIndex=
    case Child of
      #{index := Index1} when is_list(Index1)->ordsets:from_list(Index1);
      _->[]
    end,
  ParentIndex=
    case Parent of
      #{index := Index0} when is_list(Index0)->ordsets:from_list(Index0);
      _->[]
    end,
  case ordsets:subtract(ParentIndex,ChildIndex) of
    []->ok;
    _->?ERROR(parent_index)
  end.

% Get type of storage for the field
get_storage(Map,Name)->
  % TODO. We should apply to the pattern for a field config
  % because the map is handled by the pattern
  case Map of
    #{Name := #{storage :=Storage }}-> {ok,Storage};
    _->{error,{undefined_field,Name}}
  end.

% Build fields structure on object creation
build_new(Map, NewFields)->
  maps:fold(fun
    (Name,Config,Acc) when is_binary(Name)->
      Value =
        case NewFields of
          #{ Name:= Defined } -> Defined;
          #{ <<".pattern">>:= PatternID }->
            Key = { PatternID, Name},
            auto_value(Config, Key)
        end,
      case check_value(Config,Value) of
        ok->ok;
        {error,Error}->?ERROR({Name,Error})
      end,
      if
        Value =/= none -> Acc#{ Name => Value };
        true -> Acc
      end;
    (_Name,_Config, Acc)->
      Acc
  end,#{},Map).

% Merge new values on object edit
merge(Map,Project,NewFields)->
  maps:fold(fun(Name,Value,OutProject)->
    case Map of
      #{Name := Config}->
        case check_value(Config,Value) of
          ok->ok;
          {error,Error}->?ERROR({Name,Error})
        end,
        OutProject#{Name=>Value};
      _->
        ?ERROR({undefined_field,Name})
    end
  end,Project,NewFields).

% Get auto value for field
auto_value(#{default:=Default, autoincrement:=Increment, type := Type}, Key)->
  case Default of
    none ->
      case Increment of
        true ->
          ID = ecomet_id:new(Key),
          Value = (ID bsl 16) + ecomet_node:get_unique_id(),
          case ecomet_types:parse_safe(Type, Value) of
            {ok, ParsedValue} ->
              ParsedValue;
            _ ->
              none
          end;
        false ->
          none
      end;
    _ ->
      Default
  end.

% Check field value
check_value(#{required:=true},none)->
  {error,required_field};
check_value(Config,Value)->
  Type = get_type(Config),
  case ecomet_types:check_value(Type,Value) of
    ok -> ok;
    _->{error,{invalid_value,Value}}
  end.

get_type(#{type:=list,subtype:=SubType})->
  {list,SubType};
get_type(#{type:=Type})->
  Type.
get_type(Map,Name)->
  case Map of
    #{ Name:= Config } -> {ok, get_type(Config)};
    _->{error,{undefined_field,Name}}
  end.

% Get field indexes
get_index(Map,Name)->
  case Map of
    #{Name := #{index := Index } }->{ ok, Index };
    _->{error,{undefined_field,Name}}
  end.

% Return list of fields storages for object
fields_storages(Map)->
  Types =
    [Type||{Name,#{storage:=Type}}<-maps:to_list(Map),is_binary(Name)],
  ordsets:from_list(Types).

% Check if field is required
is_required(Description, FieldName) ->
  case maps:get(FieldName, Description, undefined) of
    Spec when is_map(Spec) ->
      {ok, maps:get(required, Spec)};
    _ ->
      {error, {undefined_field,FieldName}}
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
    _->none
  end.

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  check_name(Object,true),
  check_folder(Object,true),
  check_storage(Object,true),
  check_type(Object,true),
  check_index(Object,true),
  check_default(Object,true),

  % Append the field to the schema
  { ok, Name }=ecomet:read_field(Object, <<".name">>),
  { ok, PatternID } = ecomet:read_field(Object, <<".folder">>),
  Config = to_schema(Object),
  ok = ecomet_pattern:append_field(PatternID, Name, Config).

on_edit(Object)->
  {ok,PatternID} = ecomet:read_field(Object,<<".folder">>),
  IsEmpty = ecomet_pattern:is_empty(PatternID),

  check_name(Object,IsEmpty),
  check_folder(Object,IsEmpty),
  check_storage(Object,IsEmpty),
  check_type(Object,IsEmpty),
  check_index(Object,IsEmpty),
  check_default(Object,IsEmpty),

  case ecomet:field_changes(Object,<<".name">>) of
    {NewName, OldName}->
      % Rename
      ok = ecomet_pattern:remove_field(PatternID,OldName),

      Config = to_schema(Object),
      ok = ecomet_pattern:append_field(PatternID, NewName, Config),
      ok;
    none->
      % Check for schema changes

      case [ F || F <- maps:keys( ecomet_object:object_changes( Object ) ),
        case F of
          <<".",_/binary>> -> false;
          _-> true
        end ]
      of
        []->
          % No real schema changes
          ok;
        _->
          % Append the field to the schema
          { ok, Name }=ecomet:read_field(Object, <<".name">>),
          { ok, PatternID } = ecomet:read_field(Object, <<".folder">>),
          Config = to_schema(Object),
          ok = ecomet_pattern:append_field(PatternID, Name, Config)
      end
  end.

on_delete(Object)->
  {ok,PatternID} = ecomet:read_field(Object,<<".folder">>),
  try
    ecomet:open(PatternID,none),
    case ecomet_pattern:is_empty(PatternID) of
      true->
        ok;
      _->
        % The field cannot be deleted if the extent already exists
        ?LOGWARNING("remove field ~ts from pattern that has extent",[ ?PATH(Object) ])
    end,
    % Update the schema
    {ok,Name}=ecomet:read_field(Object,<<".name">>),
    ecomet_pattern:remove_field(PatternID,Name)
  catch
    _:not_exists->ok
  end.


check_name(Object,IsEmpty)->
  case ecomet:field_changes(Object,<<".name">>) of
    none->false;
    { NewName, OldName }->
      % Check the name for forbidden symbols
      case re:run(NewName,"^\\.?([a-zA-Z0-9_-]+)$") of
        {match,_}->
          if
            OldName=/=none->
              % The field is going to be renamed, remove the old name from the schema
              {ok,PatternID}=ecomet:read_field(Object,<<".folder">>),
              ecomet_pattern:remove_field(PatternID,OldName);
            true -> ok
          end;
        _->?ERROR(invalid_name)
      end,
      if
        not IsEmpty ->
          % There are already objects created by the pattern. They potentially have data
          % in the field. If we change the field's name the data will be lost.
          #{<<".folder">> := PatternOID} = ecomet:read_fields( Object, [<<".name">>, <<".folder">>] ),

          % Clean old values
          ecomet:set('*',#{ OldName => none}, {<<".pattern">>,'=', PatternOID}),

          {ok, Pattern} = ecomet:read_field( ecomet:open(PatternOID), <<".name">> ),
          ?LOGWARNING("Change name for the field ~p in pattern ~p. Field values for existing objects will be lost",[
            OldName,
            Pattern
          ]);
        true ->
          ok
      end,
      true
  end.

check_folder(Object, _IsEmpty)->
  case ecomet:field_changes(Object,<<".folder">>) of
    none -> false;
    { _NewFolder, none }->
      % Create procedure
      true;
    { _NewFolder, _OldFolder }->
      ?ERROR(cannot_change_pattern)
  end.

check_storage(Object,IsEmpty)->
  case ecomet:field_changes(Object,<<"storage">>) of
    none->false;
    { NewStorage, _OldStorage } when NewStorage=:=?RAMDISC; NewStorage=:=?DISC->
      % Cannot create persistent fields in the memory only pattern
      {ok,PatternID} = ecomet:read_field(Object,<<".folder">>),
      PatternStorage = ecomet_pattern:get_storage(PatternID),
      if
        PatternStorage=:=?RAM ->
          ?ERROR(memory_only_pattern);
        true -> ok
      end,
      true;
    { NewStorage, _OldStorage } ->
      % Just check for other supported types
      case lists:member( NewStorage, ?STORAGE_TYPES ) of
        true->ok;
        _->?ERROR(invalid_storage_type)
      end,
      if
        not IsEmpty ->
          % There are already objects created by the pattern. They potentially have data
          % in the field. If we change the field's storage type the data will be lost.
          #{<<".name">> := Name, <<".folder">> := PatternOID} = ecomet:read_fields( Object, [<<".name">>, <<".folder">>] ),

          % Clean old values
          ecomet:set('*',#{ Name => none}, {<<".pattern">>,'=', PatternOID}),

          {ok, Pattern} = ecomet:read_field( ecomet:open(PatternOID), <<".name">> ),
          ?LOGWARNING("Change storage type for the field ~p in pattern ~p. Field values for existing objects will be lost",[
            Name,
            Pattern
          ]);
        true ->
          ok
      end,
      true
  end.

check_type(Object,IsEmpty)->
  IsChanged =
    ecomet:field_changes(Object,<<"type">>) =/=none
      orelse
    ecomet:field_changes(Object,<<"subtype">>) =/=none,
  if
    not IsChanged->false;
    true ->
      % Check the type for supported types
      Supported = ecomet_types:get_supported_types(),
      Config = to_schema(Object),
      Type = get_type(Config),
      case lists:member(Type,Supported) of
        true->ok;
        _->?ERROR(invalid_type)
      end,

      if
        not IsEmpty ->
          % There are already objects created by the pattern. They potentially have data
          % in the field. If we change the field's type the data will be lost.
          #{<<".name">> := Name, <<".folder">> := PatternOID} = ecomet:read_fields( Object, [<<".name">>, <<".folder">>] ),

          % Clean old values
          ecomet:set('*',#{ Name => none}, {<<".pattern">>,'=', PatternOID}),

          {ok, Pattern} = ecomet:read_field( ecomet:open(PatternOID), <<".name">> ),
          ?LOGWARNING("Change type for field ~p in pattern ~p. Field values for existing objects will be lost",[
            Name,
            Pattern
          ]);
        true ->
          ok
      end,

      true
  end.

check_index(Object,_IsEmpty)->
  case ecomet:field_changes(Object,<<"index">>) of
    none->false;
    { none, _OldStorage }->true;
    { NewIndex, _OldStorage }->
      Supported = ecomet_index:get_supported_types(),
      % Remove duplicates
      Index = ordsets:from_list(NewIndex),
      case Index -- Supported of
        []->ok;
        _->?ERROR(invalid_index_type)
      end,
      ecomet:edit_object(Object,#{<<"index">>=>Index}),
      true
  end.

check_default(Object,_IsEmpty)->
  ChangedFields = [ecomet:field_changes(Object,F)||F <- [<<"default">>, <<"type">>, <<"subtype">>]],
  IsChanged = lists:foldl(fun(Changes, Acc) ->
    case Changes of
      {New, _} when New =/= none -> true;
      _ -> Acc
    end
   end, false, ChangedFields),
  if
    IsChanged ->
      {ok, NewDefault} = ecomet:read_field(Object, <<"default">>),
      Type = get_type(to_schema(Object)),
      case ecomet_types:parse_safe(Type,NewDefault) of
        {ok,Parsed}->
          ok = ecomet:edit_object(Object,#{<<"default">>=>Parsed});
        _->?ERROR(invalid_default_value)
      end,
      true;
    true ->
      case hd(ChangedFields) of
        none -> false;
        {none, _Old} -> true
      end
  end.
