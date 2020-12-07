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
-module(ecomet_pattern).

-include("ecomet.hrl").
-include("ecomet_schema.hrl").

-behaviour(ecomet_object).

%%=================================================================
%%	Service API
%%=================================================================
-export([

  get_map/1,
  edit_map/2,

  get_behaviours/1,
  set_behaviours/2,

  get_storage/1,
  get_parent/1,get_parents/1,
  get_children/1,get_children_recursive/1,
  get_fields/1,get_fields/2,
  is_empty/1,
  index_storages/1
]).

%%=================================================================
%%	Schema API
%%=================================================================
-export([
  append_field/3,
  remove_field/2
]).

%%===========================================================================
%% Ecomet object behaviour
%%===========================================================================
-export([
  on_create/1,
  on_edit/1,
  on_delete/1
]).

%%=================================================================
%%	Service API
%%=================================================================
get_map(Map) when is_map(Map)->
  Map;
get_map(Pattern)->
  case ecomet_transaction:dict_get({'@pattern_map@',Pattern},undefined) of
    undefined->
      case ecomet_schema:get_pattern(?OID(Pattern)) of
        Value when is_map(Value)->Value;
        _->#{}
      end;
    UnderTransaction->
      UnderTransaction
  end.

edit_map(Pattern,Map)->
  PatternID=?OID(Pattern),
  case ecomet_transaction:dict_get({'@pattern_map@',PatternID},undefined) of
    undefined->
      ecomet_schema:set_pattern(PatternID,Map);
    _UnderTransaction->
      ecomet_transaction:dict_put(#{{'@pattern_map@',PatternID}=>Map})
  end.

wrap_transaction(PatternID,Fun)->
  % Check if the schema is already under transaction
  RootTransaction = ecomet_transaction:dict_get({'@pattern_map@',PatternID},undefined)=:=undefined,
  % Put the schema into the transaction
  Map = get_map(PatternID),
  ecomet_transaction:dict_put(#{
    {'@pattern_map@',PatternID}=>Map
  }),
  % Perform schema transformations
  Fun(Map),
  % Commit the schema if there is no upper level transaction
  if
    RootTransaction ->
      NewMap=get_map(PatternID),
      ecomet_transaction:dict_remove([{'@pattern_map@',PatternID}]),
      edit_map(PatternID,NewMap);
    true ->
      ok
  end.

get_behaviours(Map) when is_map(Map)->
  maps:get(handlers,Map,[]);
get_behaviours(Pattern)->
  Map = get_map(Pattern),
  get_behaviours(Map).


set_behaviours(Map,Handlers) when is_map(Map)->
  Map#{handlers=>Handlers};
set_behaviours(Pattern,Handlers)->
  Map = get_map(Pattern),
  Map1 = set_behaviours(Map,Handlers),
  edit_map(Pattern,Map1),
  Map1.

get_storage(OIDOrMap)->
  Map = get_map(OIDOrMap),
  % The storage type of an object is defined by its .name field
  case ecomet_field:get_storage(Map,<<".name">>) of
    {ok,Storage}->Storage;
    _->
      % If the pattern is not developed yet the default storage type is disc
      ?DISC
  end.

get_parent(Pattern)->
  Object = ?OBJECT(Pattern),
  {ok,Parent} = ecomet:read_field(Object,<<"parent_pattern">>),
  Parent.

get_parents(Pattern)->
  Object = ?OBJECT(Pattern),
  {ok,Parents} = ecomet:read_field(Object,<<"parents">>),
  Parents.

get_children(Pattern)->
  OID=?OID(Pattern),
  ecomet_query:system([?ROOT],[<<".oid">>],{'AND',[
    {<<".pattern">>,'=',{?PATTERN_PATTERN,?PATTERN_PATTERN}},
    {<<"parent_pattern">>,'=',OID}
  ]}).

get_children_recursive(Pattern)->
  OID=?OID(Pattern),
  ecomet_query:system([?ROOT],[<<".oid">>],{'AND',[
    {<<".pattern">>,'=',{?PATTERN_PATTERN,?PATTERN_PATTERN}},
    {<<"parents">>,'=',OID}
  ]}).

get_fields(Pattern)->
  Map = get_map(Pattern),
  maps:filter(fun(Name,_Value)->is_binary(Name) end,Map).

get_fields(Pattern,Names)->
  Map = get_map(Pattern),
  Fields=
    [case Map of
       #{F := Config}->{ F, Config };
       _-> ?ERROR({invalid_field,F})
     end || F <- Names],
  maps:from_list(Fields).

is_empty(Pattern)->
  OID=?OID(Pattern),
  DBs=ecomet_db:get_databases(),
  0 =:= ecomet_query:system(DBs,[count],{<<".pattern">>,'=',OID}).

index_storages(Pattern)->
  #{index := Index} = get_map(Pattern),
  Index.

%%=================================================================
%%	Schema API
%%=================================================================
append_field(Pattern,Field,Config)->
  wrap_transaction(?OID(Pattern),fun(Map)->
    % Check for conflicts with the parent
    case Map of
      #{ Field:=_ }->
        % The field is updated. Check if it conflicts with the parent
        ParentID = get_parent(Pattern),
        % Get the parent's map (see THE TRICK below)
        case get_map(ParentID) of
          #{Field := Parent} ->
            ecomet_field:check_parent(Config,Parent);
          _->
            % The field is not defined in the parent
            ok
        end;
      _->
        % The field didn't exist, it is either inherit process or
        % creating a new field that is not defined in the parent.
        % In both cases we don't need to check parent config
        ok
    end,

    % Update the map
    Map1 = Map#{Field => Config},
    Map2 = Map1#{ index => get_indexed(Map1) },

    edit_map(Pattern,Map2),

    % Update children
    Fields = get_fields(Map2),
    [ inherit_fields(ChildID,Fields) || ChildID <- get_children(Pattern)]

  end),

  ok.

remove_field(Pattern,Field)->

  wrap_transaction(?OID(Pattern),fun(Map)->

    % Check if the field is defined in the parent
    ParentID = get_parent(Pattern),
    case get_map(ParentID) of
      #{Field:=_} -> ?ERROR(parent_field);
      _->ok
    end,

    % Update the map
    Map1 = maps:remove(Field,Map),
    Map2 = Map1#{ index => get_indexed(Map1) },

    edit_map(Pattern,Map2),

    % update children
    [ remove_field(ChildID,Field) || ChildID <- get_children(Pattern) ]
  end),

  ok.

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  check_handler(Object),
  set_parents(Object),
  inherit_fields(Object),
  update_behaviour(Object),
  ok.

on_edit(Object)->
  check_parent(Object),
  check_handler(Object),
  update_behaviour(Object),
  ok.

on_delete(Object)->
  case is_empty(?OID(Object)) of
    true->ok;
    _->?ERROR(has_objects)
  end.

check_parent(Object)->
  case ecomet:field_changes(Object,<<"parent_pattern">>) of
    none->ok;
    _->?ERROR(cannot_change_parent)
  end.

check_handler(Object)->
  case ecomet:field_changes(Object,<<"behaviour_module">>) of
    none->ok;
    {none,_}->ok;
    {NewHandler,_}->
      check_handler_module(NewHandler)
  end.

update_behaviour(Object)->
  ParentID=get_parent(Object),
  ParentHandlers=get_behaviours(ParentID),
  update_behaviour(Object,ParentHandlers),
  ok.


check_handler_module(Module)->
  case ?PIPE([
    fun(_)->true = ecomet_lib:module_exists(Module), ok end,
    fun(_)->true = erlang:function_exported(Module,on_create,1), ok end,
    fun(_)->true = erlang:function_exported(Module,on_edit,1), ok end,
    fun(_)->true = erlang:function_exported(Module,on_delete,1), ok end
  ],none) of
    {ok,_}->ok;
    {error,1,_}->?ERROR(invalid_module);
    {error,2,_}->?ERROR(undefined_on_create);
    {error,3,_}->?ERROR(undefined_on_edit);
    {error,4,_}->?ERROR(undefined_on_delete)
  end.

update_behaviour(Pattern,ParentHandlers)->
  Object = ?OBJECT(Pattern),
  Handlers=
    case ecomet:read_field(Object,<<"behaviour_module">>) of
      {ok,none}->ParentHandlers;
      {ok,Module}->[Module|ParentHandlers]
    end,
  case get_behaviours(Object) of
    Handlers->ok;
    _->
      set_behaviours(Object,Handlers),
      [ update_behaviour(ChildID,Handlers) || ChildID <- get_children(Object) ]
  end.

set_parents(Object)->
  Parent = get_parent(Object),
  GrandParents = get_parents(Parent),
  ok = ecomet:edit_object(Object,#{<<"parents">> => [Parent|GrandParents]}).


inherit_fields(Object)->
  ParentID = get_parent(Object),
  ParentFields = get_fields(ParentID),

  PatternID = ?OID(Object),
  wrap_transaction(PatternID,fun(_)->
    inherit_fields(PatternID,ParentFields)
  end),
  ok.


%%=================================================================
%%	Internal helpers
%%=================================================================
inherit_fields(PatternID,ParentFields)->
  ChildFields = get_fields(PatternID),

  [case ChildFields of
     #{ Name := Child}->
       % CASE 1. The field is already defined in the child, inherit algorithm
       Child1 = ecomet_field:inherit(Child,Parent),
       Child2 = ecomet_field:from_schema(Child1),
       % Edit object
       {ok, FieldID} =ecomet_folder:find_object(PatternID, Name),
       {ok, Field} = ecomet:open(FieldID,write),
       ok = ecomet:edit_object(Field,Child2);
     _->
       % CASE 2. The field is not defined in the child yet, create a new one
      Parent1 = ecomet_field:from_schema(Parent),
      ecomet:create_object(Parent1#{
        <<".name">>=>Name,
        <<".folder">>=>PatternID,
        <<".pattern">>=>{?PATTERN_PATTERN,?FIELD_PATTERN}
      })
   end || {Name, Parent} <- ordsets:from_list(maps:to_list(ParentFields)) ],
  ok.

get_indexed(Map)->
  Types=
    maps:fold(fun(Name,_Config, Acc)->
        case ecomet_field:get_index(Map,Name) of
          {ok, Index} when is_list(Index)->
            {ok,Storage}=ecomet_field:get_storage(Map,Name),
            Acc#{Storage=>true};
          _->
            Acc
        end
    end,#{}, get_fields(Map) ),
  maps:keys(Types).











