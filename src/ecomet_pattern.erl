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
  case ecomet_schema:get_pattern(?OID(Pattern)) of
    Value when is_map(Value)->Value;
    _->#{}
  end.

edit_map(Pattern,Map)->
  ecomet_schema:set_pattern(?OID(Pattern),Map).

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
  edit_map(Pattern,Map1).

get_storage(OIDOrMap)->
  % The storage type of an object is defined by its .name field
  Map = get_map(OIDOrMap),
  ecomet_field:get_storage(Map,<<".name">>).

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
  ecomet_query:system([?ROOT],[<<".oid">>],{<<"parent_pattern">>,'=',OID}).

get_children_recursive(Pattern)->
  OID=?OID(Pattern),
  ecomet_query:system([?ROOT],[<<".oid">>],{<<"parents">>,'=',OID}).

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
  Map = get_map(Pattern),

  % Check for conflicts with the parent
  case Map of
    #{ Field:=_ }->
      % The field is updated. Check if it conflicts with the parent
      ParentID = get_parent(Pattern),
      % Get the parent's map (see THE TRICK below)
      ParentMap =
        case get({'@pattern_map@',ParentID}) of
          undefined->get_map(ParentID);
          Stored->Stored
        end,
      case ParentMap of
        #{Field := Parent} ->
          ecomet_field:check_parent(Config,Parent);
        _->
          % The field is not defined in the parent
          ok
      end;
    _->
      % The field didn't exist, so it cannot be defined in the parent
      ok
  end,

  % Update the map
  Map1 = Map#{Field => Config},
  Map2 = Map1#{ index => get_indexed(Map1) },

  % Update the field config in the children.
  % THE TRICK! The schema is edited inside a transaction, therefore
  % changes are not seen until the transaction is committed. A child
  % on its change will try to check the changes against parent.
  % To make the changes in the parent available we save them into the process dictionary
  put({'@pattern_map@',?OID(Pattern)}, Map2),

  % Update children
  Fields = get_fields(Map2),
  [ inherit_fields(ChildID,Fields) || ChildID <- get_children(Pattern)],

  % Update the schema
  edit_map(Pattern, Map2).

remove_field(Pattern,Field)->
  % Check if the field is defined in the parent
  ParentID = get_parent(Pattern),
  % Get the parent's map (see THE TRICK below)
  ParentMap =
    case get({'@pattern_map@',ParentID}) of
      undefined->get_map(ParentID);
      Stored->Stored
    end,
  case ParentMap of
    #{Field:=_} -> ?ERROR(parent_field);
    _->ok
  end,

  % Update the map
  Map = get_map(Pattern),
  Map1 = maps:remove(Field,Map),
  Map2 = Map1#{ index => get_indexed(Map1) },

  % Update the field config in the children.
  % THE TRICK! The schema is edited inside a transaction, therefore
  % changes are not seen until the transaction is committed. A child
  % on its change will try to check the changes against parent.
  % To make the changes in the parent available we save them into the process dictionary
  put({'@pattern_map@',?OID(Pattern)}, Map2),

  % update children
  [ remove_field(ChildID,Field) || ChildID <- get_children(Pattern) ],

  % Update the schema
  edit_map(Pattern, Map2).

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  check_handler(Object),
  set_parents(Object),
  inherit_fields(Object),
  ok.

on_edit(Object)->
  check_parent(Object),
  check_handler(Object),
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
    {NewHandler,_}->
      check_handler_module(NewHandler),

      ParentID=get_parent(Object),
      ParentHandlers=get_behaviours(ParentID),
      update_behaviour(Object,ParentHandlers)
  end.

check_handler_module(Module)->
  case ?PIPE([
    fun(_)->true = ecomet_lib:module_exists(Module) end,
    fun(_)->true = erlang:function_exported(Module,on_create,1) end,
    fun(_)->true = erlang:function_exported(Module,on_edit,1) end,
    fun(_)->true = erlang:function_exported(Module,on_delete,1) end
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
  set_behaviours(Object,Handlers),
  [ update_behaviour(ChildID,Handlers) || ChildID <- get_children(Object) ].

set_parents(Object)->
  Parent = get_parent(Object),
  GrandParents = get_parents(Parent),
  ok = ecomet:edit_object(Object,#{<<"parents">> => [Parent|GrandParents]}).


inherit_fields(Object)->
  ParentID = get_parent(Object),
  ParentFields = get_fields(ParentID),
  inherit_fields(?OID(Object),ParentFields).


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
      ecomet:create_object(Parent#{
        <<".name">>=>Name,
        <<".folder">>=>PatternID,
        <<".pattern">>=>{?PATTERN_PATTERN,?FIELD_PATTERN}
      })
   end || {Name, Parent} <- maps:to_list(ParentFields)],
  ok.

get_indexed(Map)->
  Indexed =
    [ ecomet_field:get_storage(Map,Name) || { Name, _} <- get_fields(Map),
      case ecomet_field:get_index(Map,Name) of
        {ok, Index} when is_list(Index)->true;
        _->false
      end ],
  ordsets:from_list(Indexed).



