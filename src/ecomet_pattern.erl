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

append_field(Pattern,Field,Config)->
  Map = get_map(Pattern),
  % Update the field config in the children
  [].

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

inherit_fields(PatternID,ParentFields)->

  []

  Fields = get_fields(PatternID),
  ToAdd = maps:without(maps:keys(Fields),ParentFields),
  Added=
   [ begin
       Field = ecomet:copy_object(FieldID, #{<<".folder">> => PatternID}),
       { Name, ?OID(Field) }
     end || {Name,FieldID} <- maps:to_list(ToAdd) ],
  NewFields = maps:merge(Fields,maps:from_list(Added)),

  % Recursively update children patterns
  Children = get_children(PatternID),
  [ inherit_fields(ChildID,NewFields) || ChildID <- Children ].

