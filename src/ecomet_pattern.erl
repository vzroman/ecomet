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
  get_storage_types/1
]).

%%=================================================================
%%	Schema API
%%=================================================================
-export([
  append_field/3,
  remove_field/2
]).

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([
  check_handler/1,
  check_handler_module/1,
  check_parent/1,
  set_parents/1,
  wrap_transaction/2
]).

-endif.

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
  PatternID = ?OID(Pattern),
  case ecomet_transaction:dict_get({'@pattern_map@',PatternID},undefined) of
    undefined->
      case ecomet_schema:get_pattern(PatternID) of
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
    {<<".pattern">>,':=',{?PATTERN_PATTERN,?PATTERN_PATTERN}},
    {<<"parent_pattern">>,'=',OID}
  ]})--[OID].

get_children_recursive(Pattern)->
  OID=?OID(Pattern),
  ecomet_query:system([?ROOT],[<<".oid">>],{'AND',[
    {<<".pattern">>,':=',{?PATTERN_PATTERN,?PATTERN_PATTERN}},
    {<<"parents">>,'=',OID}
  ]})--[OID].

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

get_storage_types(Pattern)->
  #{storage_types := Types} = get_map(Pattern),
  Types.

%%=================================================================
%%	Schema API
%%=================================================================
append_field(Pattern,Field,Config)->
  wrap_transaction(?OID(Pattern),fun(Map)->
    % Check for conflicts with the parent
    case Map of
      #{ Field:=_ }->
        % The field is updated. Check if it conflicts with the parent
        case { ?OID(Pattern), get_parent(Pattern) } of
          { Same, Same }->
            % Protection from infinite loop for the .object
            ok;
          { _, ParentID}->
            % Get the parent's map (see THE TRICK below)
            case get_map(ParentID) of
              #{Field := Parent} ->
                ecomet_field:check_parent(Config,Parent);
              _->
                % The field is not defined in the parent
                ok
            end
        end;
      _->
        % The field didn't exist, it is either inherit process or
        % creating a new field that is not defined in the parent.
        % In both cases we don't need to check parent config
        ok
    end,

    % Update the map
    Map1 = Map#{Field => Config},
    Map2 = Map1#{
      storage_types => update_storage_types(Map1)
    },

    edit_map(Pattern,Map2),

    % Update children
    Fields = get_fields(Map2),
    [ inherit_fields(ChildID,Fields) || ChildID <- get_children(Pattern)]

  end),

  ok.

remove_field(Pattern,Field)->

  wrap_transaction(?OID(Pattern),fun(Map)->

    % Check if the field is defined in the parent
    case { ?OID(Pattern), get_parent(Pattern) } of
      { Same, Same }->
        ok;
      { _, ParentID }->
        case get_map(ParentID) of
          #{Field:=_} -> ?ERROR(parent_field);
          _->ok
        end
    end,

    % Update the map
    Map1 = maps:remove(Field,Map),
    Map2 = Map1#{
      storage_types => update_storage_types(Map1)
    },

    edit_map(Pattern,Map2),

    % update children
    [ remove_field(ChildID,Field) || ChildID <- get_children(Pattern) ]
  end),

  ok.

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  check_db(Object),
  check_handler(Object),
  set_parents(Object),
  wrap_transaction(?OID(Object),fun(_)->
    inherit_fields(Object),
    update_behaviour(Object)
  end),
  ok.

on_edit(Object)->
  check_parent(Object),
  check_handler(Object),
  wrap_transaction(?OID(Object),fun(_)->
    update_behaviour(Object)
  end),
  ok.

on_delete(Object)->
  case is_empty(?OID(Object)) of
    true-> [ecomet:delete_object(ecomet:open(ChildID)) || ChildID <- get_children(Object)], ok;
    _->?ERROR(has_objects)
  end.

check_parent(Object)->
  case ecomet:field_changes(Object,<<"parent_pattern">>) of
    none->ok;
    _->?ERROR(cannot_change_parent)
  end.

check_db(Object)->
  {ok,FolderID} = ecomet:read_field(Object,<<".folder">>),
  case ecomet_object:get_db_name(FolderID) of
    ?ROOT->ok;
    _->?ERROR(not_root_database)
  end.

check_handler(Object)->
  case ecomet:field_changes(Object,<<"behaviour_module">>) of
    none->ok;
    {none,_}->ok;
    {NewHandler,_}->
      check_handler_module(NewHandler)
  end.

update_behaviour(Object)->
  case { ?OID(Object), get_parent(Object) } of
    { Same, Same }->
      % Protection from loops
      ok;
    { _, ParentID }->
      ParentHandlers=get_behaviours(ParentID),
      update_behaviour(Object,ParentHandlers),
      ok
  end.


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
  inherit_fields(?OID(Object),ParentFields),
  ok.


%%=================================================================
%%	Internal helpers
%%=================================================================
inherit_fields(PatternID,ParentFields)->
  FieldFields = maps:keys( get_fields( {?PATTERN_PATTERN,?FIELD_PATTERN} )),
  ChildFields = get_fields(PatternID),

  [case ChildFields of
     #{ Name := Child}->
       % CASE 1. The field is already defined in the child, inherit algorithm
       Child1 = ecomet_field:inherit(Child,Parent),
       Child2 = ecomet_field:from_schema(Child1),
       % Edit object
       {ok, FieldID} =ecomet_folder:find_object(PatternID, Name),
       Field = ecomet:open(FieldID,write),
       ok = ecomet:edit_object(Field,Child2);
     _->
       % CASE 2. The field is not defined in the child yet, create a new one
      ParentSchemaFields = ecomet_field:from_schema(Parent),
      ParentOtherFields = parent_field_fields( PatternID, Name, FieldFields -- maps:keys( ParentSchemaFields ) ),
      ParentInheritFields = maps:merge( ParentOtherFields, ParentSchemaFields ),
      ecomet:create_object(ParentInheritFields#{
        <<".name">>=>Name,
        <<".folder">>=>PatternID,
        <<".pattern">>=>{?PATTERN_PATTERN,?FIELD_PATTERN},
        <<"is_parent">> => true
      })
   end || {Name, Parent} <- ordsets:from_list(maps:to_list(ParentFields)) ],
  ok.

update_storage_types(Map)->
  Types=
    maps:fold(fun(Name,_Config, Acc)->
      {ok,Storage}=ecomet_field:get_storage(Map,Name),
      Acc#{Storage=>true}
    end,#{}, get_fields(Map) ),
  maps:keys(Types).

parent_field_fields( PatternID, Name, Fields )->
  try
    ParentID = get_parent(PatternID),
    {ok, FieldID} =ecomet_folder:find_object(ParentID, Name),
    Field = ecomet:open(FieldID,write),
    ecomet:read_fields(Field, Fields )
  catch
    _:_->#{}
  end.











