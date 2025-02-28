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
-module(ecomet_object).

-include("ecomet.hrl").
-include("ecomet_schema.hrl").
-include("ecomet_subscription.hrl").

-callback on_create(Object::tuple())->term().
-callback on_edit(Object::tuple())->term().
-callback on_delete(Object::tuple())->term().

%%=================================================================
%%	Service API
%%=================================================================
-export([
  commit/1,
  get_db_id/1,
  get_db_name/1,
  get_node_id/1,
  get_pattern/1,
  get_pattern_oid/1,
  get_id/1,
  get_service_id/2,
  get_storage_types/1
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  create/1,create/2,
  delete/1,
  open/1,open/2,
  exists/1,
  construct/1,
  edit/2,edit/3,
  copy/2, copy/3,
  read_field/2,read_field/3,read_fields/2,read_fields/3,
  read_all/1,read_all/2,
  field_changes/2,
  object_changes/1,
  field_type/2,
  is_object/1,
  is_oid/1,
  get_oid/1,
  check_rights/1,
  get_behaviours/1,
  rebuild_index/1,

  debug/3
]).
%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([
  new_id/2,
  check_storage_type/1,
  check_path/1,
  check_db/1
]).
-endif.
%%===========================================================================
%% Behaviour API
%%===========================================================================
-export([
  on_create/1,
  on_edit/1,
  on_delete/1
]).

% @edoc handler of ecomet object
-record(object, {oid, edit, move, pattern, db}).

-type object_handler() :: #object{}.
-export_type([object_handler/0]).

-define(NODE_ID_LENGTH,16).
-define(DB_ID_LENGTH,8).
-define(PATTERN_IDL_LENGTH,16).

-define(ObjectID(PatternID,ObjectID),{PatternID,ObjectID}).
-define(map(P),
  if
    is_map(P) -> P;
    true -> ecomet_pattern:get_map(P)
 end).

-define(TRANSACTION(Fun),
  case ecomet_transaction:get_type() of
    none ->
      case ecomet_transaction:internal(Fun) of
        {ok,_@Result}->_@Result;
        {abort,_@Reason}->throw(_@Reason)
      end;
    _-> Fun()
  end ).

-define(SERVICE_FIELDS,#{
  <<".oid">>=>fun ecomet_lib:to_oid/1,
  <<".path">>=>fun ecomet_lib:to_path/1,
  <<".object">>=>fun ecomet_lib:dummy/1
}).

%% This is the result of regex compilation from
%% "^(?![ ])[^\\x00-\\x1F\\x2F\\x7F]*(?<![ ])$"
%% x00-x1F: ASCII Table Codes from 0 to 31
%% x2F: Slash Character Code
%% x7F: Delete Character Code
-define(BAD_CHARS_PATTERN, {re_pattern,0,0,0,
  <<69,82,67,80,126,0,0,0,48,0,0,0,1,128,0,0,255,255,255,255,255,
    255,255,255,0,0,0,0,1,0,0,0,0,0,64,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,131,0,58,27,126,0,5,29,32,120,0,5,
    111,0,0,0,0,255,127,255,255,255,255,255,255,255,255,255,127,255,
    255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,98,
    128,0,8,124,0,1,29,32,120,0,8,25,120,0,58,0>>}).

%%=================================================================
%%	Data API
%%=================================================================
%%============================================================================
%% Main functions
%%============================================================================
% Create new object
create(Fields)->
  create(Fields,#{}).
create(Fields,Params) when is_list(Params)->
  create(Fields,maps:from_list(Params));
create(#{<<".pattern">>:=Pattern} = Fields,#{format:=Format}=Params)->
  PatternID = Format(link,Pattern),
  Map=ecomet_pattern:get_map(PatternID),
  ParsedFields = parse_fields(Format,Map,maps:without([<<".pattern">>,<<".oid">>], Fields)),
  Params1 = maps:remove(format,Params),
  Fields1 = ParsedFields#{<<".pattern">>=>PatternID},
  create(Fields1,Params1);
create(#{ <<".pattern">>:=PatternID, <<".folder">>:=FolderID } = Fields, _Params)->

  % Get folder rights
  Folder = construct(FolderID),
  % Check rights
  #{
    <<".contentreadgroups">>:=Read,
    <<".contentwritegroups">>:=Write
  } = read_fields(Folder,#{
    <<".contentreadgroups">>=>none,
    <<".contentwritegroups">>=>none
  }),
  case check_rights(Read,Write,none) of
    {write,_} ->
      % Get the schema of the object type
      Map=ecomet_pattern:get_map(PatternID),
      % Inherit rights
      Fields1= maps:merge(#{
        <<".readgroups">>=>Read,
        <<".writegroups">>=>Write
      },Fields),
      % Parse fields
      Fields2=ecomet_field:build_new(Map,Fields1),

      % Generate new ID for the object
      OID=new_id(FolderID,PatternID),
      Object=#object{ oid=OID, edit=true, move=true, pattern= ecomet_pattern:get_map(PatternID), db=get_db_name(OID) },

      % Wrap the operation into a transaction
      ?TRANSACTION(fun()->
        prepare_create(Fields2 ,Object),
        save(Object, on_create)
      end),

      Object#object{ pattern = PatternID };
    _->?ERROR(access_denied)
  end.

% Delete an object
delete(#object{edit=false})->?ERROR(access_denied);
delete(#object{move=false})->?ERROR(access_denied);
delete(#object{oid=OID, pattern = P}=Object) when is_map(P)->

  % Check if the object is under on_create or on_edit procedure at the moment
  case ecomet_transaction:dict_get({OID,handler},none) of
    none->
      % Queue the procedure.
      ?TRANSACTION( fun()->
        prepare_delete(Object),
        save(Object, on_delete),
        ok
      end );
    _->
      % Object can not be deleted? if it is under behaviour handlers
      ?ERROR(behaviours_run)
  end;
delete(#object{pattern = P}=Object)->
  delete(Object#object{ pattern = ecomet_pattern:get_map(P) }).

% Open object
open(OID)->open(OID, _Lock = none).
open(OID, Lock)->
  IsTransaction = ecomet_transaction:get_type() =/= none,
  open(OID, Lock, IsTransaction).
open(OID, Lock, _IsTransaction = false)->
  if
    Lock =/= none-> throw(no_transaction);
    true -> ok
  end,
  Object=construct(OID),
  case check_rights(Object) of
    {read,CanMove}->Object#object{edit = false, move = CanMove};
    {write,CanMove}->Object#object{edit = true, move = CanMove };
    none->throw(access_denied);
    not_exists->throw(not_exists)
  end;
open(OID,Lock,_IsTransaction = true)->

  % Check if the object is under delete
  case ecomet_transaction:dict_get( {OID, handler}, undefined ) of
    on_delete -> throw( not_exists );
    _ -> ok
  end,

  Object = construct(OID),

  if
    Lock =/= none ->
      case get_lock(Lock, Object) of
        none->
          case ecomet_transaction:dict_get({OID, data}, none ) of
            Data when is_map( Data )-> ok;
            _-> throw(not_exists)
          end;
        _->
          todo
      end;
    true->
      ignore
  end,

  case check_rights(Object) of
    {read,CanMove}->Object#object{edit = false, move = CanMove};
    {write,CanMove}->Object#object{edit = true, move = CanMove };
    none->throw(access_denied);
    not_exists->throw(not_exists)
  end.

exists(OID)->
  case try open(OID,none) catch _:_-> error end of
    error-> false;
    _-> true
  end.

read_field(#object{oid = OID} = Object, Field)->
  case ?SERVICE_FIELDS of
    #{Field:=Fun}->{ok,Fun(Object)};
    _->
      case ecomet_transaction:get_type() of
        none ->
          lookup_field(Object, Field );
        _->
          case ecomet_transaction:dict_get( {OID, data}, none ) of
            #{ Field := { Value, _ } } ->
              { ok, Value };
            _->
              lookup_field( Object, Field )
          end
      end
  end.

lookup_field(#object{ oid = OID, pattern = P }, Field )->
  Schema = ?map(P),
  case Schema of
    #{ Field := #{ storage := S } }->
      case load_storage( OID, S ) of
        #{ fields := #{ Field := Value } } ->
          { ok, Value };
        _->
          {ok, none }
      end;
    _->
      {error, {undefined_field, Field}}
  end.

read_field(Object,Field,Params) when is_list(Params)->
  read_field(Object,Field,maps:from_list(Params));
read_field(Object,Field,Params) when is_map(Params)->
  case read_field(Object,Field) of
    {ok,Value}->
      case {Params,Value} of
        { #{default:=Default}, none}->{ok,Default};
        { #{format:=Format}, _ }->
          {ok,Type}=ecomet_field:get_type(?map(Object#object.pattern),Field),
          {ok,Format(Type,Value)};
        _->
          {ok,Value}
      end;
    Other->Other
  end.

read_fields( #object{ oid = OID, pattern = P } = Object, Fields) when is_list( Fields )->

  IsTransaction = ecomet:is_transaction(),

  {Changes, Storages0} =
    if
      IsTransaction ->
        {
          ecomet_transaction:dict_get( {OID, data}, #{} ),
          ecomet_transaction:dict_get( {OID, storages}, #{} )
        };
      true -> { #{}, #{} }
    end,

  Schema = ?map(P),

  {Result, Storages} =
    lists:foldl(fun(F, {FAcc, SAcc})->
      case ?SERVICE_FIELDS of
        #{F:=Fun}->
          % The field is a virtual field
          { FAcc#{ F => Fun(Object)}, SAcc };
        _->
          case Changes of
            #{ F:= { V, _ } }->
              % The field is already under transform
              { FAcc#{ F => V }, SAcc };
            _->
              % Read the field from storage
              case Schema of
                #{ F := #{ storage := S } } ->
                  case SAcc of
                    #{ S := SData }->
                      % The storage is already loaded
                      case SData of
                        #{ fields := #{ F := V }} -> { FAcc#{ F => V }, SAcc };
                        _-> { FAcc#{ F => none }, SAcc }
                      end;
                    _->
                      % Load the storage
                      SData = load_storage( false, OID, S  ),
                      case SData of
                        #{ fields := #{ F := V }} -> { FAcc#{ F => V }, SAcc#{ S => SData } };
                        _-> { FAcc#{ F => none }, SAcc#{ S => SData } }
                      end
                  end;
                _ ->
                  % The field is not in the schema
                  { FAcc#{ F => undefined_field }, SAcc }
              end
          end
      end
    end, {#{}, Storages0}, Fields ),

  if
    IsTransaction ->
      case ecomet_transaction:dict_get( {OID, data}, none ) of
        none ->
          ignore;
        _->
          % The object is under transform, update it's storages
          ecomet_transaction:dict_put(#{ {OID, storages} => Storages } )
      end;
    true ->
      ignore
  end,

  Result;

read_fields(Object, Fields) when is_map( Fields )->

  Result = read_fields( Object, maps:keys( Fields ) ),

  % Set default values for none fields
  maps:map(fun(F, V) ->
    if
      V =:= none -> maps:get(F, Fields);
      true -> V
    end
  end, Result ).

read_fields(Object,Fields,Params) when is_list(Params)->
  read_fields(Object,Fields,maps:from_list(Params));
read_fields(#object{ pattern = P} = Object, Fields, Params) when is_list(Fields)->

  Result = read_fields( Object, Fields ),

  case Params of
    #{format:=Formatter}->
      format_read_result( Formatter, ?map(P), Result );
    _->
      Result
  end;

read_fields(#object{pattern = P}=Object, Fields, Params) when is_map(Fields)->

  Result0 = read_fields( Object, maps:keys( Fields ) ),

  % Set default values for none fields
  Result =
    maps:map(fun(F, V) ->
      if
        V =:= none -> maps:get(F, Fields);
        true -> V
      end
    end, Result0 ),

  case Params of
    #{format:=Formatter}->
      format_read_result( Formatter, ?map(P), Result );
    _->
      Result
  end.

format_read_result( Formatter, Schema, Fields )->
  maps:map(fun(F,V) ->
    case Schema of
      #{ F := #{ type:= list, subtype := Type } }->
        Formatter({list,Type},V);
      #{ F := #{ type:= Type } }->
        Formatter(Type, V);
      _ when V =:= undefined_field->
        Formatter( string, V );
      _->
        V % system fields
    end
  end, Fields ).

% Read all object fields
read_all(Object)->
  read_all(Object,#{}).
read_all(Object,Params) when is_list(Params)->
  read_all(Object,maps:from_list(Params));
read_all(#object{pattern = P,oid = OID}=Object,Params) when is_map(Params)->
  Map = ?map(P),
  Fields=maps:map(fun(_,_)->none end,ecomet_pattern:get_fields(Map)),
  Fields1=Fields#{
    <<".oid">>=>OID
  },
  read_fields(Object,Fields1,Params).

% Edit object
edit(Object,Fields)->
  edit(Object,Fields,#{}).
edit(#object{edit=false},_Fields,_Params)->
  throw(access_denied);
edit(Object,Fields,Params) when is_list(Params)->
  edit(Object,Fields,maps:from_list(Params));
edit(#object{pattern =P}=Object,Fields,Params) when not is_map(P)->
  edit(Object#object{pattern = ?map(P)}, Fields, Params);
edit(#object{pattern =Map}=Object,Fields,#{format:=Format}=Params)->
  ParsedFields= parse_fields(Format,Map,Fields),
  Params1 = maps:remove(format,Params),
  edit(Object,ParsedFields,Params1);
edit(#object{oid = OID, pattern = Map}=Object,InFields,_Params)->

  Fields=ecomet_field:merge(Map,#{},InFields),
  % Check user rights for moving object
  check_move( Object, Fields ),

  case ecomet_transaction:dict_get({OID,handler},none) of
    none ->
      ?TRANSACTION(fun()->
        Updates = prepare_edit(Fields, Object),
        if
          map_size( Updates ) > 0 ->
            save(Object, on_edit);
          true ->
            % No real updates, ignore
            ignore
        end
      end);
    _->
      % If object is under behaviour handlers, just save changes
      prepare_edit(Fields, Object)
  end,
  ok.

copy(Object, Replace)->
  Original = read_all(Object),
  New = maps:merge(Original, Replace),
  create(New).
copy(Object, Replace, Params)->
  Original = read_all(Object),
  New = maps:merge(Original, Replace),
  create(New, Params).

parse_fields(Formatter,Map,Fields)->
  maps:map(fun(Name,Value)->
    {ok,Type}=ecomet_field:get_type(Map,Name),
    Formatter(Type,Value)
  end,Fields).


% Check changes for the field within the transaction
field_changes(#object{oid=OID},Field)->
  case ecomet_transaction:dict_get( {OID, data} ) of
    #{ Field := Changes } -> Changes;
    _-> none
  end.

% Check changes for the field within the transaction
object_changes(#object{ oid = OID })->
  ecomet_transaction:dict_get( {OID, data} ).

field_type(#object{pattern = P},Field)->
  case ?SERVICE_FIELDS of
    #{Field:=_}-> {ok, string};
    _->ecomet_field:get_type(?map(P),Field)
  end.

is_object(#object{})->
  true;
is_object(_Other)->
  false.

is_oid(?ObjectID(P,I)) when is_integer(P),is_integer(I)->
  true;
is_oid(_Invalid)->
  false.

%%Return object oid
get_oid(#object{oid=OID})->OID.

%% Check context user rights for the object
check_rights(#object{}=Object)->
  case read_fields(Object,[<<".readgroups">>,<<".writegroups">>,<<".folder">>]) of
    #{ <<".folder">>:=none } ->
      % The object can not exist without a folder
      not_exists;
    #{
      <<".readgroups">>:=Read,
      <<".writegroups">>:=Write,
      <<".folder">>:=FolderID
    } ->
      {ok, Move} = read_field( construct(FolderID), <<".contentwritegroups">> ),
      check_rights(Read,Write,Move)
  end;
check_rights(OID)->
  check_rights(construct(OID)).

check_rights(Read,Write,Move)->
  case ecomet_user:is_admin() of
    {error,Error}->?ERROR(Error);
    {ok,true}->{write,true};
    {ok,false}->

      {ok,UserGroups}=ecomet_user:get_usergroups(),

      MoveGroups=if is_list(Move)->Move; true->[] end,
      CanMove =
        case ordsets:intersection(UserGroups,MoveGroups) of
          []->false;
          _->true
        end,

      WriteGroups=if is_list(Write)->Write; true->[] end,
      case ordsets:intersection(UserGroups,WriteGroups) of
        []->
          ReadGroups=if is_list(Read)->Read; true->[] end,
          case ordsets:intersection(UserGroups,ReadGroups) of
            []->none;
            _->{ read, CanMove}
          end;
        _->{ write, CanMove}
      end
  end.

check_move( #object{move=CanMove}=Object, EditFields )->
  % check folder
  case EditFields of
    #{<<".folder">>:=NewFolder}->
      case read_field( Object, <<".folder">> ) of
        {ok, NewFolder}-> ok;
        _ when CanMove->
          % Check for recursion
          Path = ?PATH(Object),
          {ok, Name} = read_field(Object, <<".name">>),
          NewPath = <<(?PATH(NewFolder))/binary,"/",Name/binary>>,
          S = size(Path),
          case NewPath of
            <<Path:S/binary, $/,_/binary>> -> throw(infinite_recursion);
            _ -> ok
          end,
          % Check rights
          #{
            <<".contentreadgroups">>:=Read,
            <<".contentwritegroups">>:=Write
          } = read_fields(construct(NewFolder),#{
            <<".contentreadgroups">>=>none,
            <<".contentwritegroups">>=>none
          }),
          case check_rights( Read, Write, none ) of
            {write,_}-> ok;
            _-> ?ERROR(access_denied)
          end;
        _->
          ?ERROR(access_denied)
      end;
    _->
      ok
  end,

  % check name
  case EditFields of
    #{<<".name">>:=NewName}->
      case read_field( Object, <<".name">> ) of
        {ok, NewName}-> ok;
        _ when CanMove->
          ok;
        _->
          ?ERROR(access_denied)
      end;
    _->
      ok
  end,

  ok.

get_behaviours(#object{pattern = P})->
  ecomet_pattern:get_behaviours(?map(P)).

%%=================================================================
%%	Service API
%%=================================================================
load_storage(OID,Type)->
  IsTransaction = ecomet_transaction:get_type() =/= none,
  load_storage( IsTransaction, OID, Type ).
load_storage(_IsTransaction = false, OID, Type)->
  DB = get_db_name( OID ),
  case ecomet_db:read(DB, ?DATA, Type, OID ) of
    SData when is_map( SData ) -> SData;
    _ -> #{}
  end;
load_storage(_IsTransaction = true, OID, Type)->
  case ecomet_transaction:dict_get( {OID, handler}, none ) of
    on_create ->
      % The object is under create, no storages
      #{};
    _->
      case ecomet_transaction:dict_get( {OID, storages}, none ) of
        #{ Type := SData} when is_map( SData ) -> SData;
        _->
          load_storage( false, OID, Type )
      end
  end.

%%=====================================================================
%% Behaviour handlers
%%=====================================================================
on_create(Object)->
  check_name(Object),
  check_storage_type(Object),
  check_path(Object),
  edit_rights(Object),
  edit(Object,#{
    <<".ts">>=>ecomet_lib:log_ts()
  }).

on_edit(Object)->
  % Check domain change
  check_db(Object),
  % Check for unique name in folder
  check_path(Object),
  case field_changes(Object,<<".pattern">>) of
    none->ok;
    {_,none}->ok;
    _->?ERROR(can_not_change_pattern)
  end,
  case field_changes(Object,<<".ts">>) of
    none->ok;
    {_,none}->ok;
    _->?ERROR(can_not_change_ts)
  end,
  case field_changes(Object,<<".name">>) of
    none->ok;
    {New,none}->
      check_name(New),
      ok;
    {New, Old}->
      check_name(New),
      case is_system(Old) of
        true->?ERROR(system_object);
        _->ok
      end
  end,
  edit_rights(Object).

on_delete(Object)->
  case is_system(Object) of
    true->
      % We are not allowed to delete system objects until it is not the
      % delete of the containing folder
      {ok, FolderID} = ecomet:read_field(Object, <<".folder">>),
      try
        open(FolderID,none),
        ?ERROR(system_object)
      catch
          _:not_exists ->ok
      end;
    _->
      ok
  end,
  ok.

% Checking for whitespaces (leading / trailing)
% and bad characters in the object name
check_name(<<>>) ->
  ?ERROR(<<"Name can not be empty">>);
check_name(BinaryString) when is_binary(BinaryString) ->
  case re:run(BinaryString, ?BAD_CHARS_PATTERN) of
    {match, _} -> ok;
    _ -> throw({name_has_whitespaces_or_bad_characters,BinaryString})
  end;
check_name(Object) ->
  {ok, BinaryString} = ecomet:read_field(Object, <<".name">>),
  check_name(BinaryString).

check_storage_type(Object)->
  {ok,FolderID}=ecomet:read_field(Object,<<".folder">>),
  PatternID = get_pattern_oid(FolderID),
  case ecomet_pattern:get_storage(PatternID) of
    ?RAM->
      % Ram only folders cannot contain persistent objects
      ObjectPatternID=get_pattern_oid(Object),
      case ecomet_pattern:get_storage(ObjectPatternID) of
        ?RAM->ok;
        _->?ERROR(ram_only_folder)
      end;
    _->
      % Persistent folders can contain any types of objects
      ok
  end.

% Path is always unique
check_path(Object)->
  Changed=
    case field_changes(Object,<<".name">>) of
      none->
        case field_changes(Object,<<".folder">>) of
          none->false;
          _->true
        end;
      _->true
    end,
  if
    Changed->
      {ok,FolderID}=read_field(Object,<<".folder">>),
      {ok,Name}=read_field(Object,<<".name">>),
      OID = get_oid(Object),
      % Check that name does not include the path delimiter
      case ecomet_folder:find_object_system(FolderID,Name) of
        {error,not_found}->ok;
        {ok,OID}->ok;
        _->?ERROR({not_unique,Name})
      end;
    true->ok
  end.

% Object can not change its folder if it is in another database
check_db(#object{oid=OID}=Object)->
  case field_changes(Object,<<".folder">>) of
    none->ok;
    {FolderID,_}->
      case {get_db_id(OID),ecomet_folder:get_db_id(FolderID)} of
        {Same,Same}->ok;
        _->
          ?ERROR(different_database)
      end
  end.

% ReadGroups must contain all the WriteGroups
edit_rights(Object)->
  IsChanged=
    case field_changes(Object,<<".writegroups">>) of
      none->
        case field_changes(Object,<<".readgroups">>) of
          none->false;
          _->true
        end;
      _->true
    end,
  if
    IsChanged ->
      #{
        <<".readgroups">>:=Read,
        <<".writegroups">>:=Write
      } = read_fields(Object,#{
        <<".readgroups">> => [],
        <<".writegroups">> => []
      }),
      edit(Object,#{
        <<".readgroups">> => ordsets:from_list(Read++Write),
        <<".writegroups">> => ordsets:from_list(Write)
      });
    true -> ok
  end.

is_system(<<".",_/binary>> = _Name)->
  true;
is_system(Name) when is_binary(Name)->
  false;
is_system(Object)->
  {ok,Name}=ecomet:read_field(Object,<<".name">>),
  is_system(Name).

%%============================================================================
%%	Internal helpers
%%============================================================================
new_id(_FolderID,?ObjectID(_,?PATTERN_PATTERN))->
  % Patterns have system-wide unique Id via schema locking,
  % IMPORTANT! Patterns are allowed to be created only in the root database
  % which has 0 id, therefore the ServiceID of the pattern
  % always equals its PatternID
  Id = ecomet_schema:new_pattern_id(),
  {?PATTERN_PATTERN,Id};
new_id(FolderID,?ObjectID(_,PatternID))->

  ID= ecomet_id:new({pattern,PatternID}),

  DB = ecomet_folder:get_db_id( FolderID ),

  ServiceID = get_service_id( DB, { ?PATTERN_PATTERN ,PatternID} ),

  { ServiceID, ID }.

get_service_id( DB, ?ObjectID(_,PatternID) )->

  PatternIDH = PatternID bsr ?PATTERN_IDL_LENGTH,
  PatternIDL = PatternID rem (1 bsl ?PATTERN_IDL_LENGTH),

  % The ID is unique only node-wide. To make the OID unique system-wide
  % we add the system-wide unique nodeId (current node) to the ServiceID
  % of the object.
  % Additionally, to be able to define the database the object is stored in
  % we add the database id to the serviceId too
  NodeID = ecomet_node:get_unique_id(),

  ((((PatternIDH bsl ?NODE_ID_LENGTH) + NodeID) bsl ?DB_ID_LENGTH + DB) bsl ?PATTERN_IDL_LENGTH) + PatternIDL.

get_db_id(?ObjectID(ServiceID,_))->
  IDH=ServiceID bsr ?PATTERN_IDL_LENGTH,
  IDH rem (1 bsl ?DB_ID_LENGTH).

get_db_name(OID)->
  ID = get_db_id(OID),
  ecomet_schema:get_db_name(ID).

get_node_id(?ObjectID(ServiceID,_))->
  IDH=ServiceID bsr (?DB_ID_LENGTH + ?PATTERN_IDL_LENGTH),
  IDH rem (1 bsl ?NODE_ID_LENGTH).

get_pattern(?ObjectID(PatternID,_))->
  PatternID.

get_id(?ObjectID(_,ObjectID))->
  ObjectID.

%%Return oid of pattern of the object
get_pattern_oid(#object{oid=OID})->
  get_pattern_oid(OID);
get_pattern_oid(?ObjectID(ServiceID,_))->
  get_pattern_oid(ServiceID);
get_pattern_oid(ServiceID)->
  IDH = ServiceID div (1 bsl (?NODE_ID_LENGTH + ?DB_ID_LENGTH + ?PATTERN_IDL_LENGTH)),
  IDL = ServiceID rem (1 bsl ?PATTERN_IDL_LENGTH),
  PatternID = IDH * (1 bsl ?PATTERN_IDL_LENGTH) + IDL,
  ?ObjectID(?PATTERN_PATTERN,PatternID).

get_storage_types(#object{ pattern = P })->
  ecomet_pattern:get_storage_types( ?map(P) ).

% Acquire lock on object
get_lock(Lock = none,#object{oid=OID,pattern = P})->
  % Define the key
  DB = get_db_name(OID),
  Type=ecomet_pattern:get_storage(?map(P)),
  case ecomet_transaction:read(DB, ?DATA, Type, OID, Lock ) of
    not_found-> none;
    Storage->Storage
  end;
get_lock(Lock,#object{oid=OID,pattern = P})->
  % Define the key
  DB = get_db_name(OID),
  Type=ecomet_pattern:get_storage(?map(P)),
  case ecomet_transaction:read(DB, ?DATA, Type, OID, Lock ) of
    not_found-> none;
    Storage-> Storage
  end.

% Fast object open, only for system dirty read
construct(OID)->
  PatternID=get_pattern_oid(OID),
  DB = get_db_name(OID),
  #object{oid=OID,edit=false,move=false,pattern = PatternID, db=DB}.

prepare_create(Fields, #object{oid = OID})->
  ecomet_transaction:dict_put( #{
    {OID, data} => maps:map(fun(_F, V)->{ V, none } end, Fields) ,
    {OID, storages} => #{}
  }),
  ok.

prepare_edit(Fields, #object{oid = OID, pattern = P})->

  Schema = ?map(P),

  Storages0 = ecomet_transaction:dict_get({OID, storages}, #{}),
  Storages =
    maps:fold(fun(F, _V, Acc)->
      #{ storage := S } = maps:get( F, Schema ),
      case maps:is_key(S, Acc) of
        true -> Acc;
        _ -> Acc#{ S => load_storage( false, OID, S ) }
      end
    end, Storages0, Fields),

  NewChanges =
    maps:fold(fun(F, V, Acc)->
      #{ storage := S } = maps:get( F, Schema ),
      SData = maps:get( S, Storages ),
      case SData of
        #{ fields := #{ F := V0 } } when V0 =:= V ->
          Acc;
        #{ fields := #{ F := V0 } }->
          Acc#{ F => { V, V0 } };
        _ when V =/= none->
          Acc#{ F => { V, none } };
        _->
          Acc
      end
    end, #{}, Fields ),


  if
    map_size(NewChanges) > 0 ->

      Changes0 = ecomet_transaction:dict_get({OID, data}, #{}),
      OtherChanges = maps:without( maps:keys(Fields), Changes0 ),
      Changes = maps:merge( OtherChanges, NewChanges ),

      Dict0 = #{ {OID, data} => Changes },
      Dict =
        if
          map_size( Storages ) > map_size( Storages0 )->
            Dict0#{ {OID, storages} => Storages };
          true ->
            Dict0
        end,
      ecomet_transaction:dict_put( Dict );
    true ->
      ignore
  end,

  NewChanges.

prepare_delete(#object{oid = OID, pattern = P})->

  Fields = maps:keys( ecomet_pattern:get_fields( ?map( P ) ) ),
  Changes = maps:from_list([{ F, none } || F <- Fields]),

  ecomet_transaction:dict_put( #{
    {OID, data} => Changes,
    {OID, storages} => #{}
  }).


% Save routine. This routine runs when we edit object, that is not under behaviour handlers yet
save(#object{oid=OID,pattern = P}=Object, Handler)->

  ecomet_transaction:dict_put( #{ {OID, handler} => Handler }),

  % Run behaviours
  [ case erlang:function_exported(Behaviour,Handler,1) of
      true ->
        Behaviour:Handler(Object);
      _ ->
        ?LOGWARNING("invalid behaviour ~p:~p",[Behaviour, Handler])
  end || Behaviour <- ecomet_pattern:get_behaviours(?map(P)) ],

  % Confirm the commit
  ecomet_transaction:log( OID, {Object, Handler} ),

  % The result
  ok.

% Save object changes to the storage
commit([ {#object{oid = OID} = Object, Handler}|Rest])->
  Changes = ecomet_transaction:dict_get( {OID, data} ),
  if
    map_size( Changes ) > 0 ->
      [ commit_object(Handler, Object, Changes ) | commit( Rest ) ];
    true->
      commit( Rest )
  end;
commit([])->
  [].

changes_by_storage( Changes, Schema )->
  maps:fold(fun(F, {V, _}, {SAcc, IAcc} )->

    #{storage := S, index := I} = maps:get( F, Schema ),

    SAcc0 = maps:get( S, SAcc, #{} ),
    SAcc1 = SAcc#{ S => SAcc0#{ F => V } },

    if
      I =:= none; I=:=[] ->
        { SAcc1,  IAcc };
      true ->
        {SAcc1, IAcc#{ F => {V,I,S} }}
    end

  end, {#{}, #{}}, Changes ).

merge_fields( Fields0, Fields )->
  Fields1 = maps:merge( Fields0, Fields ),
  maps:filter(fun(_F,V) -> V =/= none end, Fields1 ).

load_storage_types( OID, Types, Preloaded )->
  lists:foldl(fun( S, Acc )->
    case maps:is_key( S, Acc ) of
      true -> Acc;
      false -> Acc#{ S => load_storage( false, OID, S ) }
    end
  end, Preloaded, Types ).

get_storage_indexes( Storages )->
  maps:fold(fun(S, Data, Acc ) ->
    case Data of
      #{ index := SIndex }->  Acc#{ S => SIndex };
      _-> Acc
    end
  end, #{}, Storages ).

-record(s_data,{ oid, db, type, fields, data ,index, tags }).
%-------------------------Create commit---------------------------------------------
commit_object(on_create, #object{oid = OID, pattern = P, db = DB} = Object, Changes )->

  {ByStorageTypes, IndexedFields} = changes_by_storage( Changes, ?map( P ) ),

  {Index, Tags} = ecomet_index:build_index(OID, IndexedFields, #{}),

  %-----Commit changes---------------------------
  AddTags =  maps:fold(fun(Type, Fields, Acc)->
    commit_storage_create(#s_data{
      oid = OID,
      db = DB,
      type =Type,
      fields = maps:filter(fun(_F, V)-> V=/= none end, Fields ),
      index = maps:get( Type, Index, none),
      tags = maps:get( Type, Tags, none)
    }, Acc )
  end, ?EMPTY_SET, ByStorageTypes),

  Fields = maps:map(fun(_F,{V,_}) -> V end, Changes),

  % Log timestamp. If it is an object creation then timestamp must be the same as
  % the timestamp of the object, otherwise it is taken as the current timestamp
  TS = maps:get( <<".ts">>, Fields ),

  #{
    action => create,
    oid => OID,
    fields => log_fields( Object, Fields ),
    db => DB,
    ts => TS,
    tags => AddTags
  };

%-------------------------Edit commit---------------------------------------------
commit_object(on_edit, #object{oid = OID, pattern = P}=Object, Changes )->

  Storages = ecomet_transaction:dict_get( {OID, storages}, #{} ),
  {ByStorageTypes, IndexedFields} = changes_by_storage( Changes, ?map( P ) ),

  if
    map_size( IndexedFields ) =:= 0 ->
      commit_update_light( Object, Changes, ByStorageTypes, Storages );
    true ->
      commit_update_full( Object, Changes, ByStorageTypes, IndexedFields , Storages )
  end;

%-------------------------Delete commit---------------------------------------------
commit_object(on_delete, #object{oid = OID, pattern = P, db = DB}, _Changes )->

  Storages0 = ecomet_transaction:dict_get( {OID, storages}, #{} ),

  % Load storages that are not loaded
  Storages = load_storage_types( OID, ecomet_pattern:get_storage_types( P ), Storages0 ),

  % Get full map of indexes
  Index = get_storage_indexes( Storages ),

  %------------Commit--------------------------------
  maps:foreach(fun( S, _ )->
    ok = ecomet_transaction:delete(DB, ?DATA, S, OID,  none)
  end, Storages),

  ecomet_index:destroy_index( OID, Index ),

  % This is to trigger only sticky subscriptions
  #{
    action => delete,
    oid => OID
  }.

%-------------------------Light Edit commit (no tags changed)---------------------------------------------
commit_update_light(#object{oid = OID, db = DB} = Object, Changes, ByStorageTypes, Storages )->

  %-----Commit changes---------------------------
  maps:foreach(fun(Type, Fields)->
    commit_storage_update(#s_data{
      oid = OID,
      db = DB,
      type =Type,
      fields = Fields,
      data = maps:get( Type, Storages )
    } )
  end, ByStorageTypes),

  Fields = maps:map(fun(_F,{V,_}) -> V end, Changes ),
  % This is to trigger only sticky subscriptions
  #{
    action => light_update,
    oid => OID,
    fields => log_fields(Object, Fields)
  }.
%-------------------------Full Edit commit (tags changed)---------------------------------------------
% The heaviest version of commit, because we need to build full object with it's tags to properly trigger query subscriptions
-record(full_update_acc,{ add_tags, other_tags, del_tags, fields0 }).
commit_update_full( #object{ oid = OID, pattern = P, db = DB } = Object, Changes, ByStorageTypes, IndexedFields , Storages0 )->

  % Load storages that are not loaded
  Storages = load_storage_types( OID, ecomet_pattern:get_storage_types( P ), Storages0 ),

  % Get full map of indexes
  Index0 = get_storage_indexes( Storages ),

  % Update the index
  { Index, Tags } = ecomet_index:build_index(OID, IndexedFields, Index0 ),

  %-----Commit changes---------------------------
  CommitAcc =  maps:fold(fun(Type, Fields, Acc)->
    commit_storage_update(#s_data{
      oid = OID,
      db = DB,
      type =Type,
      fields = Fields,
      data = maps:get( Type, Storages ),
      index = maps:get( Type, Index, none),
      tags = maps:get( Type, Tags, none)
    }, Acc )
  end, #full_update_acc{
    add_tags = ?EMPTY_SET,
    other_tags = ?EMPTY_SET,
    del_tags = ?EMPTY_SET,
    fields0 = #{}
  }, ByStorageTypes),

  #full_update_acc{
    add_tags = AddTags,
    other_tags = OtherTags,
    del_tags = DelTags,
    fields0 = Fields0
  } = CommitAcc,

  Fields = maps:map(fun(_F,{V,_}) -> V end, Changes),

  TS = ecomet_lib:log_ts(),

  #{
    action => update,
    oid => OID,
    fields => log_fields(Object, Fields),
    fields0 => log_fields(Object, Fields0),
    db => DB,
    ts => TS,
    tags => {AddTags, OtherTags, DelTags}
  }.

%=============================Create===================================
% Create new storage without index
commit_storage_create(#s_data{
  index = none,
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
}, Acc )->

  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields },  none),
  Acc;

% Create new storage with index
commit_storage_create(#s_data{
  index = Index,
  tags = Tags,
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
}, Acc )->

  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields, index => Index },  none),

  ?SET_OR( Acc, ?NEW_SET( Tags ) ).

%=============================Light update===================================
% Light update storage with index
commit_storage_update(#s_data{
  data = #{ index := Index, fields := Fields0 },
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
} )->

  case merge_fields( Fields0, Fields ) of
    NewFields when map_size( NewFields ) > 0->
      ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => NewFields, index => Index }, none);
    _->
      ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none)
  end;

% Light update storage without index
commit_storage_update(#s_data{
  data = #{ fields := Fields0 },
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
} )->

  case merge_fields( Fields0, Fields ) of
    NewFields when map_size( NewFields ) > 0->
      ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => NewFields }, none);
    _->
      ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none)
  end;

% Light create storage
commit_storage_update(#s_data{
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
})->
  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields }, none).

%=============================Full update===================================
% Update storage without index without tags
commit_storage_update(#s_data{
  data = #{ fields := Fields0 },
  index = none,
  tags = none,
  oid = OID,
  db = DB,
  type =Type,
  fields = Fields
}, #full_update_acc{
  fields0 = Fields0Acc
} = Acc)->

  case merge_fields( Fields0, Fields ) of
    NewFields when map_size( NewFields ) > 0->
      ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => NewFields }, none);
    _->
      ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none)
  end,

  Acc#full_update_acc{
    fields0 = maps:merge(Fields0Acc, Fields0 )
  };

% Update storage without index with deleted tags
commit_storage_update(#s_data{
  data = #{ fields := Fields0 },
  index = none,
  tags = { _AddTags, _OtherTags, DelTags },
  oid = OID,
  db = DB,
  type =Type,
  fields = Fields
}, #full_update_acc{
  del_tags = DelTagsAcc,
  fields0 = Fields0Acc
} = Acc)->

  case merge_fields( Fields0, Fields ) of
    NewFields when map_size( NewFields ) > 0->
      ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => NewFields }, none);
    _->
      ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none)
  end,

  DelTagsSet = ?NEW_SET( DelTags ),

  Acc#full_update_acc{
    del_tags = ?SET_OR(DelTagsAcc,  DelTagsSet),
    fields0 = maps:merge(Fields0Acc, Fields0 )
  };

% Update storage with index with tags
commit_storage_update(#s_data{
  data = #{ fields := Fields0 },
  index = Index,
  tags = { AddTags, OtherTags, DelTags },
  oid = OID,
  db = DB,
  type =Type,
  fields = Fields
}, #full_update_acc{
  add_tags = AddTagsAcc,
  other_tags = OtherTagsAcc,
  del_tags = DelTagsAcc,
  fields0 = Fields0Acc
} = Acc)->

  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => merge_fields( Fields0, Fields ), index => Index },  none),

  AddTagsSet = ?NEW_SET( AddTags ),
  OtherTagsSet = ?NEW_SET( OtherTags ),
  DelTagsSet = ?NEW_SET( DelTags ),

  Acc#full_update_acc{
    add_tags = ?SET_OR(AddTagsAcc,  AddTagsSet),
    other_tags = ?SET_OR(OtherTagsAcc,  OtherTagsSet),
    del_tags = ?SET_OR(DelTagsAcc,  DelTagsSet),
    fields0 = maps:merge(Fields0Acc, Fields0 )
  };

% Create storage without index
commit_storage_update(#s_data{
  index = none,
  oid = OID,
  db = DB,
  type =Type,
  fields = Fields
}, Acc)->

  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields },  none),
  Acc;

% Create storage with index
commit_storage_update(#s_data{
  index = Index,
  tags = { AddTags, _OtherTags, _DelTags },
  oid = OID,
  db = DB,
  type =Type,
  fields = Fields
}, #full_update_acc{
  add_tags = AddTagsAcc
} = Acc)->

  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields, index => Index },  none),

  Acc#full_update_acc{
    add_tags = ?SET_OR( AddTagsAcc, ?NEW_SET(AddTags) )
  }.

log_fields( #object{ oid = OID } = Object, Fields )->
  ecomet_query:query_object( Object#object{
    pattern = get_pattern_oid( OID ),
    edit = false,
    move = false
  }, Fields ).

%%==============================================================================================
%%	Reindexing
%%==============================================================================================
rebuild_index(OID)->

  Object = #object{
    pattern = P,
    db = DB
  } = ?OBJECT(OID),

  Schema = ?map(P),

  % Load storages that are not loaded
  Storages = load_storage_types( OID, ecomet_pattern:get_storage_types( Schema ), #{} ),
  FieldList = maps:keys( ecomet_pattern:get_fields( Schema )),
  AllFields = read_fields( Object, FieldList ),
  Changes = maps:map(fun(_F,V)->{V,none} end, AllFields),

  {ByStorageTypes, IndexedFields} = changes_by_storage( Changes, Schema ),

  % Get full map of indexes
  Index0 = get_storage_indexes( Storages ),

  ?TRANSACTION(fun()->
    % Update the index
    { Index, Tags } = ecomet_index:build_index(OID, IndexedFields, Index0 ),

    %-----Commit changes---------------------------
    maps:fold(fun(Type, Fields, Acc)->
      commit_storage_update(#s_data{
        oid = OID,
        db = DB,
        type =Type,
        fields = Fields,
        data = maps:get( Type, Storages ),
        index = maps:get( Type, Index, none),
        tags = maps:get( Type, Tags, none)
      }, Acc )
              end, #full_update_acc{
      add_tags = ?EMPTY_SET,
      other_tags = ?EMPTY_SET,
      del_tags = ?EMPTY_SET,
      fields0 = #{}
    }, ByStorageTypes)
  end),
  ok.

debug(Folder, Count, Batch)->
  ecomet:dirty_login(<<"system">>),
  P = ?OID(<<"/root/.patterns/test_pattern">>),
  F = ?OID(<<"/root/",Folder/binary>>),

%%  Fields = #{<<".folder">> => F, <<".pattern">> => P},
%%
%%  ?LOGINFO("DEBUG: start"),
%%  [ begin
%%      create(maps:merge(Fields,#{
%%        <<".name">> => integer_to_binary( I ),
%%        <<"f1">> => integer_to_binary(erlang:phash2({I}, 200000000)),
%%        <<"f2">> => integer_to_binary(erlang:phash2(I, 200000000))
%%      }))
%%    end ||I <- lists:seq(1, Count)],
%%
%%  ?LOGINFO("DEBUG: end"),
%%
%%  ok.

  %fill(#{<<".folder">> => F, <<".pattern">> => P}, 0, Batch, Count).
  spawn(fun()->
    try
    ecomet:dirty_login(<<"system">>),
    fill(#{<<".folder">> => F, <<".pattern">> => P}, 0, Batch, Count)
    catch
      _:E:S->?LOGERROR("DEBUG: error ~p: ~p",[E,S])
    end
  end).

fill(Fields,C, Batch, Stop) when C < Stop ->
  Res = ecomet:transaction(fun()->
    [ create(maps:merge(Fields,#{
      <<".name">> => integer_to_binary( I ),
      <<"f1">> => integer_to_binary(erlang:phash2({I}, 200000000)),
      <<"f2">> => integer_to_binary(erlang:phash2(I, 200000000))
    })) ||I <- lists:seq(C,C-1+Batch)],
    ok
  end),
  if (C rem 10000) =:= 0-> ?LOGINFO("DEBUG: write ~p res ~p",[C+Batch,Res]); true-> ignore end,
  %timer:sleep(10),
  fill(Fields,C+Batch,Batch,Stop);
fill(_F,_C,_B,_S)->
  ok.

% ecomet_object:debug(<<"F1">>, 1000000, 10).
% ecomet_object:debug(<<"F2">>, 1000000, 10).
% ecomet_object:debug(<<"F3">>, 1000000, 10).