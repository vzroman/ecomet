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

-callback on_create(Object::tuple())->term().
-callback on_edit(Object::tuple())->term().
-callback on_delete(Object::tuple())->term().

%%=================================================================
%%	Service API
%%=================================================================
-export([
  load_storage/2,
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
  rebuild_index/1, rebuild_index/2,

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
-define(BAD_CHARS_PATTERN, {re_pattern, 0, 0, 0,
  <<69, 82, 67, 80, 126, 0, 0, 0, 16, 0, 0, 0, 1, 128, 0, 0, 255,
    255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
    64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 131, 0, 58, 27, 126, 0, 5, 29, 32,
    120, 0, 5, 111, 0, 0, 0, 0, 255, 127, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 98, 128, 0, 8, 124, 0,
    1, 29, 32, 120, 0, 8, 25, 120, 0, 58, 0>>}).

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
  case get_lock(Lock, Object) of
    none-> throw(not_exists);
    _->
      case check_rights(Object) of
        {read,CanMove}->Object#object{edit = false, move = CanMove};
        {write,CanMove}->Object#object{edit = true, move = CanMove };
        none->throw(access_denied);
        not_exists->throw(not_exists)
      end
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
          lookup_field( Object, Field );
        _->
          case ecomet_transaction:dict_get( {OID, data} ) of
            #{ Field := { Value, _ } } ->
              { ok, Value };
            _->
              lookup_field( Object, Field )
          end
      end
  end.

lookup_field(#object{ oid = OID, pattern = P }, Field )->
  Schema = ecomet:pattern_fields( ?map(P) ),
  case Schema of
    #{ Field := #{ storage := S } }->
      case load_storage( OID, S ) of
        SFields when is_map( SFields )->
          {ok, maps:get( Field, SFields, none )};
        _->
          {ok,none}
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

read_fields(Object,Fields)->
  read_fields(Object,Fields,#{}).
read_fields(Object,Fields,Params) when is_list(Fields)->
  FieldsMap = maps:from_list([ {Field,none} || Field <- Fields ]),
  read_fields(Object,FieldsMap,Params);
read_fields(Object,Fields,Params) when is_list(Params)->
  read_fields(Object,Fields,maps:from_list(Params));
read_fields(#object{oid = OID,pattern = P}=Object,Fields,Params) when is_map(Fields),is_map(Params)->

  IsTransaction = ecomet:is_transaction(),

  Changes =
    if
      IsTransaction -> ecomet_transaction:dict_get( {OID, data}, #{} );
      true -> #{}
    end,

  Schema = ecomet_pattern:get_fields( ?map(P) ),

  {ReadyFields, Rest} =
    maps:fold(fun(F,_V, {AccReady, AccRest})->
      case ?SERVICE_FIELDS of
        #{F:=Fun}->
          % The field is a virtual field
          { AccReady#{ F => Fun(Object)}, AccRest };
        _->
          case Changes of
            #{ F:= { V, _ } }->
              { AccReady#{ F => V } };
            _->
              case maps:is_key( F, Schema ) of
                true -> { AccReady, [F|AccRest] };
                false -> { AccReady#{ F => undefined_field }, AccRest }
              end
          end
      end
    end, {#{}, []}, Fields ),

  {AllFields, _} =
    lists:foldl(fun(F, {FAcc, SAcc})->
      #{ storage := S } = maps:get( F, Schema ),
      case SAcc of
        #{ S := SFields } -> { FAcc#{ F => maps:get( F, SFields, none ) }, SAcc };
        _ ->
          case load_storage(IsTransaction, OID, S ) of
            SFields when is_map( SFields ) ->
              { FAcc#{ S => maps:get( F, SFields, none )}, SAcc#{ S => SFields } };
            _->
              { FAcc#{ F => none}, SAcc#{ S => #{} } }
          end
      end
    end, {ReadyFields,#{}}, Rest),

  case Params of
    #{format:=Format}->
      maps:map(fun(F,V) ->
        case Schema of
          #{ F := #{ type:= list, subtype := Type } }->
            Format({list,Type},V);
          #{ F := #{ type:= Type } }->
            Format(Type, V);
          _->
            Format( string, V )
        end
      end, AllFields );
    _->
      AllFields
  end.

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
        prepare_edit(Fields, Object),
        save(Object, on_edit)
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
            <<Path:S/binary,_/binary>> -> throw(infinite_recursion);
            _->ok
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
    #{ fields := Fields } -> Fields;
    _ -> none
  end;
load_storage(_IsTransaction = true, OID, Type)->
  case ecomet_transaction:dict_get( {OID, storages} ) of
    #{ Type := #{ fields := Fields } } -> Fields;
    _->
      load_storage( false, OID, Type )
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
    not_found->
      ecomet_transaction:on_abort(DB,?DATA,Type,OID,delete),
      none;
    Storage->
      ecomet_transaction:on_abort(DB,?DATA,Type,OID,Storage),
      Storage
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

prepare_edit(Fields, #object{oid = OID, pattern = P} = Object)->

  Schema = ecomet_pattern:get_fields( ?map(P) ),

  {ChangesAcc, Storages} = get_transaction_data( Object ),

  Changes =
    maps:fold(fun( Field, Value, Acc )->
      case Acc of
        #{ Field := { _V1, V0 } } when V0 =:= Value->
          maps:remove( Field, Acc );
        #{ Field := { _V1, V0 } }->
          Acc#{ Field => { Value, V0 } };
        _->
          #{ storage := Type } = maps:get( Field, Schema ),
          case Storages of
            #{ Type := #{ fields := #{ Field := V0 } } } when V0 =/= Value->
              Acc#{ Field => { Value, V0 } };
            _->
              Acc#{ Field => { Value, none } }
          end
      end
    end, ChangesAcc, Fields ),

  ecomet_transaction:dict_put( #{
    {OID, data} => Changes,
    {OID, storages} => Storages
  }).

prepare_delete(#object{pattern = P} = Object)->

  Fields = maps:keys( ecomet_pattern:get_fields( ?map( P ) ) ),
  Changes = maps:from_list([{ F, none } || F <- Fields]),
  prepare_edit( Changes, Object ).

get_transaction_data(#object{oid = OID, pattern = P, db = DB})->
  case ecomet_transaction:dict_get( {OID, data}, none ) of
    none ->
      % Load object storages
      StorageTypes = ecomet_pattern:get_storage_types( ?map(P) ),
      Storages =
        lists:foldl(fun( Type, Acc )->
          case ecomet_transaction:read(DB, ?DATA, Type, OID, none ) of
            Data when is_map( Data )-> Acc#{ Type => Data };
            _-> Acc
          end
        end, #{}, StorageTypes),
      { #{}, Storages };
    ChangesAcc ->
      % The object is already under transformation
      Storages = ecomet_transaction:dict_get( {OID, storages} ),
      {ChangesAcc, Storages}
  end.


% Save routine. This routine runs when we edit object, that is not under behaviour handlers yet
save(#object{oid=OID,pattern = P}=Object, Handler)->

  % All operations within transaction. No changes applied if something is wrong
  ecomet_transaction:dict_put([
    {{OID,handler},Handler}	% Object is under the Handler
  ]),

  % Run behaviours
  [ case erlang:function_exported(Behaviour,Handler,1) of
      true ->
        Behaviour:Handler(Object);
      _ ->
        ?LOGWARNING("invalid behaviour ~p:~p",[Behaviour, Handler])
  end || Behaviour <- ecomet_pattern:get_behaviours(?map(P)) ],

  % Release object from under the Handler
  ecomet_transaction:dict_remove([{OID,handler}]),

  % Confirm the commit
  ecomet_transaction:log(OID,Object),

  % The result
  ok.

% Save object changes to the storage
commit( Commits )->
  CommitLog = commit( Commits, _Log = [] ),
  ecomet_index:commit( CommitLog ).

commit([ #object{oid = OID} = Object|Rest], Acc)->
  Changes = ecomet_transaction:dict_get( {OID, data} ),
  if
    map_size( Changes ) > 0 ->
      Log = commit_object( Object, Changes ),
      commit( Rest, [Log | Acc] )
  end;
commit([], Acc)->
  Acc.

-record(s_commit, {tags, data, type, oid, db, fields }).
-record(commit_acc, {add_tags, tags, del_tags, object }).

commit_object( #object{oid = OID, pattern = P, db = DB}=Object, Changes )->
  Map = ?map( P ),
  %----------Define fields changes-------------------
  Storages = ecomet_transaction:dict_get( {OID, storages}, #{} ),

  FieldsSchema = ecomet_pattern:get_fields( Map ),

  Tags = ecomet_index:build_index(Changes, FieldsSchema),
  % #{
  %   ram => { Add, Del },
  %   ramdisc => { Add, Del },
  %   disc => { Add, Del }
  % }


  FieldsByStorageTypes =
    maps:fold(fun(F, {V, _}, Acc )->
      #{storage := S} = maps:get( F, FieldsSchema ),
      SAcc0 =
        case Acc of
          #{ S := _SAcc } -> _SAcc;
          _->
            case Storages of
              #{ S:= #{ fields := _SAcc } } -> _SAcc;
              _-> #{}
            end
        end,

      if
        V =/= none -> Acc#{ S => SAcc0#{ F => V } };
        true -> Acc#{ S => maps:remove( F, SAcc0 ) }
      end
    end, #{}, Changes ),


  %-----Commit changes---------------------------
  EmptySet = ecomet_subscription:new_bit_set(),
  CommitAcc0 = #commit_acc{
    add_tags = EmptySet,
    tags = EmptySet,
    del_tags = EmptySet,
    object = #{}
  },
  CommitAcc1 =  maps:fold(fun(Type, Fields, Acc)->
    commit_storage(#s_commit{
      type = Type,
      oid = OID,
      db = DB,
      fields = Fields,
      tags = maps:get( Type, Tags, none ),
      data = maps:get( Type, Storages, none )
    }, Acc )
  end, CommitAcc0, FieldsByStorageTypes),

  % Add not changed tags and fields from not changed storages
  CommitAcc =
    if
      is_map(Storages), map_size( Storages ) > map_size( FieldsByStorageTypes )->
        NotChangedStorages = maps:without( maps:keys( FieldsByStorageTypes ), Storages ),
        maps:fold(fun(_S, SData, Acc )->
            not_changed( SData, Acc )
        end, CommitAcc1, NotChangedStorages);
      true ->
        CommitAcc1
    end,

  #commit_acc{
    add_tags = AddTags,
    tags = OldTags,
    del_tags = DelTags,
    object = ObjectMap
  } = CommitAcc,

  % Log timestamp. If it is an object creation then timestamp must be the same as
  % the timestamp of the object, otherwise it is taken as the current timestamp
  TS =
    case Changes of
      #{<<".ts">>:=CreateTS}->CreateTS;
      _->ecomet_lib:log_ts()
    end,

  #{
    object => ecomet_query:object_map(Object#object{pattern = get_pattern_oid( OID ), edit = false, move = false}, ObjectMap),
    db => DB,
    ts => TS,
    index_log => Tags,
    tags => { AddTags, OldTags, DelTags},
    changes => Changes
  }.

% Create new storage without tags
commit_storage(#s_commit{
  data = none,
  tags = none,
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
}, Acc )->

  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields },  none),
  Acc;

% Create new storage with tags
commit_storage(#s_commit{
  data = none,
  tags = {AddTags, _},
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
}, #commit_acc{
  add_tags = AddTagsAcc
} = Acc)->

  TagsHash = ecomet_subscription:build_tag_hash( AddTags ),
  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields, tags => TagsHash }, none),

  Acc#commit_acc{
    add_tags = ecomet_subscription:bit_or(AddTagsAcc,  TagsHash)
  };

% Update storage with tags with no new tags
commit_storage(#s_commit{
  data = #{ tags := Tags, fields := Object },
  tags = none,
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
}, #commit_acc{
  tags = TagsAcc,
  object = ObjectAcc
} = Acc ) when map_size( Fields ) > 0->

  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields, tags => Tags }, none),
  Acc#commit_acc{
    tags = ecomet_subscription:bit_or(Tags,  TagsAcc),
    object = maps:merge( Object, ObjectAcc )
  };

% Update storage without tags with no new tags
commit_storage(#s_commit{
  data = #{ fields := Object },
  tags = none,
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
}, #commit_acc{
  object = ObjectAcc
} = Acc) when Fields > 0->
  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields }, none),
  Acc#commit_acc{
    object = maps:merge( Object, ObjectAcc )
  };

% Update storage with tags with new tags
commit_storage(#s_commit{
  data = #{ tags := Tags0, fields := Object },
  tags = { AddTags, DelTags },
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
}, #commit_acc{
  add_tags = AddTagsAcc,
  tags = TagsAcc,
  del_tags = DelTagsAcc,
  object = ObjectAcc
} = Acc)->

  AddTagsHash = ecomet_subscription:build_tag_hash( AddTags ),
  DelTagsHash = ecomet_subscription:build_tag_hash( DelTags ),
  NotChanged = ecomet_subscription:bit_subtract(Tags0, DelTagsHash),

  Tags = ecomet_subscription:bit_or(AddTagsHash,  NotChanged),

  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, #{ fields => Fields, tags => Tags }, none),
  Acc#commit_acc{
    add_tags = ecomet_subscription:bit_or(AddTagsHash,  AddTagsAcc),
    tags = ecomet_subscription:bit_or(NotChanged,  TagsAcc),
    del_tags = ecomet_subscription:bit_or(DelTagsHash,  DelTagsAcc),
    object = maps:merge( Object, ObjectAcc)
  };

% Delete storage with tags
commit_storage(#s_commit{
  data = #{ tags := Tags, fields := Object},
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
}, #commit_acc{
  del_tags = DelTagsAcc,
  object = ObjectAcc
} = Acc) when map_size( Fields ) =:=0->

  ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none),

  Acc#commit_acc{
    del_tags = ecomet_subscription:bit_or(Tags,  DelTagsAcc),
    object = maps:merge( Object, ObjectAcc )
  };

% Delete storage without tags
commit_storage(#s_commit{
  data = #{ fields := Object },
  db = DB,
  type = Type,
  oid = OID,
  fields = Fields
}, #commit_acc{
  object = ObjectAcc
} = Acc ) when map_size( Fields ) =:=0->

  ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none),

  Acc#commit_acc{
    object = maps:merge( Object, ObjectAcc )
  }.

not_changed( #{ fields := Object, tags := Tags }, #commit_acc{
  tags = TagsAcc,
  object = ObjectAcc
} = Acc )->
  Acc#commit_acc{
    tags = ecomet_subscription:bit_or(Tags,  TagsAcc),
    object = maps:merge( Object, ObjectAcc )
  };
not_changed( #{ fields := Object }, #commit_acc{
  object = ObjectAcc
} = Acc )->
  Acc#commit_acc{
    object = maps:merge( Object, ObjectAcc )
  }.


%%==============================================================================================
%%	Reindexing
%%==============================================================================================
rebuild_index(OID)->
  PatternOID = ecomet_object:get_pattern_oid( OID ),
  Fields = ecomet_pattern:get_fields( PatternOID ),
  rebuild_index( OID, maps:keys(Fields) ).

rebuild_index(OID, Fields)->

  ?TRANSACTION(fun()->

    Object = open(OID),

    #object{ pattern = P, db=DB } = Object,
    Map = ?map(P),
    StorageTypes =
      maps:keys(lists:foldl(fun(F,Acc)->
        case ecomet_field:get_storage(Map,F) of
          {ok, Type}-> Acc#{Type => true};
          _-> Acc
        end
      end,#{},Fields)),
    Storages=
      lists:foldl(fun(Type,Acc)->
        case ecomet_transaction:read(DB, ?DATA, Type, OID,  none) of
          Data when is_map(Data)->
            ecomet_transaction:write(DB, ?DATA, Type, OID, Data,  none),
            ecomet_transaction:on_abort(DB, ?DATA, Type, OID, Data),
            Acc#{ Type => Data };
          _->
            Acc
        end
      end,#{}, StorageTypes ),

    FieldsChanges =
      lists:foldl(fun(F,Acc)->
        case ecomet_field:get_storage(Map,F) of
          {ok,Type}->
            case Storages of
              #{ Type := #{ fields:= #{ F:=V }} }->
                Acc#{F => {none,V}};
              _->
                Acc#{ F => {none,none} }
            end;
          _->
            Acc
        end
      end, #{}, Fields),

    StorageType = ecomet_pattern:get_storage(?map(P)),
    Tags1 = ecomet_index:build_index(FieldsChanges, Map),
    Tags =
      if
        StorageType =:= ram -> Tags1;
        StorageType =:= ramdisc ->
          case Tags1 of
            #{ disc := {DiscAdd, DiscDel}, ramdisc := {RamDiscAdd, RamDiscDel} }->
              maps:remove(disc, Tags1#{ ramdisc => {DiscAdd ++ RamDiscAdd, DiscDel ++ RamDiscDel} });
            #{ disc := DiscTags } ->
              maps:remove(disc, Tags1#{ ramdisc => DiscTags });
            _->
              Tags1
          end;
        StorageType=:=disc->
          case Tags1 of
            #{ disc := {DiscAdd, DiscDel}, ramdisc := {RamDiscAdd, RamDiscDel} }->
              maps:remove(ramdisc, Tags1#{ disc => {DiscAdd ++ RamDiscAdd, DiscDel ++ RamDiscDel} });
            #{ ramdisc := RamDiscTags } ->
              maps:remove(ramdisc, Tags1#{ disc => RamDiscTags });
            _->
              Tags1
          end
      end,

    EmptySet = ecomet_subscription:new_bit_set(),
    maps:foreach(fun(Type, TData) ->
      TFields = maps:get(fields, TData, #{}),
      TypeTags0 = maps:get(tags, maps:get(StorageType, Storages,#{}), EmptySet),
      case Tags of
        #{ Type:= {TAddTags0,TDelTags0} }->
          TAddTags = ecomet_subscription:build_tag_hash( TAddTags0 ),
          TDelTags = ecomet_subscription:build_tag_hash( TDelTags0 ),
          if
            TypeTags0=:=EmptySet->
              Data = TData#{ tags => TAddTags },
              ok = ecomet_transaction:write(DB, ?DATA, Type, OID, Data,  none);
            true->
              % TODO. We subtract tag hashes (not tags themselves), so there can be hash clashes and so we
              % can remove tags (hashes) that object actually still has if actual tag and deleted tag have the same hash
              ObjectTags = ecomet_subscription:bit_or( ecomet_subscription:bit_subtract(TypeTags0,TDelTags), TAddTags),
              if
                ObjectTags =:= EmptySet, map_size(TFields) =:= 0->
                  ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none);
                ObjectTags =:= EmptySet->
                  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, TData,  none);
                true->
                  Data = TData#{ tags => ObjectTags },
                  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, Data,  none)
              end
          end;
        _ when TypeTags0 =:=EmptySet, map_size( TFields )=:=0->
          ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none);
        _ when TypeTags0 =:=EmptySet->
          Data = maps:remove( tags, TData ),
          ok = ecomet_transaction:write(DB, ?DATA, Type, OID, Data,  none);
        _->
          Data = TData#{ tags => TypeTags0 },
          ok = ecomet_transaction:write(DB, ?DATA, Type, OID, Data,  none)
      end
    end, Storages),
    ecomet_index:commit([ #{db => DB, object => #{ <<".oid">>=>OID, object => Object }, index_log => Tags1} ]),
    ok
  end).

debug(Folder, Count, Batch)->
  ecomet:dirty_login(<<"system">>),
  P = ?OID(<<"/root/.patterns/test_pattern">>),
  F = ?OID(<<"/root/",Folder/binary>>),
  spawn(fun()->
    ecomet:dirty_login(<<"system">>),
    fill(#{<<".folder">> => F, <<".pattern">> => P}, 0, Batch, Count)
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

% ecomet_object:debug(<<"test2">>, 1000000, 1000).