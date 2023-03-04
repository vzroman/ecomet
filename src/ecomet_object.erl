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
        save(Object, on_delete),
        prepare_delete(OID ,Object),
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

read_field(#object{oid = OID, pattern = P} = Object, Field)->
  case ?SERVICE_FIELDS of
    #{Field:=Fun}->{ok,Fun(Object)};
    _->
      ecomet_field:get_value(?map(P),OID,Field)
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

  Map = ?map(P),
  % Load storage for fields absent in the project
  Storage=
    maps:fold(fun(F,_,Acc)->
      case ecomet_field:get_storage(Map,F) of
        {ok,S}->
          case Acc of
            #{S:=_}->Acc;
            _->
              case load_storage(OID,S) of
                Values when is_map(Values)-> Acc#{S=>Values};
                _-> Acc#{S=>#{}}
              end
          end;
        _->
          % undefined_field
          Acc
      end
    end,#{},maps:without(maps:keys(?SERVICE_FIELDS),Fields)),

  Formatter=
    case Params of
      #{format:=Format}->Format;
      _->fun(_Type,Value)->Value end
    end,

  maps:map(fun(Name,Default)->
    case ?SERVICE_FIELDS of
      #{Name:=Fun}->
        % The field is a virtual field
        Fun(Object);
      _->
        % The real field
        Value=
          case ecomet_field:get_storage(Map,Name) of
            {ok,S}->
              case Storage of
                #{ S := #{ Name := _Value } } when _Value =/= none-> _Value;
                _-> Default
              end;
            _->
              % undefined_field
              undefined_field
          end,
        if
          Value=/=undefined_field ->
            {ok,Type}=ecomet_field:get_type(Map,Name),
            Formatter(Type,Value);
          true ->
            Formatter(string,undefined_field)
        end
    end
  end,Fields).

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
field_changes(#object{oid=OID,pattern = P,db = DB},Field)->
  {ok,Type} = ecomet_field:get_storage(?map(P), Field),
  case ecomet_transaction:changes(DB, ?DATA, Type, OID ) of
    none -> none;
%-------Type storage is under create-------------------------------------
    {delete, #{fields := #{Field := NewValue}}}->
      if
        NewValue =/= none-> {NewValue, none};
        true -> none
      end;
    {delete, _NewStorage}->
      none;
%-------Type storage is under update-------------------------------------
    {#{fields := OldFields}, #{fields := NewFields}}->
      case {OldFields, NewFields} of
        {#{Field := OldValue}, #{Field := NewValue}}->
          if
            OldValue =/= NewValue-> {NewValue, OldValue};
            true -> none
          end;
        {#{Field := none}, _}->
          none;
        {#{Field := OldValue}, _}->
          {none, OldValue};
        {_, #{Field := none}}->
          none;
        {_, #{Field := NewValue}}->
          {NewValue, none};
        {_,_}->
          none
      end
  end.

% Check changes for the field within the transaction
object_changes(Object)->
  maps:fold(fun(_T,{ Data0, Data1 }, Acc)->
    Fields0 = maps:get(fields, Data0, #{}),
    maps:fold(fun(F,V1,TAcc)->
      TAcc#{ F => {V1, maps:get(F, Fields0, none)} }
    end, Acc, maps:get(fields, Data1, #{}))
  end, #{}, compile_changes( Object )).

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
  DB = get_db_name( OID ),
  case ecomet_transaction:read(DB, ?DATA, Type, OID, _Lock=none ) of
    #{ fields := Fields } = Storage->
      ecomet_transaction:on_abort(DB, ?DATA, Type, OID, Storage),
      Fields;
    _->
      ecomet_transaction:on_abort(DB, ?DATA, Type, OID, delete),
      none
  end.

%%=====================================================================
%% Behaviour handlers
%%=====================================================================
on_create(Object)->
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
    {_,none}->ok;
    {_New, Old}->
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
          _:object_deleted ->ok
      end;
    _->
      ok
  end,
  ok.

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
      if
        Name =:= <<>> -> ?ERROR("name can not be empty");
        true -> ok
      end,
      OID = get_oid(Object),
      % Check that name does not include the path delimiter
      case binary:split(Name,<<"/">>) of
        [Name]->
          case ecomet_folder:find_object_system(FolderID,Name) of
            {error,not_found}->ok;
            {ok,OID}->ok;
            _->?ERROR({not_unique,Name})
          end;
        _->?ERROR("the '/' symbol is not allowed in names")
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

by_storage_types( Fields, Map )->
  maps:fold(fun(F,V,Acc)->
    {ok,T} = ecomet_field:get_storage( Map, F ),
    TAcc = maps:get(T, Acc, #{}),
    Acc#{ T=> TAcc#{ F => V }}
  end,#{}, Fields).

prepare_create(Fields, #object{oid = OID, pattern = P, db = DB})->

  ByTypes = by_storage_types( Fields, ?map(P)),

  % Transaction write
  maps:map(fun(Type,Data)->
    ok = ecomet_transaction:write( DB, ?DATA, Type, OID, #{ fields => Data },  none),
    % Avoid looking up for rollback values
    ok = ecomet_transaction:on_abort( DB, ?DATA, Type, OID, delete )
  end, ByTypes).

prepare_edit(Fields, #object{oid = OID, pattern = P, db = DB})->
  ByTypes = by_storage_types( Fields, ?map(P) ),
  maps:map(fun(Type,Data)->
    case ecomet_transaction:changes(DB, ?DATA, Type, OID ) of
      {_, #{fields := Data0}}->
        ok = ecomet_transaction:write( DB, ?DATA, Type, OID, #{ fields => maps:merge(Data0,Data) },  none);
      _->
        % The storage type is not loaded yet
        case ecomet_transaction:read(DB, ?DATA, Type, OID, none) of
          #{fields := Data0} = Storage->
            ok = ecomet_transaction:write( DB, ?DATA, Type, OID, #{ fields => maps:merge(Data0,Data) }, none),
            ok = ecomet_transaction:on_abort(DB, ?DATA, Type, OID, Storage);
          _->
            ok = ecomet_transaction:write( DB, ?DATA, Type, OID, #{ fields => Data }, none),
            ok = ecomet_transaction:on_abort(DB, ?DATA, Type, OID, delete)
        end
    end
  end, ByTypes).

prepare_delete(OID, #object{pattern = P, db = DB})->
  Map = ?map(P),
  [ ok = ecomet_transaction:delete( DB, ?DATA, T, OID,  none ) || T <- ecomet_pattern:get_storage_types( Map ) ],
  ok.

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

compile_changes( #object{oid = OID, pattern = P, db = DB} )->
  lists:foldl(fun(Type, Acc)->
    case ecomet_transaction:changes(DB, ?DATA, Type, OID) of
      {#{fields := Fields0}=Data0, #{fields:=Fields1}}->
        Fields = maps:filter(fun(_F, V)-> V =/= none end, Fields1),
        if
          Fields =:= Fields0 ->
            % No real changes
            ok = ecomet_transaction:write(DB, ?DATA, Type, OID, Data0,  none),
            Acc;
          map_size( Fields ) > 0 ->
            Acc#{ Type => { Data0, #{ fields=>Fields }}};
          true ->
            % Storage type is to be deleted
            Acc#{ Type=> {Data0, #{}} }
        end;
      {delete, #{fields := Fields1}}->
        case maps:filter(fun(_F, V)-> V =/= none end, Fields1) of
          Fields when map_size(Fields) > 0->
            Acc#{ Type => { #{}, #{ fields=>Fields }}};
          _->
            % No real changes
            ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none),
            Acc
        end;
      {Data0,delete}->
        Acc#{ Type => { Data0, #{}}};
      {delete}->
        case ecomet_db:read(DB, ?DATA, Type, OID) of
          #{fields := _ } = Data0->
            ecomet_transaction:on_abort(DB, ?DATA, Type, OID, Data0),
            Acc#{ Type => { Data0, #{}}};
          _->
            % No real changes
            ecomet_transaction:on_abort(DB, ?DATA, Type, OID, delete),
            Acc
        end;
      _->
        % No storage type changes
        Acc
    end
  end,#{}, ecomet_pattern:get_storage_types(?map(P))).

object_rollback(#object{oid = OID, pattern = P, db = DB}, Changes)->
  lists:foldl(fun(Type,Acc)->
    case Changes of
      #{ Type := {Data0, _}} when is_map(Data0)->
        Acc#{ Type => Data0 };
      _->
        case ecomet_transaction:read(DB, ?DATA, Type, OID,  none) of
          Data0 when is_map( Data0 )->
            Acc#{ Type => Data0 };
          _->
            Acc
        end
    end
  end,#{}, ecomet_pattern:get_storage_types(?map(P))).


% Save object changes to the storage
commit([Object|Rest])->
  case compile_changes( Object ) of
    Changes when map_size( Changes ) > 0->
      Rollback = object_rollback(Object, Changes ),
      [do_commit( Object, Changes, Rollback )| commit(Rest) ];
    _->
      % No real changes
      commit( Rest )
  end;
commit([])->
  [].

do_commit( #object{oid = OID, pattern = P, db = DB}=Object, Changes, Rollback )->
  %----------Define fields changes-------------------
  FieldsChanges =
    maps:fold(fun(_Type,{ Data0, Data1 },Acc0)->
      Fields0 = maps:get(fields,Data0,#{}),
      Fields1 = maps:get(fields,Data1,#{}),
      lists:foldl(fun(F, Acc)->
        case {Fields0, Fields1} of
          {#{F := V0}, #{F := V1}}->
            if
              V0 =/= V1 -> Acc#{ F => {V0, V1}};
              true -> Acc
            end;
          {_, #{F := V1}} ->
            Acc#{ F => {none, V1} };
          {#{F := V0}, _} ->
            Acc#{ F => {V0, none} }
        end
      end, Acc0, lists:usort( maps:keys(Fields0) ++ maps:keys(Fields1)))
   end, #{}, Changes),

  Tags1 = ecomet_index:build_index(FieldsChanges, ?map(P)),
  % #{
  %   ram => { Add, Del },
  %   ramdisc => { Add, Del },
  %   disc => { Add, Del }
  % }
  StorageType = ecomet_pattern:get_storage(?map(P)),
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
  % [ramdisc, disc] -> ecomet_pattern:get_storage( ?map(P) )
  % [ram] -> ram
  %
  % Now we have this form:
  % #{
  %   ramdisc => { Add, Del },
  %   ram => { Add, Del }
  % }
  %
  % If Changes don't have some of types that Indexes have we need to add them.
  % Fields should be taken from Rollback
  Changes1 =
    case Tags  of
      #{StorageType:=_}->
        case Changes of
          #{StorageType:=_} -> Changes;
          _->
            TRollBack = maps:get(StorageType, Rollback,#{}),
            Changes#{ StorageType=>{ TRollBack, maps:remove(tags,TRollBack) }}
        end;
      _->
        Changes
    end,
  %-----Commit changes---------------------------
  EmptySet = ecomet_subscription:new_bit_set(),
  { Add, NotChanged, Del }=
    maps:fold(fun(Type,{_, TData},{AddAcc,NotChangedAcc,DelAcc}=Acc) ->
      TFields = maps:get(fields, TData, #{}),
      TypeTags0 = maps:get(tags, maps:get(StorageType, Rollback,#{}), EmptySet),
      case Tags of
        #{ Type:= {TAddTags0,TDelTags0} }->
          TAddTags = ecomet_subscription:build_tag_hash( TAddTags0 ),
          TDelTags = ecomet_subscription:build_tag_hash( TDelTags0 ),
          if
            TypeTags0=:=EmptySet->
              Data = TData#{ tags => TAddTags },
              ok = ecomet_transaction:write(DB, ?DATA, Type, OID, Data,  none),
              { ecomet_subscription:bit_or(TAddTags, AddAcc), NotChangedAcc, DelAcc};
            true->
              ObjectTags = ecomet_subscription:bit_or( ecomet_subscription:bit_subtract(TypeTags0,TDelTags), TAddTags),
              if
                ObjectTags =:= EmptySet, map_size(TFields) =:= 0->
                  ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none),
                  {AddAcc, NotChangedAcc, ecomet_subscription:bit_or(TDelTags, DelAcc)};
                ObjectTags =:= EmptySet->
                  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, TData,  none),
                  {AddAcc, NotChangedAcc, ecomet_subscription:bit_or(TDelTags, DelAcc)};
                true->
                  Data = TData#{ tags => ObjectTags },
                  ok = ecomet_transaction:write(DB, ?DATA, Type, OID, Data,  none),
                  {
                    ecomet_subscription:bit_or(TAddTags, AddAcc),
                    ecomet_subscription:bit_or(NotChangedAcc, ecomet_subscription:bit_subtract(TypeTags0, TDelTags )),
                    ecomet_subscription:bit_or(TDelTags, DelAcc)
                  }
              end
          end;
        _ when TypeTags0 =:=EmptySet, map_size( TFields )=:=0->
          ok = ecomet_transaction:delete(DB, ?DATA, Type, OID,  none),
          Acc;
        _ when TypeTags0 =:=EmptySet->
          Data = maps:remove( tags, TData ),
          ok = ecomet_transaction:write(DB, ?DATA, Type, OID, Data,  none),
          Acc;
        _->
          Data = TData#{ tags => TypeTags0 },
          ok = ecomet_transaction:write(DB, ?DATA, Type, OID, Data,  none),
          Acc
      end
  end,{ EmptySet, EmptySet, EmptySet }, Changes1),


  %--------------Prepare commit log-------------------
  ObjectMap0 =
    maps:fold(fun(_Type, {_,TData}, Acc)->
      maps:merge( maps:get(fields,TData,#{}), Acc )
    end,#{}, Changes),
  ObjectMap =
    maps:fold(fun(_Type,TData, Acc)->
      maps:merge( maps:get(fields,TData,#{}), Acc )
    end, ObjectMap0, maps:without(maps:keys(Changes), Rollback)),

  % Actually changed fields with their previous values
  Changes0 = maps:map(fun(_F,{V0,_})-> V0 end,FieldsChanges),

  % Log timestamp. If it is an object creation then timestamp must be the same as
  % the timestamp of the object, otherwise it is taken as the current timestamp
  TS =
    case FieldsChanges of
      #{<<".ts">>:={none, CreateTS}}->CreateTS;
      _->ecomet_lib:log_ts()
    end,

  #{
    object => ecomet_query:object_map(Object#object{pattern = get_pattern_oid( OID ), edit = false, move = false}, ObjectMap),
    db => DB,
    ts => TS,
    index_log => Tags1,
    tags => { Add,NotChanged,Del},
    changes => Changes0
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