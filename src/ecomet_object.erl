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
  commit/2,
  get_db_id/1,
  get_db_name/1,
  get_node_id/1,
  get_pattern/1,
  get_pattern_oid/1,
  get_id/1,
  get_service_id/2
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  create/1,create/2,
  delete/1,
  open/1,open/2,open/3,
  construct/1,
  edit/2,edit/3,
  dirty_edit/2,dirty_edit/3,
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
  get_behaviours/1
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

-define(INTERNAL,#{
  read=>read,
  write=>write,
  delete=>delete,
  transaction=>internal_sync
}).
-define(EXTERNAL,#{
  read=>dirty_read,     % Reading is in dirty mode because it might be performed before the actual transaction starts
  write=>write,
  delete=>delete,
  transaction=>internal_sync
}).
-define(DIRTY,#{
  read=>dirty_read,
  write=>dirty_write,
  delete=>dirty_delete,
  transaction=>dirty
}).

% @edoc handler of ecomet object
-record(object, {oid, edit, map, deleted=false, db}).

-type object_handler() :: #object{}.
-export_type([object_handler/0]).

-define(NODE_ID_LENGTH,16).
-define(DB_ID_LENGTH,8).
-define(PATTERN_IDL_LENGTH,16).

-define(ObjectID(PatternID,ObjectID),{PatternID,ObjectID}).

-define(TRANSACTION(Fun),
  case ecomet_transaction:get_type() of
    _T when _T=:=none;_T=:=dirty->
      ?LOGDEBUG("start internal transaction"),
      case ecomet_transaction:internal_sync(Fun) of
        {ok,_TResult}->_TResult;
        {error,_TError}->?ERROR(_TError)
      end;
    _->
      ?LOGDEBUG("skip transaction"),
      Fun()
  end).
-define(DIRTY_TRANSACTION(Fun),
  case ecomet_transaction:get_type() of
    none->
      case ecomet_transaction:dirty(Fun) of
        {ok,_TResult}->_TResult;
        {error,_TError}->?ERROR(_TError)
      end;
    _-> Fun()
  end).

-define(TMODE,
  case ecomet_transaction:get_type() of
    _T when _T=:=none;_T=:=dirty->?DIRTY;
    external ->
      ?EXTERNAL;
    _->
      ?INTERNAL
  end).

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
  ParsedFields = parse_fields(Format,Map,maps:remove(<<".pattern">>,Fields)),
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
  case check_rights(Read,Write) of
    write ->
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
      Object=#object{ oid=OID, edit=true, map=Map, db=get_db_name(OID) },

      % Wrap the operation into a transaction
      ?TRANSACTION(fun()->
        % Put empty storages to dict. Trick for no real lookups
        put_empty_storages(OID,Map),
        save(Object,Fields2,on_create)
      end),
      Object;
    _->?ERROR(access_denied)
  end.

% Delete an object
delete(#object{edit=false})->?ERROR(access_denied);
delete(#object{oid=OID}=Object)->
  % Check the rights on the containing folder
  {ok,FolderID}=read_field(Object,<<".folder">>),
  #{
    <<".contentreadgroups">>:=Read,
    <<".contentwritegroups">>:=Write
  } = read_fields(construct(FolderID),#{
    <<".contentreadgroups">>=>none,
    <<".contentwritegroups">>=>none
  }),
  case check_rights(Read,Write) of
    write ->
      % Check if the object is under on_create or on_edit procedure at the moment
      Fields=ecomet_transaction:dict_get({OID,fields},#{}),
      case ecomet_transaction:dict_get({OID,handler},none) of
        none->
          % Queue the procedure.
          ?TRANSACTION( fun()->
            get_lock( write, Object, none ),
            save(Object#object{ deleted=true },Fields,on_delete)
          end );
        _->
          % Object can not be deleted? if it is under behaviour handlers
          ?ERROR(behaviours_run)
      end;
    _->?ERROR(access_denied)
  end,
  ok.

% Open object
open(OID)->open(OID,none,none).
open(OID,Lock)->open(OID,Lock,none).
open(OID,Lock,Timeout)->
  % Build object
  Object=
    case ecomet_transaction:dict_get({OID,object},none) of
      % No object in transaction dict yet, build new
      none->
        New=construct(OID),
        case check_rights(New) of
          none->?ERROR(access_denied);
          read->New#object{edit=false};
          write->New#object{edit=true}
        end;
      % Object is deleted
      #object{deleted=true}->?ERROR(object_deleted);
      % We have object in transaction dict, take it
      FoundObject->FoundObject
    end,
  % Set lock if requested
  if
    Lock=/=none->
      get_lock(Lock,Object,Timeout);
    true->ok
  end,
  Object.

%%open_stick( OID )->
%%
%%  Object0 = #object{
%%    db = DB,
%%    map = Map
%%  } =
%%    case ecomet:is_transaction() of
%%      true-> ?ERROR( is_transaction );
%%      _->
%%        Obj0 = construct( OID ),
%%        case check_rights( Obj0 ) of
%%          write->Obj0#object{edit=true};
%%          _->?ERROR(access_denied)
%%        end
%%    end,
%%
%%  % Load storage types that are not loaded yet
%%  Storage=
%%    [ case ecomet_backend:dirty_read(DB,?DATA,Type,OID) of
%%        not_found -> { Type, #{} };
%%        Loaded -> { Type, Loaded }
%%      end || Type <- ecomet_pattern:get_storage_types(Map) ],
%%
%%  BackTags =
%%    [ { Type, Tags } || { Type, #{ tags := Tags} } <- Storage ],
%%
%%  {[], Tags, [], _ }= ecomet_index:build_index(OID,Map,[], maps:from_list(BackTags) ),
%%  Rights = [ V || {<<".readgroups">>,V,_} <- Tags],
%%
%%  Object.

% Read field value
read_field(#object{oid=OID,map=Map}=Object,Field)->
  case ?SERVICE_FIELDS of
    #{Field:=Fun}->{ok,Fun(Object)};
    _->
      % Check if we have project with changes in transaction dict
      Fields=ecomet_transaction:dict_get({OID,fields},#{}),
      case maps:find(Field,Fields) of
        {ok,Value}->
          {ok,Value};
        error->
          ecomet_field:get_value(Map,OID,Field)
      end
  end.
read_field(Object,Field,Params) when is_list(Params)->
  read_field(Object,Field,maps:from_list(Params));
read_field(Object,Field,Params) when is_map(Params)->
  case read_field(Object,Field) of
    {ok,Value}->
      case {Params,Value} of
        { #{default:=Default}, none}->{ok,Default};
        { #{format:=Format}, _ }->
          {ok,Type}=ecomet_field:get_type(Object#object.map,Field),
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
read_fields(#object{oid = OID,map = Map}=Object,Fields,Params) when is_map(Fields),is_map(Params)->

  % Check if we have project with changes in transaction dict
  Project=ecomet_transaction:dict_get({OID,fields},#{}),

  % Load storage for fields absent in the project
  ToLoad = maps:merge(Project,?SERVICE_FIELDS),
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
    end,#{},maps:without(maps:keys(ToLoad),Fields)),

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
          case Project of
            #{ Name:= none }->
              Default;
            #{ Name:= New }->
              % The field has changes in the transaction
              New;
            _->
              % Look up the field in the storage
              case ecomet_field:get_storage(Map,Name) of
                {ok,S}->
                  #{S:=Values} = Storage,
                  maps:get(Name,Values,Default);
                _->
                  % undefined_field
                  undefined_field
              end
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
read_all(#object{map=Map,oid = OID}=Object,Params) when is_map(Params)->
  Fields=maps:map(fun(_,_)->none end,ecomet_pattern:get_fields(Map)),
  Fields1=Fields#{
    <<".oid">>=>OID
  },
  read_fields(Object,Fields1,Params).

% Edit object
edit(Object,Fields)->
  edit(Object,Fields,#{}).
edit(Object,Fields,Params) when is_list(Params)->
  edit(Object,Fields,maps:from_list(Params));
edit(#object{edit=false},_Fields,_Params)->?ERROR(access_denied);
edit(#object{map=Map}=Object,Fields,#{format:=Format}=Params)->
  ParsedFields= parse_fields(Format,Map,Fields),
  Params1 = maps:remove(format,Params),
  edit(Object,ParsedFields,Params1);
edit(#object{oid=OID,map=Map}=Object,Fields,_Params)->
  OldFields=ecomet_transaction:dict_get({OID,fields},#{}),

  NewFields=ecomet_field:merge(Map,OldFields,Fields),
  case ecomet_transaction:dict_get({OID,handler},none) of
    none->
      ?TRANSACTION(fun()->
        get_lock(write,Object,none),
        save(Object,NewFields,on_edit)
      end );
    _->
      % If object is under behaviour handlers, just save changes to the dict
      ecomet_transaction:dict_put([{{OID,fields},NewFields}])
  end,
  ok.

% Dirty edit object
dirty_edit(Object,Fields)->
  dirty_edit(Object,Fields,#{}).
dirty_edit(Object,Fields,Params) when is_list(Params)->
  dirty_edit(Object,Fields,maps:from_list(Params));
dirty_edit(#object{edit=false},_Fields,_Params)->?ERROR(access_denied);
dirty_edit(#object{map=Map}=Object,Fields,#{format:=Format}=Params)->
  ParsedFields= parse_fields(Format,Map,Fields),
  Params1 = maps:remove(format,Params),
  dirty_edit(Object,ParsedFields,Params1);
dirty_edit(#object{oid=OID,map=Map}=Object,Fields,_Params)->
  OldFields=ecomet_transaction:dict_get({OID,fields},#{}),

  NewFields=ecomet_field:merge(Map,OldFields,Fields),
  case ecomet_transaction:dict_get({OID,handler},none) of
    none->
      ?DIRTY_TRANSACTION(fun()->save(Object,NewFields,on_edit) end );
    _->
      % If object is under behaviour handlers, just save changes to dict
      ecomet_transaction:dict_put([{{OID,fields},NewFields}])
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
field_changes(#object{oid=OID,map=Map},Field)->
  Fields=ecomet_transaction:dict_get({OID,fields},#{}),
  ecomet_field:field_changes(Map,Fields,OID,Field).

% Check changes for the field within the transaction
object_changes(#object{oid=OID})->
  ecomet_transaction:dict_get({OID,fields},#{}).

field_type(#object{map=Map},Field)->
  case ?SERVICE_FIELDS of
    #{Field:=_}-> {ok, string};
    _->ecomet_field:get_type(Map,Field)
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
  #{
    <<".readgroups">>:=Read,
    <<".writegroups">>:=Write
  } = read_fields(Object,#{
    <<".readgroups">> => none,
    <<".writegroups">> => none
  }),
  check_rights(Read,Write);
check_rights(OID)->
  check_rights(construct(OID)).

check_rights(Read,Write)->
  case ecomet_user:is_admin() of
    {error,Error}->?ERROR(Error);
    {ok,true}->write;
    {ok,false}->
      {ok,UserGroups}=ecomet_user:get_usergroups(),
      WriteGroups=if is_list(Write)->Write; true->[] end,
      case ordsets:intersection(UserGroups,WriteGroups) of
        []->
          ReadGroups=if is_list(Read)->Read; true->[] end,
          case ordsets:intersection(UserGroups,ReadGroups) of
            []->none;
            _->read
          end;
        _->write
      end
  end.

get_behaviours(#object{map=Map})->
  ecomet_pattern:get_behaviours(Map).

%%=================================================================
%%	Service API
%%=================================================================
% Load fields storage for object. We try to minimize real storage lookups.
% 1. If object is locked, we save loaded storage to transaction dictionary. Next time we will retrieve it from there
% 2. If object is not locked, then it's dirty read. We can use cache, it may be as heavy as real lookup, but next dirty
%		lookup (read next field value) will be fast
load_storage(OID,Type)->
  % Check transaction storage first. If object is locked it may be there
  Storage =
    case ecomet_transaction:dict_get({OID,Type},undefined) of
      % Storage not loaded yet
      undefined->
        % Check lock on the object, if no, then it is dirty operation and we can use cache to boost reading
        { UseCache, DB }=
          case ecomet_transaction:dict_get({OID,object},none) of
            % Object can not be locked, if it is not contained in the dict
            none->{ true, get_db_name(OID) };
            % Check lock
            #object{map=Map,db= DBName }->
              LockKey=get_lock_key(OID,Map),
              case ecomet_transaction:find_lock(LockKey) of
                % No lock, boost by cache
                none->{ true, DBName };
                % Strict reading
                _->{ false, DBName }
              end
          end,
        #{ read:=Read } =?TMODE,
        LoadedStorage=
          case ecomet_backend:Read(DB,?DATA,Type,OID) of
            not_found->none;
            Loaded->Loaded
          end,
        % If object is locked (we do not use cache), then save storage to transaction dict
        if
          UseCache->ok;
          true-> ecomet_transaction:dict_put(#{ {OID,Type} =>LoadedStorage })
        end,
        LoadedStorage;
      % Storage is already loaded
      TStorage-> TStorage
    end,
  if
    is_map(Storage) -> maps:get(fields,Storage) ;
    true -> none
  end.

% Save object changes to the storage
commit(OID,Dict)->
  % Retrieve the object from the transaction dictionary
  Object=maps:get({OID,object},Dict),

  #object{ map=Map, db=DB } = Object,

  % Load storage types that are not loaded yet
  Storages=load_storage_types(Object,Dict),
  BackTags = get_backtags(Storages),

  % Get loaded fields grouped by storage types
  LoadedFields=get_fields(Storages),

  % Build the version of the object
  Version=
    maps:fold(fun(_T,TFields,Acc)->
      maps:merge(Acc,TFields)
    end,#{},LoadedFields),

  if
    Object#object.deleted->
      %----------Delete procedure--------------------------
      % The delete procedure is always wrapped into a true
      % transaction, the 'delete' method MUST do it
      % Purge object indexes
      {[],Tags}= ecomet_index:delete_object(OID,BackTags),
      % Purge object storage
      [ ok = ecomet_backend:delete(DB,?DATA,Type,OID,none) || Type <- maps:keys(Storages) ],
      % The log record
      #ecomet_log{
        object = ecomet_query:object_map(Object,#{}),
        db = DB,
        ts=ecomet_lib:log_ts(),
        tags={[],[],Tags},
        rights = {[],[],[ V || {<<".readgroups">>,V,_} <-Tags]},
        changes = maps:map(fun(_,_)->none end,ecomet_pattern:get_fields(Map))
      };
    true->
      %----------Create/Edit procedure-----------------------
      % Step 1. Fields
      Fields = maps:get({OID,fields},Dict),

      % Get fields changes
      {UpdatedFields,ChangedFields}=ecomet_field:save_changes(Map,Fields,LoadedFields,OID),

      TransactionType = ecomet_transaction:get_type(),
      % Indexed fields changes
      Indexed = [ F || {F,_} <- ChangedFields,
        case ecomet_field:get_index(Map,F) of
          {ok,none}->false;
          _->true
        end],

      if
        length(ChangedFields)=:=0 ->
          % No changes. Optimization
          #ecomet_log{ };
        TransactionType =:= dirty,length(Indexed) > 0->
          % Dirty transaction. Optimization.
          % If there are no changes in the indexed fields changes may
          % be performed in the dirty mode.
          % But if indexed are involved we have to wrap it into a true transaction to keep
          % them consistent
          Dict1 = maps:fold( fun(T,S,Acc)->Acc#{ {OID,T}=> S } end, Dict, Storages),
          case ecomet_transaction:internal_sync(fun()-> commit(OID,Dict1) end) of
            { ok, Log }->Log;
            { error, Error }->
              ?ERROR( Error )
          end;
        true ->
          %---------------The dump procedure----------------------------------------------------
          % Step 2. Indexes
          % Update the object indexes and get changes
          {Add,Unchanged,Del,UpdatedBackTags}= ecomet_index:build_index(OID,Map,ChangedFields,BackTags),

          % Used backend handlers
          #{ write := Write, delete := Delete } = ?TMODE,

          % Step 3. Dump changes to the storage, build the new version of the object
          NewVersion=
            maps:fold(fun(T,S,Acc)->
              case maps:find(T,UpdatedFields) of
                error->
                  % There are no changes in this type of the storage
                  maps:merge(Acc,maps:get(fields,S,#{}));
                {ok,TFields}->
                  % The storage has changes
                  case maps:size(TFields) of
                    0->
                      % Storage is empty now, delete it
                      ok = ecomet_backend:Delete(DB,?DATA,T,OID),
                      Acc;
                    _->
                      % Update storage tags
                      TTags = maps:get(T,UpdatedBackTags,maps:get(tags,S,#{})),
                      NewStorage=
                        case maps:size(TTags) of
                          0->#{fields=>TFields};
                          _->#{fields=>TFields, tags=>TTags}
                        end,
                      % Dump the new version of the storage
                      ok = ecomet_backend:Write(DB,?DATA,T,OID,NewStorage),
                      maps:merge(Acc,TFields)
                  end
              end
            end,#{},Storages),

          % Actually changed fields with their previous values
          Changes = maps:from_list([{Name,maps:get(Name,Version,none)}||{Name,_}<-ChangedFields]),

          % Log timestamp. If it is an object creation then timestamp must be the same as
          % the timestamp of the object, otherwise it is taken as the current timestamp
          TS =
            case Changes of
              #{<<".ts">>:=none}->maps:get(<<".ts">>,NewVersion);
              _->ecomet_lib:log_ts()
            end,

          % The log record
          #ecomet_log{
            object = ecomet_query:object_map(Object,NewVersion),
            db = DB,
            ts=TS,
            tags={ Add, Unchanged, Del},
            rights={
              [ V || {<<".readgroups">>,V,_} <-Add],
              [ V || {<<".readgroups">>,V,_} <-Unchanged],
              [ V || {<<".readgroups">>,V,_} <-Del]
            },
            changes = Changes
          }
      end
  end.

get_backtags(Storage)->
  maps:from_list([ { Type, Tags } || { Type, #{ tags := Tags} } <- maps:to_list(Storage) ]).

get_fields(Storage)->
  maps:from_list([ { Type, Fields } || { Type, #{ fields := Fields} } <- maps:to_list( Storage )]).
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
    _->?ERROR(can_not_change_pattern)
  end,
  case field_changes(Object,<<".ts">>) of
    none->ok;
    _->?ERROR(can_not_change_ts)
  end,
  case field_changes(Object,<<".name">>) of
    none->ok;
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
        <<".readgroups">> => ordsets:from_list(Read++Write)
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

  ID= ecomet_schema:local_increment({id,PatternID}),

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

% Acquire lock on object
get_lock(Lock,#object{oid=OID,map=Map}=Object,Timeout)->
  % Define the key
  DB = get_db_name(OID),
  Type=ecomet_pattern:get_storage(Map),
  Key = ecomet_transaction:lock_key(DB,?DATA,Type,OID),

  % Set lock on main backtag storage
  Storage = ecomet_transaction:lock(Key,Lock,Timeout),

  ecomet_transaction:dict_put([
    {{OID,object},Object},
    {{OID,Type},Storage}
  ]).

get_lock_key(OID,Map)->
  DB = get_db_name(OID),
  Type=ecomet_pattern:get_storage(Map),
  ecomet_transaction:lock_key(DB,?DATA,Type,OID).

% Fast object open, only for system dirty read
construct(OID)->
  PatternID=get_pattern_oid(OID),
  Map=ecomet_pattern:get_map(PatternID),
  DB = get_db_name(OID),
  #object{oid=OID,edit=false,map=Map,db=DB}.

put_empty_storages(OID,Map)->
  StorageTypes = [{{OID,Storage},none} || Storage<-ecomet_pattern:get_storage_types(Map) ],
  ecomet_transaction:dict_put(StorageTypes).

% Backtag handlers for commit routine
load_storage_types(#object{oid=OID,map=Map,db=DB},Dict)->
  #{read:=Read} = ?TMODE,
  List=
    [ case maps:find({OID,Type},Dict) of
        {ok, none} -> { Type, #{} };
        {ok, Loaded} -> { Type, Loaded };
        error->
          case ecomet_backend:Read(DB,?DATA,Type,OID) of
            not_found -> { Type, #{} };
            Loaded -> { Type, Loaded }
          end
      end || Type <- ecomet_pattern:get_storage_types(Map) ],
  maps:from_list(List).

% Save routine. This routine runs when we edit object, that is not under behaviour handlers yet
save(#object{oid=OID,map=Map}=Object,Fields,Handler)->

  % All operations within transaction. No changes applied if something is wrong
  ecomet_transaction:dict_put([
    {{OID,object},Object},	% Object is opened (created)
    {{OID,fields},Fields},	% Project has next changes (may by empty)
    {{OID,handler},Handler}	% Object is under the Handler
  ]),
  ecomet_transaction:queue_commit(OID),

  % Run behaviours
  [ Behaviour:Handler(Object) || Behaviour <- ecomet_pattern:get_behaviours(Map) ],

  % Release object from under the Handler
  ecomet_transaction:dict_remove([{OID,handler}]),

  % Confirm the commit
  ecomet_transaction:apply_commit(OID),
  % The result
  ok.
