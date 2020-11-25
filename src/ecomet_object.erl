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

-callback on_create(Object::tuple())->{ok,none}|{ok,Object::tuple()}|{error,term()}.
-callback on_edit(Object::tuple())->{ok,none}|{ok,Object::tuple()}|{error,term()}.
-callback on_delete(Object::tuple())->{ok,none}|{ok,Object::tuple()}|{error,term()}.

%%=================================================================
%%	Service API
%%=================================================================
-export([
  load_storage/2,
  delete_storage/3,
  save_storage/4,
  commit/2,
  get_db_name/1,
  get_pattern/1,
  get_id/1
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  create/1,
  delete/1,
  open/1,open/2,open/3,
  edit/2,
  read_field/2,
  read_all/1,
  field_changes/2,
  is_object/1,
  is_oid/1,
  get_oid/1,
  check_rights/1
]).

%%===========================================================================
%% Behaviour API
%%===========================================================================
-export([
  on_create/1,
  on_edit/1,
  on_delete/1
]).

-record(object,{oid,edit,map,deleted=false,db}).

-define(OID(PatternID,ObjectID),{PatternID,ObjectID}).
-define(TRANSACTION(Fun),
  case ecomet_transaction:get_type() of
    none->
      case ecomet_transaction:internal(Fun) of
        {ok,_}->ok;
        {error,Error}->?ERROR(Error)
      end;
    _->Fun()
  end).
%%=================================================================
%%	Data API
%%=================================================================
%%============================================================================
%% Main functions
%%============================================================================
% Create new object
create(#{ <<".pattern">>:=PatternID, <<".folder">>:=FolderID } = Fields)->
  % Check rights
  case check_rights(FolderID,<<".contentreadgroups">>,<<".contentwritegroups">>) of
    write ->
      % Get the schema of the object type
      Map=ecomet_pattern:get_map(PatternID),
      % Parse fields
      Fields=ecomet_field:build_new(Map,Fields),
      % Generate new ID for the object
      OID=new_id(FolderID,PatternID),
      Object=#object{oid=OID,edit=true,map=Map},
      % Wrap the operation into a transaction
      ?TRANSACTION(fun()->
        % Put empty storages to dict. Trick for no real lookups
        put_empty_storages(OID,Map),
        save(Object,Fields,on_create)
      end),
      Object;
    _->?ERROR(access_denied)
  end.

% Delete an object
delete(#object{edit=false})->?ERROR(access_denied);
delete(#object{oid=OID}=Object)->
  % Check the rights on the containing folder
  {ok,FolderID}=read_field(Object,<<".folder">>),
  case check_rights(FolderID,<<".contentreadgroups">>,<<".contentwritegroups">>) of
    write ->
      % Check if the object is under on_create or on_edit procedure at the moment
      Fields=ecomet_transaction:dict_get({OID,fields},#{}),
      case ecomet_transaction:dict_get({OID,handler},none) of
        none->
          % Queue the procedure
          save(Object#object{deleted=true},Fields,on_delete);
        _->
          % Object can not be deleted? if it is under behaviour handlers
          ?ERROR(behaviours_run)
      end;
    _->?ERROR(access_denied)
  end,
  ok.

% Read field value
read_field(#object{oid=OID,map=Map},Field)->
  % Check if we have project with changes in transaction dict
  Fields=ecomet_transaction:dict_get({OID,fields},#{}),
  case maps:find(Field,Fields) of
    {ok,Value}->
      {ok,Value};
    error->
      ecomet_field:get_value(Map,OID,Field)
  end.

% Read all object fields
read_all(#object{oid=OID,map=Map})->
  % Check if we have project with changes in transaction dict
  Fields=ecomet_transaction:dict_get({OID,fields},#{}),
  List=
    [case Fields of
       #{Name:=Value}->{ Name, Value };
       _->
         {ok,Value}=ecomet_field:get_value(Map,OID,Name),
         { Name, Value }
     end|| Name <- ecomet_field:field_names(Map)],
  maps:from_list(List).

% Check changes for the field within the transaction
field_changes(#object{oid=OID,map=Map},Field)->
  Fields=ecomet_transaction:dict_get({OID,fields},#{}),
  ecomet_field:field_changes(Map,Fields,OID,Field).

is_object(#object{})->
  true;
is_object(_Other)->
  false.

is_oid(?OID(_,_))->
  true;
is_oid(_Invalid)->
  false.

%%Return object oid
get_oid(#object{oid=OID})->OID.

%%Return oid of pattern of the object
get_pattern_oid(#object{oid=OID})->
  get_pattern_oid(OID);
get_pattern_oid(?OID(PatternID,_))->
  ?OID(?PATTERN_PATTERN,PatternID).

%% Check context user rights for the object
check_rights(Object)->
  check_rights(Object,<<".readgroups">>,<<".writegroups">>).
check_rights(#object{oid=OID,map=Map},Read,Write)->
  case ecomet_user:is_admin() of
    {error,Error}->?ERROR(Error);
    {ok,true}->write;
    {ok,false}->
      {ok,UID}=ecomet_user:get_user(),
      {ok,UGroups}=ecomet_user:get_usergroups(),
      UserGroups=ordsets:from_list([UID|UGroups]),

      WriteGroups=ecomet_field:get_value(Map,OID,Write,[]),
      case ordsets:intersection(UserGroups,WriteGroups) of
        []->
          ReadGroups=ecomet_field:get_value(Map,OID,Read,[]),
          case ordsets:intersection(UserGroups,ReadGroups) of
            []->none;
            _->read
          end;
        _->write
      end
  end;
check_rights(OID,Read,Write)->check_rights(construct(OID),Read,Write).

%%=================================================================
%%	Service API
%%=================================================================
% Load fields storage for object. We try to minimize real storage lookups.
% 1. If object is locked, we save loaded storage to transaction dictionary. Next time we will retrieve it from there
% 2. If object is not locked, then it's dirty read. We can use cache, it may be as heavy as real lookup, but next dirty
%		lookup (read next field value) will be fast
load_storage(OID,Type)->
  % Check transaction storage first. If object is locked it may there
  case ecomet_transaction:dict_get({OID,Type,fields},undefined) of
    % Storage not loaded yet
    undefined->
      % Check lock on the object, if no, then it is dirty operation and we can use cache to boost reading
      UseCache=
        case ecomet_transaction:dict_get({OID,object},none) of
          % Object can not be locked, if it is not contained in the dict
          none->true;
          % Check lock
          #object{map=Map}->
            LockKey=get_lock_key(OID,Map),
            case ecomet_transaction:find_lock(LockKey) of
              % No lock, boost by cache
              none->true;
              % Strict reading
              _->false
            end
        end,
      DB=get_db_name(OID),
      Storage=
        case ecomet_backend:dirty_read(DB,?DATA,Type,{OID,fields},UseCache) of
          not_found->none;
          Loaded->Loaded
        end,
      % If object is locked (we do not use cache), then save storage to transaction dict
      if
        UseCache->ok;
        true-> ecomet_transaction:dict_put([{{OID,Type,fields},Storage}])
      end,
      Storage;
    % Storage is already loaded
    TStorage->
      TStorage
  end.

% Save object storage
save_storage(OID,Type,Key,Value)->
  DB=get_db_name(OID),
  ecomet_backend:write(DB,object,Type,{OID,Key},Value).

delete_storage(OID,Type,Key)->
  DB=get_db_name(OID),
  ecomet_backend:delete(DB,object,Type,{OID,Key}).

% Save object changes to the storage
commit(OID,Dict)->
  % Retrieve the object from the transaction dictionary
  Object=maps:get({OID,object},Dict),
  Map=Object#object.map,
  % Load backtags that are not loaded yet
  BackTags=load_backtags(Object,Dict),
  if
    Object#object.deleted->
      %----------Delete procedure--------------------------
      % Purge object fields
      ecomet_field:delete_object(Map,OID),
      % Purge object indexes
      {[],Tags}= ecomet_index:delete_object(OID,BackTags),
      % Purge object backtags
      delete_backtags(Object),
      % The log record
      #ecomet_log{
        oid=OID,
        ts=ecomet_lib:log_ts(),
        addtags=[],
        deltags=Tags,
        tags=[],
        fields=[]
      };
    true->
      %----------Create/Edit procedure-----------------------
      % Step 1. Fields
      #{ {OID,fields} := Fields} = Dict,
      % Get loaded fields grouped by storage types
      LoadedFields=
        lists:foldl(fun(Storage,Acc)->
          case maps:find({OID,Storage,fields},Dict) of
            {ok,StorageFields}->Acc#{Storage=>StorageFields};
            error->Acc
          end
        end,#{},ecomet_field:fields_storages(Map)),
      % Dump fields changes
      ChangedFields=ecomet_field:save_changes(Map,Fields,LoadedFields,OID),

      % Step 2. Indexes
      % Update the object indexes and get changes
      {Add,Unchanged,Del,UpdatedBackTags}= ecomet_index:build_index(OID,Map,ChangedFields,BackTags),
      % Update the backtags
      save_backtags(maps:to_list(UpdatedBackTags),OID),
      % TODO. We need to know fields types when exporting values. Is there a better solution?
      TypedFields=
        [ begin
            {ok,Type}=ecomet_field:get_type(Map,Field),
            { Field, {Type,Value} }
          end || { Field, Value } <- ChangedFields ],
        lists:foldl(fun({Field,Value},Acc)->
          {ok,Type}=ecomet_field:get_type(Map,Field),
          [{Field,{Type,Value}}|Acc]
        end,[],ChangedFields),
      % The log record
      #ecomet_log{
        oid=OID,
        ts=ecomet_lib:log_ts(),
        addtags=Add,
        deltags=Del,
        tags=Unchanged,
        fields=TypedFields
      }
  end.

%%=====================================================================
%% Behaviour handlers
%%=====================================================================
on_create(Object)->
  {ok,FolderID}=ecomet:read_field(Object,<<".folder">>),
  % Check storage type of folder, temporary folders can not contain persistent objects
  ?assertNotError(ecomet_folder:check_ram(FolderID,Object),folder_is_ramonly),
  % Check for unique names in folder
  {ok,Name}=ecomet:read_field(Object,<<".name">>),
  ?assertMatch({error,notfound},ecomet_folder:find_object(FolderID,Name),name_not_unique),
  %Get rights
  ecomet:edit_object(Object,lists:append(
    create_rights(Object,FolderID),
    [{<<".ts">>,ecomet_ntpsrv:get_ts()}]
  )).

on_edit(Object)->
  % Check domain change
  check_domain(Object),
  % Check for unique name in folder
  check_path(Object),
  ?assertMatch(none,ecomet_object:field_changes(Object,<<".pattern">>),can_not_change_pattern),
  ?assertMatch(none,ecomet_object:field_changes(Object,<<".ts">>),can_not_change_ts),
  edit_rights(Object).

on_delete(_Object)->ok.

%%============================================================================
%%	Internal helpers
%%============================================================================
% Unique object identification. The principles:
% * the id is a tuple of 2 elements: { PatternID, ObjectID }
%   - the PatternID is an id of the pattern (type) of the object. It defines its schema.
%    The PatternID consists only of the second (ObjectID) of the related pattern
%   - the ObjectID is a unique (system wide) id of the object within a pattern (type)
% * the ObjectID is a composed integer that can be presented as:
%   - IDHIGH = ObjectID div ?BITSTRING_LENGTH. It's sort of high level degree
%   - IDLOW  = ObjectID rem ?BITSTRING_LENGTH (low level degree)
% * The system wide increment is too expensive, so the initial increment is the node-wide only
%   and then the unique ID of the node is twisted into the IDHIGH. Actually it is added
%   as 2 least significant bytes to the IDHIGH (IDHIGH = IDHIGH bsl 16 + NodeID )
% * To be able to obtain the database to which the object belongs we insert (code) it into
%   the IDHIGH the same way as we do with the NodeID: IDHIGH = IDHIGH bsl 8 + MountID.
% The final IDHIGH is:
%   <IDHIGH,NodeID:16,DB:8>
new_id(FolderID,?OID(PatternID,_))->
  NodeID = ecomet_node:get_unique_id(),
  DB = get_folder_db( FolderID ),
  ID= ecomet_schema:local_increment({id,PatternID}),
  % We can get id that is unique for this node. Unique id for entire system is too expensive.
  % To resolve the problem we mix NodeId (it's unique for entire system) into IDH.
  % IDH format - <IDH,NodeID:16>
  IDH=ID div ?BITSTRING_LENGTH,
  IDL=ID rem ?BITSTRING_LENGTH,
  IDH1 = ((IDH bsl 16) + NodeID) bsl 8 + DB,
  { PatternID, IDH1 * ?BITSTRING_LENGTH + IDL }.

get_folder_db(FolderID)->
  case ecomet_schema:get_mounted_db(FolderID) of
    none->
      % The folder is a simple folder, no DB is mounted to it.
      % Obtain the ID of the db from the ID of the folder
      get_db_id(FolderID);
    DB->
      % The folder itself is a mounted point
      ecomet_schema:get_db_id(DB)
  end.

get_db_id(?OID(_,ID))->
  IDH=ID div ?BITSTRING_LENGTH,
  IDH rem 8.

get_db_name(OID)->
  ID = get_db_id(OID),
  ecomet_schema:get_db_name(ID).

get_pattern(?OID(PatternID,_))->
  PatternID.

get_id(?OID(_,ObjectID))->
  ObjectID.

get_lock_key(OID,Map)->
  DB = get_db_name(OID),
  {ok,Type}=ecomet_pattern:get_storage(Map),
  ecomet_transaction:lock_key(DB,object,Type,{OID,backtag}).

% Fast object open, only for system dirty read
construct(OID)->
  PatternID=get_pattern_oid(OID),
  Map=ecomet_pattern:get_map(PatternID),
  #object{oid=OID,edit=false,map=Map}.

put_empty_storages(OID,Map)->
  BackTags= [{{OID,Storage,backtag},none} || Storage<-ecomet_field:index_storages(Map) ],
  Fields = [{{OID,Storage,fields},none} || Storage<-ecomet_field:fields_storages(Map)],
  ecomet_transaction:dict_put(BackTags ++ Fields).

% Backtag handlers for commit routine
load_backtags(#object{oid=OID,map=Map},Dict)->
  DB = get_db_name(OID),
  List=
    [ case Dict of
        #{ {OID,Type,backtag} := none } -> { Type, #{} };
        #{ {OID,Type,backtag} := Loaded } -> { Type, Loaded };
        _->
          case ecomet_backend:dirty_read(DB,object,Type,{OID,backtag}) of
            not_found -> { Type, #{} };
            Loaded -> { Type, Loaded }
          end
      end || Type <- ecomet_field:index_storages(Map) ],
  maps:from_list(List).

save_backtags([{Storage,Tags}|Rest],OID)->
  save_storage(OID,Storage,backtag,Tags),
  save_backtags(Rest,OID);
save_backtags([],_OID)->ok.

delete_backtags(#object{oid=OID,map=Map})->
  DB=get_db_name(OID),
  [ ok = ecomet_backend:delete(DB,object,Type,{OID,backtag}) || Type <- ecomet_field:index_storages(Map)],
  ok.

% Save routine. This routine runs when we edit object, that is not under behaviour handlers yet
save(#object{oid=OID,map=Map}=Object,Fields,Handler)->
  % All operations within transaction. No changes applied if something is wrong
  ?TRANSACTION(fun()->
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
    % The result
    ok
  end).
