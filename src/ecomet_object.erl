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

-callback on_create(Object::tuple())->{ok,none}|{ok,Object::tuple()}|{error,term()}.
-callback on_edit(Object::tuple())->{ok,none}|{ok,Object::tuple()}|{error,term()}.
-callback on_delete(Object::tuple())->{ok,none}|{ok,Object::tuple()}|{error,term()}.

%%=================================================================
%%	Service API
%%=================================================================
-export([
  load_storage/2
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  read_field/2,
  is_object/1,
  get_oid/1
]).

-record(object,{oid,edit,map,deleted=false}).

-define(OID(PatternID,ObjectID),{PatternID,ObjectID}).
%%=================================================================
%%	Data API
%%=================================================================
%%============================================================================
%% Main functions
%%============================================================================
% Create new object
create(#{ <<".pattern">>:=PatternID, <<".folder">>:=FolderID } = Fields)->
  Map=ecomet_pattern:get_map(PatternID),
  Fields=ecomet_field:build_new(Map,FieldList),
  FolderID=maps:get(<<".folder">>,Fields),
  ?assertMatch(write,check_rights(FolderID),access_denied),
  OID=new_id(FolderID,PatternID),
  Object=#object{oid=OID,edit=true,map=Map},
  ?TRANSACTION(fun()->
    % Put empty storages to dict. Trick for no real lookups
    put_empty_storages(OID,Map),
    save(Object,Fields,on_create)
               end),
  Object.

read_field(Object,Field)->
  ok.

is_object(#object{})->
  true;
is_object(_Other)->
  false.

%%Return object oid
get_oid(#object{oid=OID})->OID.

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
      DB=get_db(OID),
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

get_folder_db(?OID(_,ID)=FolderID)->
  case ecomet_schema:get_mounted_db(FolderID) of
    none->
      % The folder is a simple folder, no DB is mounted to it.
      % Obtain the ID of the db from the ID of the folder
      IDH=ID div ?BITSTRING_LENGTH,
      IDH rem 8;
    DB->
      % The folder itself is a mounted point
      ecomet_schema:get_db_id(DB)
  end.