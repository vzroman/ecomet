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

%%=================================================================
%%	Data API
%%=================================================================
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