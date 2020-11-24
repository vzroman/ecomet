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

%%=================================================================
%%	Service API
%%=================================================================
-export([

  get_map/1,
  edit_map/2,

  get_behaviours/1,
  set_behaviours/2,

  get_storage/1
]).

%%=================================================================
%%	Service API
%%=================================================================
get_map(Map) when is_map(Map)->
  Map;
get_map(PatternID)->
  case ecomet_schema:get_pattern(PatternID) of
    Value when is_map(Value)->Value;
    _->#{}
  end.

edit_map(PatternID,Map)->
  ecomet_schema:set_pattern(PatternID,Map).

get_behaviours(Map) when is_map(Map)->
  maps:get(handlers,Map,[]);
get_behaviours(ObjectOrOID)->
  case ecomet_object:is_object(ObjectOrOID) of
    true->
      OID = ecomet_object:get_oid(ObjectOrOID),
      get_behaviours(OID);
    _->
      % Assume its an OID
      Map = get_map(ObjectOrOID),
      get_behaviours(Map)
  end.

set_behaviours(Map,Handlers) when is_map(Map)->
  Map#{handlers=>Handlers};
set_behaviours(ObjectOrOID,Handlers)->
  case ecomet_object:is_object(ObjectOrOID) of
    true->
      OID = ecomet_object:get_oid(ObjectOrOID),
      set_behaviours(OID,Handlers);
    _->
      % Assume its an OID
      Map = get_map(ObjectOrOID),
      Map1 = set_behaviours(Map,Handlers),
      edit_map(ObjectOrOID,Map1)
  end.

get_storage(OIDOrMap)->
  % The storage type of an object is defined by its .name field
  Map = get_map(OIDOrMap),
  ecomet_field:get_storage(Map,<<".name">>).
