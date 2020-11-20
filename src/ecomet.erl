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
-module(ecomet).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  transaction/1, sync_transaction/1,
  oid2path/1,
  path2oid/1,
  read_field/2,read_field/3
]).

%%=================================================================
%%	Data API
%%=================================================================
oid2path(OID)->
  ecomet_folder:oid2path(OID).

path2oid(Path)->
  ecomet_folder:path2oid(Path).

read_field(Object,FieldName)->
  read_field(Object,FieldName,none).
read_field(Object,FieldName,Default)->
  case ecomet_object:read_field(Object,FieldName) of
    {ok,none}->{ok,Default};
    Result->Result
  end.

transaction(Fun)->
  ecomet_backend:transaction(Fun).

sync_transaction(Fun)->
  ecomet_backend:sync_transaction(Fun).