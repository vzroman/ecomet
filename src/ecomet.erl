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
%%	Object-level CRUD
%%=================================================================
-export([
  create_object/1,
  open/1,open/2,open/3,open_nolock/1,open_rlock/1,open_wlock/1,
  read_field/2,read_field/3,read_fields/2,
  edit_object/2,
  delete_object/1
]).

%%=================================================================
%%	Transactions
%%=================================================================
-export([
  is_transaction/0,
  transaction/1,
  sync_transaction/1,
  start_transaction/0,
  commit_transaction/0,
  rollback_transaction/0
]).

%%=================================================================
%%	Identification API
%%=================================================================
-export([
  get_oid/1,
  get_path/1,
  oid2path/1,
  path2oid/1
]).

%%=================================================================
%%	Object-level CRUD
%%=================================================================
create_object(Fields)->
  ecomet_object:create(Fields).

open(OID)->
  ecomet_object:open(OID).
open(OID,Lock)->
  ecomet_object:open(OID,Lock).
open(OID,Lock,Timeout)->
  ecomet_object:open(OID,Lock,Timeout).
%----Legacy API---------------------------------------------------
open_nolock(OID)->
  open(OID,none).
open_rlock(OID)->
  open(OID,read).
open_wlock(OID)->
  open(OID,write).

read_field(Object,Field)->
  ecomet_object:read_field(Object,Field).
read_field(Object,Field,Default)->
  ecomet_object:read_field(Object,Field,Default).
read_fields(Object,Fields)->
  ecomet_object:read_fields(Object,Fields).

edit_object(Object,Fields)->
  ecomet_object:edit(Object,Fields).

delete_object(Object)->
  ecomet_object:delete(Object).

%%=================================================================
%%	Transactions API
%%=================================================================
is_transaction()->
  ecomet_transaction:get_type()=/=none.

transaction(Fun)->
  ecomet_backend:transaction(Fun).

sync_transaction(Fun)->
  ecomet_backend:sync_transaction(Fun).

start_transaction()->
  ecomet_transaction:start().

commit_transaction()->
  ecomet_transaction:commit().

rollback_transaction()->
  ecomet_transaction:rollback().

%%=================================================================
%%	Identification API
%%=================================================================
get_oid(Object)->
  ecomet_object:get_oid(Object).

get_path(Object)->
  oid2path(get_oid(Object)).

oid2path(OID)->
  ecomet_folder:oid2path(OID).

path2oid(Path)->
  ecomet_folder:path2oid(Path).

