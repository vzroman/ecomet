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
%%	Authentication
%%=================================================================
-export([
  login/2, login/3,
  logout/0,
  get_user/0,
  is_admin/0
]).

%%=================================================================
%%	Object-level CRUD
%%=================================================================
-export([
  create_object/1,
  open/1,open/2,open/3,open_nolock/1,open_rlock/1,open_wlock/1,
  read_field/2,read_field/3,read_fields/2,
  field_changes/2,
  edit_object/2,
  delete_object/1,
  copy_object/2
]).


%%=================================================================
%%	Query API
%%=================================================================
-export([
  query/1,
  get/3,get/4,
  subscribe/4,subscribe/5,
  unsubscribe/1,
  set/3,set/4,
  delete/2,delete/3,

  parse_query/1,
  run_query/1
]).


%%=================================================================
%%	Transactions
%%=================================================================
-export([
  is_transaction/0,
  transaction/1,
  transaction_sync/1,
  start_transaction/0,
  commit_transaction/0,
  rollback_transaction/0,
  on_commit/1
]).

%%=================================================================
%%	Identification API
%%=================================================================
-export([
  get_oid/1,
  get_path/1,
  oid2path/1,
  path2oid/1,
  is_object/1
]).

%%=================================================================
%%	Authentication
%%=================================================================
login(User,Password)->
  ecomet_user:login(User,Password).

login(User,Password,Info)->
  ecomet_user:login(User,Password,Info).

logout()->
  ecomet_user:logout().

get_user()->
  ecomet_user:get_user().

is_admin()->
  ecomet_user:is_admin().

%%=================================================================
%%	Object-level CRUD
%%=================================================================
create_object(Fields)->
  ecomet_object:create(Fields).

open(ID)->
  ecomet_lib:to_object(ID).
open(ID,Lock)->
  ecomet_lib:to_object(ID,Lock).
open(ID,Lock,Timeout)->
  ecomet_lib:to_object(ID,Lock,Timeout).
%----Legacy API---------------------------------------------------
open_nolock(ID)->
  open(ID,none).
open_rlock(ID)->
  open(ID,read).
open_wlock(ID)->
  open(ID,write).

read_field(Object,Field)->
  ecomet_object:read_field(Object,Field).
read_field(Object,Field,Default)->
  ecomet_object:read_field(Object,Field,Default).
read_fields(Object,Fields)->
  ecomet_object:read_fields(Object,Fields).

field_changes(Object,Field)->
  ecomet_object:field_changes(Object,Field).

edit_object(Object,Fields)->
  ecomet_object:edit(Object,Fields).

delete_object(Object)->
  ecomet_object:delete(Object).

copy_object(ID,Replace)->
  Object = ecomet_lib:to_object(ID),
  ecomet_object:copy(Object,Replace).

%%=================================================================
%%	Query API
%%=================================================================
query(QueryString)->
  ecomet_query:run(QueryString).

get(DBs,Fields,Conditions)->
  ecomet_query:get(DBs,Fields,Conditions).
get(DBs,Fields,Conditions,Params)->
  ecomet_query:get(DBs,Fields,Conditions,Params).

subscribe(ID,DBs,Fields,Conditions)->
  ecomet_query:subscribe(ID,DBs,Fields,Conditions).
subscribe(ID,DBs,Fields,Conditions,Params)->
  ecomet_query:subscribe(ID,DBs,Fields,Conditions,Params).

unsubscribe(ID)->
  ecomet_query:unsubscribe(ID).

set(DBs,Fields,Conditions)->
  ecomet_query:set(DBs,Fields,Conditions).
set(DBs,Fields,Conditions,Params)->
  ecomet_query:set(DBs,Fields,Conditions,Params).

delete(DBs,Conditions)->
  ecomet_query:delete(DBs,Conditions).
delete(DBs,Conditions,Params)->
  ecomet_query:delete(DBs,Conditions,Params).

parse_query(QueryString)->
  ecomet_query:parse(QueryString).
run_query(ParsedQuery)->
  ecomet_query:run_statements(ParsedQuery).

%%=================================================================
%%	Transactions API
%%=================================================================
is_transaction()->
  ecomet_transaction:get_type()=/=none.

transaction(Fun)->
  ecomet_transaction:internal(Fun).

transaction_sync(Fun)->
  ecomet_transaction:internal_sync(Fun).

start_transaction()->
  ecomet_transaction:start().

commit_transaction()->
  ecomet_transaction:commit().

rollback_transaction()->
  ecomet_transaction:rollback().

on_commit(Fun)->
  ecomet_transaction:on_commit(Fun).
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

is_object(Object)->
  ecomet_object:is_object(Object).

