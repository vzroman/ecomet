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
  create_object/1,create_object/2,
  open/1,open/2,open/3,open_nolock/1,open_rlock/1,open_wlock/1,
  read_field/2,read_field/3,read_fields/2,read_fields/3,
  read_all/1,read_all/2,
  field_changes/2,
  field_type/2,
  edit_object/2,edit_object/3,
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
  to_oid/1,
  path2oid/1,
  to_path/1,
  is_object/1,
  is_oid/1,
  get_pattern_oid/1
]).

%%=================================================================
%%	Formatters
%%=================================================================
-export([
  to_string/2,
  from_string/2,

  to_json/2,
  from_json/2
]).

%%=================================================================
%%	Utilities
%%=================================================================
-export([
  object_behaviours/1,
  ts/0,
  stop/0
]).

% @edoc ecomet object denotes map where each key is field_key()
% and corresponding value is field_value()
-type object() :: map().

% @edoc object handler of ecomet object
-type object_handler() :: ecomet_object:object_handler().

% @edoc object identifier
-type oid() :: {non_neg_integer(), non_neg_integer()}.

% @edoc value of field is valid erlang term
-type field_value() :: term().

% @edoc field key of any field in DB is binary
-type field_key() :: binary().

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
% @edoc Create new ecomet object
-spec create_object(Fields :: object()) -> object_handler().

create_object(Fields)->
  ecomet_object:create(Fields).

create_object(Fields,Params)->
  ecomet_object:create(Fields,Params).

% @edoc Return object hanler for given OID
-spec open(ID :: oid()) -> object_handler().

open(ID)->
  ecomet_lib:to_object(ID).

open(ID, Lock)->
  ecomet_lib:to_object(ID, Lock).

open(ID,Lock,Timeout)->
  ecomet_lib:to_object(ID, Lock, Timeout).

%----Legacy API---------------------------------------------------
open_nolock(ID)->
  open(ID, none).

open_rlock(ID)->
  open(ID, read).

open_wlock(ID)->
  open(ID, write).

% @edoc Return field value for given ecomet object
-spec read_field(Object :: object_handler(), Field :: field_key()) -> {ok, field_value()} | {error, Error::term()}.

read_field(Object, Field) ->
  ecomet_object:read_field(Object, Field).


% @edoc Return field value or default for given ecomet object
-spec read_field(Object :: object_handler(), Field :: field_key(), Params :: map()) -> {ok, field_value()} | {error, Error::term()}.

read_field(Object, Field, Params) ->
  ecomet_object:read_field(Object, Field, Params).

% @edoc Return fields for given ecomet object
-spec read_fields(Object :: object_handler(), Fields :: [field_key()]) -> map().

read_fields(Object, Fields)->
  ecomet_object:read_fields(Object, Fields).

read_fields(Object, Fields, Params)->
  ecomet_object:read_fields(Object, Fields, Params).

read_all(Object)->
  ecomet_object:read_all(Object).

read_all(Object,Params)->
  ecomet_object:read_all(Object,Params).


field_changes(Object, Field)->
  ecomet_object:field_changes(Object,Field).

field_type(Object, Field)->
  ecomet_object:field_type(Object,Field).

% @edoc Deletes existing ecomet object
-spec edit_object(Object :: object_handler(), Fields :: map()) -> ok.

edit_object(Object, Fields)->
  ecomet_object:edit(Object, Fields).

edit_object(Object, Fields, Params)->
  ecomet_object:edit(Object, Fields, Params).

% @edoc Deletes existing ecomet object
-spec delete_object(Object :: object_handler()) -> ok.

delete_object(Object)->
  ecomet_object:delete(Object).

% @edoc Deletes existing ecomet object
-spec copy_object(ID :: oid(), Overwrite :: map()) -> ok.

copy_object(ID, Overwrite)->
  Object = ecomet_lib:to_object(ID),
  ecomet_object:copy(Object, Overwrite).

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
to_oid(Object)->
  ecomet_lib:to_oid(Object).

path2oid(Path)->
  ecomet_folder:path2oid(Path).

to_path(Object)->
  ecomet_lib:to_path(Object).

is_object(Object)->
  ecomet_object:is_object(Object).

is_oid(Value)->
  ecomet_object:is_oid(Value).

get_pattern_oid(ID)->
  ecomet_object:get_pattern_oid(ID).

%%=================================================================
%%	Formatters
%%=================================================================
to_string(Type,Value)->
  ecomet_types:to_string(Type,Value).

from_string(Type,Value)->
  ecomet_types:from_string(Type,Value).

to_json(Type,Value)->
  ecomet_types:to_json(Type,Value).

from_json(Type,Value)->
  ecomet_types:from_json(Type,Value).

%%=================================================================
%%	Utilities
%%=================================================================
object_behaviours(ID)->
  ecomet_object:get_behaviours(open(ID)).

ts()->
  ecomet_lib:ts().

stop()->
  application:stop(ecomet),
  dlss:stop().

