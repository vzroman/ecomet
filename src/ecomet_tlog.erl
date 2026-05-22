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
-module(ecomet_tlog).

-export([
  create/1,
  open/1,
  close/1,
  remove/1,
  add/2,
  delete/2,
  replay/2
]).

create(DBDir)->
  zaya_rocksdb:create(db_params(DBDir)).

open(DBDir)->
  zaya_rocksdb:open(db_params(DBDir)).

close(Ref)->
  zaya_rocksdb:close(Ref).

remove(DBDir)->
  zaya_rocksdb:remove(db_params(DBDir)).

db_params(DBDir)->
  #{
    dir => filename:join(DBDir, "TLOG")
  }.

add(Ref, Ops)->
  TRef = make_ref(),
  zaya_rocksdb:write(Ref,[{TRef, Ops}]),
  TRef.

delete(Ref, TRef)->
  zaya_rocksdb:delete(Ref, [TRef]).

replay(Ref, Callback)->
  Entries = zaya_rocksdb:find(Ref, #{}),
  lists:foreach(fun({TRef, Ops})->
    ok = Callback(Ops),
    ok = delete(Ref, TRef)
  end, Entries),
  ok.
