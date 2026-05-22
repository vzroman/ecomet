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
  seq/1,
  commit/3,
  replay/2
]).

-record(ref,{
  db,
  seq
}).

-define(DEFAULT_ROCKSDB_PARAMS,#{
  rocksdb => #{
    open_options=>#{
      paranoid_checks => false,
      compression => none
    },
    read => #{
      verify_checksums => false
    },
    write => #{
      sync => true
    }
  },
  pool => disabled
}).

create(DBDir)->
  DBRef = zaya_rocksdb:create(params(DBDir)),
  init_ref(DBRef).

open(DBDir)->
  Params = params(DBDir),
  DBRef =
    case filelib:is_dir(log_dir(DBDir)) of
      true -> zaya_rocksdb:open(Params);
      false -> zaya_rocksdb:create(Params)
    end,
  init_ref(DBRef).

close(#ref{db = DBRef})->
  zaya_rocksdb:close(DBRef).

remove(DBDir)->
  case filelib:is_dir(log_dir(DBDir)) of
    true -> zaya_rocksdb:remove(params(DBDir));
    false -> ok
  end.

seq(#ref{seq = SeqRef})->
  atomics:add_get(SeqRef, 1, 1) - 1.

commit(#ref{db = DBRef}, Write, Delete)->
  zaya_rocksdb:commit(DBRef, Write, Delete).

replay(#ref{db = DBRef} = Ref, Callback)->
  Entries = zaya_rocksdb:find(DBRef, #{}),
  lists:foreach(fun({Key, Ops})->
    ok = Callback(Ops),
    ok = commit(Ref, [], [Key])
  end, Entries),
  ok.

params(DBDir)->
  maps:merge(?DEFAULT_ROCKSDB_PARAMS, #{
    dir => log_dir(DBDir)
  }).

log_dir(DBDir)->
  filename:join(DBDir, "TLOG").

init_ref(DBRef)->
  SeqRef = atomics:new(1, [{signed, false}]),
  ok = atomics:put(SeqRef, 1, init_seq(DBRef)),
  #ref{db = DBRef, seq = SeqRef}.

init_seq(DBRef)->
  zaya_rocksdb:foldl(DBRef, #{}, fun({{commit, Seq}, _Value}, Acc) when is_integer(Seq)->
    erlang:max(Seq + 1, Acc);
  (_Other, Acc)->
    Acc
  end, 0).
