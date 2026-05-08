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
-module(ecomet_backend_contract_SUITE).

-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").

-record(key, {type, storage, key}).

%% API
-export([
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1
]).

-export([
  index_prepare_rollback_inverts_boolean_updates/1,
  db_prepare_rollback_returns_public_keys_for_data/1,
  db_prepare_rollback_routes_boolean_indexes_through_ecomet_index/1,
  db_commit_can_apply_prepared_mixed_rollback/1,
  db_commit_can_apply_prepared_index_only_rollback/1
]).

%% Test backend API
-export([
  read/2,
  write/2,
  delete/2,
  commit/3,
  prepare_rollback/3,
  find/2
]).

all()->
  [
    index_prepare_rollback_inverts_boolean_updates,
    db_prepare_rollback_returns_public_keys_for_data,
    db_prepare_rollback_routes_boolean_indexes_through_ecomet_index,
    db_commit_can_apply_prepared_mixed_rollback,
    db_commit_can_apply_prepared_index_only_rollback
  ].

groups()->
  [].

init_per_suite(Config)->
  catch ets:delete(?LOCKS),
  {LockOwner, LockPid} = start_locks(),
  [{lock_owner, LockOwner}, {lock_pid, LockPid} | Config].

end_per_suite(Config)->
  LockOwner = ?GET(lock_owner, Config),
  LockPid = ?GET(lock_pid, Config),
  catch (LockOwner ! stop),
  catch exit(LockPid, shutdown),
  catch ets:delete(?LOCKS),
  ok.

index_prepare_rollback_inverts_boolean_updates(_Config)->
  AddKey = {{<<"field">>, <<"value">>, simple}, [idl, 1, 0, 1]},
  DelKey = {{<<"field">>, <<"old">>, simple}, [idl, 1, 0, 1]},
  Rollback = ecomet_index:prepare_rollback([
    {AddKey, true},
    {DelKey, false}
  ]),
  [{AddKey, false}, {DelKey, true}] = Rollback,
  ok.

db_prepare_rollback_returns_public_keys_for_data(_Config)->
  with_ref(fun(Ref = #{ram := {_, RamRef}, disc := {_, DiscRef}})->
    ok = write(RamRef, [{{?DATA, [update_key]}, old_value}]),
    ok = write(DiscRef, [{{?DATA, [delete_key]}, delete_value}]),

    Write = [
      {#key{type = ram, storage = ?DATA, key = update_key}, new_value},
      {#key{type = ramdisc, storage = ?DATA, key = insert_key}, insert_value}
    ],
    Delete = [
      #key{type = disc, storage = ?DATA, key = delete_key}
    ],

    {RollbackWrite, RollbackDelete} =
      ecomet_db:prepare_rollback(Ref, Write, Delete),

    ExpectedWrite = [
      {#key{type = ram, storage = ?DATA, key = update_key}, old_value},
      {#key{type = disc, storage = ?DATA, key = delete_key}, delete_value}
    ],
    ExpectedDelete = [
      #key{type = ramdisc, storage = ?DATA, key = insert_key}
    ],

    ?assertEqual(lists:sort(ExpectedWrite), lists:sort(RollbackWrite)),
    ?assertEqual(lists:sort(ExpectedDelete), lists:sort(RollbackDelete))
  end).

db_prepare_rollback_routes_boolean_indexes_through_ecomet_index(_Config)->
  with_ref(fun(Ref)->
    AddKey = {{<<"field">>, <<"value">>, simple}, [idl, 1, 0, 1]},
    DelKey = {{<<"field">>, <<"old">>, simple}, [idl, 1, 0, 1]},
    Write = [
      {#key{type = ram, storage = ?INDEX, key = AddKey}, true},
      {#key{type = ram, storage = ?INDEX, key = DelKey}, false}
    ],

    {RollbackWrite, []} = ecomet_db:prepare_rollback(Ref, Write, []),

    Expected = [
      {#key{type = ram, storage = ?INDEX, key = AddKey}, false},
      {#key{type = ram, storage = ?INDEX, key = DelKey}, true}
    ],
    ?assertEqual(lists:sort(Expected), lists:sort(RollbackWrite))
  end).

db_commit_can_apply_prepared_mixed_rollback(_Config)->
  with_ref(fun(Ref = #{ram := {_, RamRef}})->
    DataKey = #key{type = ram, storage = ?DATA, key = item},
    IndexKey = {{<<"field">>, <<"value">>, simple}, [idl, 1, 0, 1]},
    IndexWrite = #key{type = ram, storage = ?INDEX, key = IndexKey},
    ForwardWrite = [
      {DataKey, new_value},
      {IndexWrite, true}
    ],

    {RollbackWrite, RollbackDelete} =
      ecomet_db:prepare_rollback(Ref, ForwardWrite, []),
    ok = ecomet_db:commit(Ref, ForwardWrite, []),
    [{_, new_value}] = read(RamRef, [{?DATA, [item]}]),

    ok = ecomet_db:commit(Ref, RollbackWrite, RollbackDelete),

    [] = read(RamRef, [{?DATA, [item]}]),
    ?assertEqual([], find(RamRef, #{}))
  end).

db_commit_can_apply_prepared_index_only_rollback(_Config)->
  with_ref(fun(Ref = #{ram := {_, RamRef}})->
    IndexKey = {{<<"field">>, <<"value">>, simple}, [idl, 1, 0, 1]},
    IndexWrite = #key{type = ram, storage = ?INDEX, key = IndexKey},
    ForwardWrite = [
      {IndexWrite, true}
    ],

    {RollbackWrite, []} = ecomet_db:prepare_rollback(Ref, ForwardWrite, []),
    ok = ecomet_db:commit(Ref, ForwardWrite, []),
    [_ | _] = find(RamRef, #{}),

    ok = ecomet_db:commit(Ref, RollbackWrite, []),

    ?assertEqual([], find(RamRef, #{}))
  end).

with_ref(Fun)->
  RamRef = create_ref(),
  RamdiscRef = create_ref(),
  DiscRef = create_ref(),
  Ref = #{
    ram => {?MODULE, RamRef},
    ramdisc => {?MODULE, RamdiscRef},
    disc => {?MODULE, DiscRef}
  },
  try Fun(Ref)
  after
    catch close_ref(RamRef),
    catch close_ref(RamdiscRef),
    catch close_ref(DiscRef)
  end.

start_locks()->
  Parent = self(),
  Owner = spawn(fun()->
    {ok, LockPid} = elock:start_link(?LOCKS),
    Parent ! {self(), ready, LockPid},
    receive
      stop ->
        exit(LockPid, shutdown),
        ok
    end
  end),
  receive
    {Owner, ready, LockPid}->
      wait_locks(),
      {Owner, LockPid}
  end.

wait_locks()->
  case ets:info(?LOCKS) of
    undefined ->
      timer:sleep(10),
      wait_locks();
    _ ->
      ok
  end.

create_ref()->
  ets:new(?MODULE, [
    public,
    ordered_set,
    {read_concurrency, true},
    {write_concurrency, true}
  ]).

close_ref(Table)->
  ets:delete(Table),
  ok.

read(Table, Keys)->
  [Rec || K <- Keys, Rec <- ets:lookup(Table, K)].

write(Table, KVs)->
  true = ets:insert(Table, KVs),
  ok.

delete(Table, Keys)->
  [ets:delete(Table, K) || K <- Keys],
  ok.

commit(Table, Write, Delete)->
  ok = write(Table, Write),
  ok = delete(Table, Delete),
  ok.

prepare_rollback(Table, Write, Delete)->
  {WriteRollback0, DeleteRollback} = prepare_write_rollback(Write, Table, {[],[]}),
  WriteRollback = prepare_delete_rollback(Delete, Table, WriteRollback0),
  {WriteRollback, DeleteRollback}.

prepare_write_rollback([{K, V} | Rest], Table, Acc0 = {WriteAcc, DeleteAcc})->
  Acc =
    case ets:lookup(Table, K) of
      [{K, V}] -> Acc0;
      [Rec0] -> {[Rec0 | WriteAcc], DeleteAcc};
      [] -> {WriteAcc, [K | DeleteAcc]}
    end,
  prepare_write_rollback(Rest, Table, Acc);
prepare_write_rollback([], _Table, Acc)->
  Acc.

prepare_delete_rollback([K | Rest], Table, Acc0)->
  Acc =
    case ets:lookup(Table, K) of
      [Rec] -> [Rec | Acc0];
      [] -> Acc0
    end,
  prepare_delete_rollback(Rest, Table, Acc);
prepare_delete_rollback([], _Table, Acc)->
  Acc.

find(Table, #{})->
  [{K, V} || {K, V} <- ets:tab2list(Table)].
