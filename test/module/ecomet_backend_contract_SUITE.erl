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
-record(ref, {name, storages, tlog = undefined}).
-record(commit, {write, delete, index}).

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
  db_commit_can_apply_prepared_delete_with_index_rollback/1,
  db_commit_logs_original_ops_before_protected_storage_commit/1,
  db_commit_skips_log_for_single_storage_commit/1,
  db_open_replays_pending_tlog_entries/1
]).

%% Test backend API
-export([
  read/2,
  write/2,
  delete/2,
  commit/3,
  prepare_rollback/3,
  find/2,
  create/1,
  open/1,
  close/1,
  remove/1,
  is_persistent/0
]).

all()->
  [
    index_prepare_rollback_inverts_boolean_updates,
    db_prepare_rollback_returns_public_keys_for_data,
    db_prepare_rollback_routes_boolean_indexes_through_ecomet_index,
    db_commit_can_apply_prepared_mixed_rollback,
    db_commit_can_apply_prepared_delete_with_index_rollback,
    db_commit_logs_original_ops_before_protected_storage_commit,
    db_commit_skips_log_for_single_storage_commit,
    db_open_replays_pending_tlog_entries
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
  with_ref(fun(Ref = #ref{storages = #{ram := {_, RamRef}, disc := {_, DiscRef}}})->
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
    DataKey = #key{type = ram, storage = ?DATA, key = item},
    AddKey = {{<<"field">>, <<"value">>, simple}, [idl, 1, 0, 1]},
    DelKey = {{<<"field">>, <<"old">>, simple}, [idl, 1, 0, 1]},
    Write = [
      {DataKey, new_value},
      {#key{type = ram, storage = ?INDEX, key = AddKey}, true},
      {#key{type = ram, storage = ?INDEX, key = DelKey}, false}
    ],

    {RollbackWrite, RollbackDelete} = ecomet_db:prepare_rollback(Ref, Write, []),

    Expected = [
      {#key{type = ram, storage = ?INDEX, key = AddKey}, false},
      {#key{type = ram, storage = ?INDEX, key = DelKey}, true}
    ],
    ?assertEqual(lists:sort(Expected), lists:sort(RollbackWrite)),
    ?assertEqual([DataKey], RollbackDelete)
  end).

db_commit_can_apply_prepared_mixed_rollback(_Config)->
  with_ref(fun(Ref = #ref{storages = #{ram := {_, RamRef}}})->
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

db_commit_can_apply_prepared_delete_with_index_rollback(_Config)->
  with_ref(fun(Ref = #ref{storages = #{ram := {_, RamRef}}})->
    DataKey = #key{type = ram, storage = ?DATA, key = item},
    IndexKey = {{<<"field">>, <<"value">>, simple}, [idl, 1, 0, 1]},
    IndexWrite = #key{type = ram, storage = ?INDEX, key = IndexKey},
    ok = ecomet_db:commit(Ref, [
      {DataKey, old_value},
      {IndexWrite, true}
    ], []),
    [{_, old_value}] = read(RamRef, [{?DATA, [item]}]),
    [_ | _] = find(RamRef, #{}),

    ForwardDelete = [DataKey],
    ForwardWrite = [
      {IndexWrite, false}
    ],

    {RollbackWrite, RollbackDelete} =
      ecomet_db:prepare_rollback(Ref, ForwardWrite, ForwardDelete),
    ok = ecomet_db:commit(Ref, ForwardWrite, ForwardDelete),
    [] = read(RamRef, [{?DATA, [item]}]),
    ?assertEqual([], find(RamRef, #{})),

    ok = ecomet_db:commit(Ref, RollbackWrite, RollbackDelete),

    [{_, old_value}] = read(RamRef, [{?DATA, [item]}]),
    [_ | _] = find(RamRef, #{})
  end).

db_commit_logs_original_ops_before_protected_storage_commit(_Config)->
  with_ref(fun(#ref{storages = Storages})->
    with_tlog_mock(fun()->
      trace_commits(self()),
      Ref = #ref{name = tlog_test, storages = Storages, tlog = tlog_ref},
      Write = [
        {#key{type = ramdisc, storage = ?DATA, key = ramdisc_item}, ramdisc_value},
        {#key{type = disc, storage = ?DATA, key = disc_item}, disc_value}
      ],

      ok = ecomet_db:commit(Ref, Write, []),

      TRef = receive
        {tlog_add, tlog_ref, #commit{
          write = #{
            ramdisc := [{{?DATA, [ramdisc_item]}, ramdisc_value}],
            disc := [{{?DATA, [disc_item]}, disc_value}]
          },
          delete = #{},
          index = #{}
        }} ->
          tlog_entry_ref
      after 1000 ->
        ct:fail(tlog_write_missing)
      end,
      receive
        {storage_commit, ramdisc} ->
          ok
      after 1000 ->
        ct:fail(ramdisc_commit_missing)
      end,
      receive
        {storage_commit, disc} ->
          ok
      after 1000 ->
        ct:fail(disc_commit_missing)
      end,
      receive
        {tlog_delete, tlog_ref, TRef} ->
          ok
      after 1000 ->
        ct:fail(tlog_delete_missing)
      end
    end)
  end).

db_commit_skips_log_for_single_storage_commit(_Config)->
  with_ref(fun(#ref{storages = Storages})->
    with_tlog_mock(fun()->
      Ref = #ref{name = tlog_test, storages = Storages, tlog = tlog_ref},
      Write = [
        {#key{type = ramdisc, storage = ?DATA, key = ramdisc_item}, ramdisc_value}
      ],

      ok = ecomet_db:commit(Ref, Write, []),

      receive
        {tlog_add, _TLog, _Commit} ->
          ct:fail(unexpected_tlog_write)
      after 100 ->
        ok
      end
    end)
  end).

db_open_replays_pending_tlog_entries(Config)->
  Dir = filename:join(?config(priv_dir, Config), "db-open-replay"),
  Params = db_params(replay_test, Dir),

  with_tlog_mock(fun()->
    meck:expect(ecomet_tlog, open, fun(DBDir) ->
      ?assertEqual(Dir, DBDir),
      tlog_ref
    end),
    meck:expect(ecomet_tlog, replay, fun(tlog_ref, Callback) ->
      ok = Callback(#commit{
        write = #{
          ramdisc => [{{?DATA, [replay_ramdisc]}, ramdisc_value}],
          disc => [{{?DATA, [replay_disc]}, disc_value}]
        },
        delete = #{},
        index = #{}
      })
    end),

    Ref = ecomet_db:open(Params),

    ?assertEqual(
      [{#key{type = ramdisc, storage = ?DATA, key = replay_ramdisc}, ramdisc_value}],
      ecomet_db:read(Ref, [#key{type = ramdisc, storage = ?DATA, key = replay_ramdisc}])
    ),
    ?assertEqual(
      [{#key{type = disc, storage = ?DATA, key = replay_disc}, disc_value}],
      ecomet_db:read(Ref, [#key{type = disc, storage = ?DATA, key = replay_disc}])
    ),
    ok = ecomet_db:close(Ref)
  end).

with_ref(Fun)->
  RamRef = create_ref(),
  RamdiscRef = create_ref(),
  DiscRef = create_ref(),
  persistent_term:put({?MODULE, type, RamRef}, ram),
  persistent_term:put({?MODULE, type, RamdiscRef}, ramdisc),
  persistent_term:put({?MODULE, type, DiscRef}, disc),
  Ref = #{
    ram => {?MODULE, RamRef},
    ramdisc => {?MODULE, RamdiscRef},
    disc => {?MODULE, DiscRef}
  },
  try
    Fun(#ref{
      name = backend_contract_test,
      storages = Ref,
      tlog = undefined
    })
  after
    catch close_ref(RamRef),
    catch close_ref(RamdiscRef),
    catch close_ref(DiscRef)
  end.

with_tlog_mock(Fun)->
  meck:new(ecomet_tlog, [non_strict]),
  meck:expect(ecomet_tlog, add, fun(TLog, Commit) ->
    self() ! {tlog_add, TLog, Commit},
    tlog_entry_ref
  end),
  meck:expect(ecomet_tlog, delete, fun(TLog, TRef) ->
    self() ! {tlog_delete, TLog, TRef},
    ok
  end),
  meck:expect(ecomet_tlog, close, fun(tlog_ref) ->
    ok
  end),
  try Fun()
  after
    meck:unload(ecomet_tlog),
    trace_commits(undefined)
  end.

trace_commits(Pid)->
  persistent_term:put({?MODULE, trace_commits}, Pid),
  ok.

db_params(Name, Dir)->
  #{
    name => Name,
    dir => Dir,
    ramdisc => #{
      module => ?MODULE,
      params => #{}
    },
    disc => #{
      module => ?MODULE,
      params => #{}
    }
  }.

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
  persistent_term:erase({?MODULE, type, Table}),
  ets:delete(Table),
  ok.

read(Table, Keys)->
  [Rec || K <- Keys, Rec <- ets:lookup(Table, K)].

write(Table, KVs)->
  trace_storage_commit(Table),
  true = ets:insert(Table, KVs),
  ok.

delete(Table, Keys)->
  trace_storage_commit(Table),
  [ets:delete(Table, K) || K <- Keys],
  ok.

commit(Table, Write, Delete)->
  trace_storage_commit(Table),
  true = ets:insert(Table, Write),
  [ets:delete(Table, K) || K <- Delete],
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

create(Params)->
  open(Params).

open(Params)->
  Ref = create_ref(),
  persistent_term:put({?MODULE, type, Ref}, storage_type(Params)),
  Ref.

close(Ref)->
  close_ref(Ref).

remove(_Params)->
  ok.

is_persistent()->
  true.

storage_type(#{dir := Dir})->
  list_to_atom(filename:basename(Dir)).

trace_storage_commit(Table)->
  case persistent_term:get({?MODULE, trace_commits}, undefined) of
    undefined ->
      ok;
    Pid ->
      Pid ! {storage_commit, persistent_term:get({?MODULE, type, Table}, undefined)}
  end.
