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
-module(ecomet_index_cleanup).

-include("ecomet.hrl").

-export([
  cleanup_stale_oids/0,
  cleanup_stale_oids/1
]).

-record(key,{type,storage,key}).
-record(bitmap_acc,{
  stale_idls = [],
  existence_checks = 0,
  stale_oids = 0,
  removed_refs = 0,
  sample_live_oid = none
}).
-record(db_acc,{
  index_keys_scanned = 0,
  oid_refs_scanned = 0,
  existence_checks = 0,
  stale_oids = 0,
  removed_refs = 0,
  updated_index_keys = 0,
  write_batches = 0,
  sample_oids = #{},
  pending_ops = []
}).

-define(WRITE_BATCH_SIZE, 1000).

cleanup_stale_oids()->
  cleanup_stale_oids(ecomet_db:get_databases()).

cleanup_stale_oids(DB) when is_atom(DB)->
  cleanup_stale_oids([DB]);
cleanup_stale_oids(DBs) when is_list(DBs)->
  UniqueDBs = lists:usort(DBs),
  ?LOGINFO("start stale index cleanup for ~p db(s): ~p",[length(UniqueDBs), UniqueDBs]),
  Reports =
    lists:foldl(fun(DB, Acc)->
      Acc#{ DB => cleanup_db(DB) }
    end, #{}, UniqueDBs),
  Totals = build_totals(Reports),
  ?LOGINFO("finish stale index cleanup totals ~p",[Totals]),
  #{
    dbs => Reports,
    totals => Totals
  }.

cleanup_db(DB)->
  case ecomet_db:is_available(DB) of
    false->
      ?LOGWARNING("skip stale index cleanup for ~p: database is not available",[DB]),
      #{status => skipped, reason => not_available};
    true->
      ?LOGINFO("start stale index cleanup for db ~p",[DB]),
      Cache = ets:new(?MODULE, [set, private]),
      try
        ScanAcc =
          zaya:foldl(DB, index_query(), fun(Entry, Acc)->
            scan_index_entry(DB, Cache, Entry, Acc)
          end, #db_acc{}),
        Report = db_acc_to_map(flush_pending_ops(DB, ScanAcc)),
        ?LOGINFO("finish stale index cleanup for db ~p: ~p",[DB, Report]),
        Report
      catch
        Class:Reason:Stacktrace->
          ?LOGERROR("stale index cleanup failed for ~p: ~p:~p ~p",[DB, Class, Reason, Stacktrace]),
          #{status => error, reason => {Class, Reason}}
      after
        ets:delete(Cache)
      end
  end.

index_query()->
  #{
    ms => [
      {
        {{?INDEX,[{'$1',[idl,'$2','$3']}]}, '$4'},
        [],
        ['$_']
      }
    ]
  }.

scan_index_entry(DB, Cache, {#key{type = Storage, storage = ?INDEX, key = {Tag,[idl,PatternID,IDH]}}, Bitmap}, Acc0)->
  {OIDRefsScanned, BitmapAcc} = scan_bitmap(Cache, PatternID, IDH, Bitmap),
  #bitmap_acc{
    stale_idls = StaleIDLs,
    existence_checks = ExistenceChecks,
    stale_oids = StaleOIDs,
    removed_refs = RemovedRefs,
    sample_live_oid = SampleLiveOID
  } = BitmapAcc,
  Acc1 = Acc0#db_acc{
    index_keys_scanned = Acc0#db_acc.index_keys_scanned + 1,
    oid_refs_scanned = Acc0#db_acc.oid_refs_scanned + OIDRefsScanned,
    existence_checks = Acc0#db_acc.existence_checks + ExistenceChecks,
    stale_oids = Acc0#db_acc.stale_oids + StaleOIDs,
    removed_refs = Acc0#db_acc.removed_refs + RemovedRefs,
    sample_oids = remember_sample_oid(Storage, SampleLiveOID, Acc0#db_acc.sample_oids)
  },
  case StaleIDLs of
    []->
      maybe_log_scan_progress(DB, Acc1);
    _->
      maybe_log_scan_progress(DB, Acc1#db_acc{
        updated_index_keys = Acc1#db_acc.updated_index_keys + 1,
        pending_ops = build_delete_ops(Storage, Tag, PatternID, IDH, StaleIDLs) ++ Acc1#db_acc.pending_ops
      })
  end.

scan_bitmap(Cache, PatternID, IDH, Bitmap)->
  ecomet_bitmap:foldl(fun(IDL, Acc)->
    scan_oid(Cache, PatternID, IDH, IDL, Acc)
  end, #bitmap_acc{}, Bitmap, {none, none}).

scan_oid(Cache, PatternID, IDH, IDL, Acc)->
  OID = {PatternID, IDH * ?BITSTRING_LENGTH + IDL},
  case cached_exists(Cache, OID) of
    {true, true}->
      maybe_set_sample_live_oid(OID, Acc#bitmap_acc{
        existence_checks = Acc#bitmap_acc.existence_checks + 1
      });
    {true, false}->
      maybe_set_sample_live_oid(OID, Acc);
    {false, true}->
      Acc#bitmap_acc{
        stale_idls = [IDL | Acc#bitmap_acc.stale_idls],
        existence_checks = Acc#bitmap_acc.existence_checks + 1,
        stale_oids = Acc#bitmap_acc.stale_oids + 1,
        removed_refs = Acc#bitmap_acc.removed_refs + 1
      };
    {false, false}->
      Acc#bitmap_acc{
        stale_idls = [IDL | Acc#bitmap_acc.stale_idls],
        removed_refs = Acc#bitmap_acc.removed_refs + 1
      }
  end.

cached_exists(Cache, OID)->
  case ets:lookup(Cache, OID) of
    [{OID, Exists}]->
      {Exists, false};
    []->
      Exists = ecomet_object:exists(OID),
      true = ets:insert(Cache, {OID, Exists}),
      {Exists, true}
  end.

maybe_set_sample_live_oid(OID, #bitmap_acc{sample_live_oid = none} = Acc)->
  Acc#bitmap_acc{sample_live_oid = OID};
maybe_set_sample_live_oid(_OID, Acc)->
  Acc.

remember_sample_oid(_Storage, none, SampleOIDs)->
  SampleOIDs;
remember_sample_oid(Storage, OID, SampleOIDs)->
  case maps:is_key(Storage, SampleOIDs) of
    true-> SampleOIDs;
    false-> SampleOIDs#{ Storage => OID }
  end.

build_delete_ops(Storage, Tag, PatternID, IDH, IDLs)->
  [{#key{type = Storage, storage = ?INDEX, key = {Tag,[idl,PatternID,IDH,IDL]}}, false} || IDL <- IDLs].

flush_pending_ops(_DB, #db_acc{pending_ops = []} = Acc)->
  Acc;
flush_pending_ops(DB, #db_acc{pending_ops = PendingOps, sample_oids = SampleOIDs} = Acc0)->
  Batches = batch_ops(lists:reverse(PendingOps), ?WRITE_BATCH_SIZE),
  TotalBatches = length(Batches),
  TotalOps = length(PendingOps),
  ?LOGINFO("apply ~p stale index cleanup batch(es) for db ~p, removing ~p ref(s)",[TotalBatches, DB, TotalOps]),
  lists:foldl(fun({BatchNumber, Batch}, Acc)->
    ok = write_cleanup_batch(DB, Batch, SampleOIDs),
    ?LOGINFO("stale index cleanup db ~p batch ~p/~p applied (~p ref(s))",[DB, BatchNumber, TotalBatches, length(Batch)]),
    Acc#db_acc{write_batches = Acc#db_acc.write_batches + 1}
  end, Acc0#db_acc{pending_ops = []}, lists:zip(lists:seq(1, TotalBatches), Batches)).

write_cleanup_batch(DB, Batch, SampleOIDs)->
  ByType = group_batch_by_type(Batch),
  case ecomet_db:transaction(fun()->
    maps:foreach(fun(Type, KVs)->
      {SampleOID, SampleData} = get_sample_data(DB, Type, SampleOIDs),
      ok = ecomet_db:write(DB, ?DATA, Type, SampleOID, SampleData, none),
      ok = ecomet_db:bulk_write(DB, ?INDEX, Type, KVs, none)
    end, ByType)
  end) of
    {ok, _}-> ok;
    {abort, Reason}-> throw(Reason)
  end.

group_batch_by_type(Batch)->
  maps:map(fun(_Type, KVs)->
    lists:reverse(KVs)
  end, lists:foldl(fun({#key{type = Type, storage = ?INDEX, key = Key}, Value}, Acc)->
    TypeAcc = maps:get(Type, Acc, []),
    Acc#{ Type => [{Key, Value} | TypeAcc] }
  end, #{}, Batch)).

get_sample_data(DB, Type, SampleOIDs)->
  case maps:get(Type, SampleOIDs, undefined) of
    undefined->
      find_sample_data(DB, Type);
    OID->
      case ecomet_db:read(DB, ?DATA, Type, OID) of
        not_found-> find_sample_data(DB, Type);
        Data->{OID, Data}
      end
  end.

find_sample_data(DB, Type)->
  case zaya:find(DB, #{
    types => [Type],
    limit => 1,
    ms => [
      {
        {{?DATA,['$1']}, '$2'},
        [],
        ['$_']
      }
    ]
  }) of
    [{#key{type = Type, storage = ?DATA, key = OID}, Data} | _]->
      {OID, Data};
    []->
      throw({no_sample_data_for_index_cleanup, DB, Type})
  end.

batch_ops([], _Size)->
  [];
batch_ops(Ops, Size)->
  batch_ops(Ops, Size, []).

batch_ops([], _Size, Acc)->
  lists:reverse(Acc);
batch_ops(Ops, Size, Acc)->
  {Batch, Rest} = take_batch(Size, Ops, []),
  batch_ops(Rest, Size, [lists:reverse(Batch) | Acc]).

take_batch(0, Rest, Acc)->
  {Acc, Rest};
take_batch(_Size, [], Acc)->
  {Acc, []};
take_batch(Size, [Op | Rest], Acc)->
  take_batch(Size - 1, Rest, [Op | Acc]).

maybe_log_scan_progress(DB, #db_acc{
  index_keys_scanned = IndexKeysScanned,
  oid_refs_scanned = OIDRefsScanned,
  stale_oids = StaleOIDs,
  removed_refs = RemovedRefs,
  updated_index_keys = UpdatedIndexKeys
} = Acc)->
  Step = progress_log_step(),
  if
    Step > 0, IndexKeysScanned > 0, IndexKeysScanned rem Step =:= 0 ->
      ?LOGINFO(
        "stale index cleanup db ~p progress: ~p index key(s), ~p oid ref(s), ~p stale oid(s), ~p removed ref(s), ~p updated key(s)",
        [DB, IndexKeysScanned, OIDRefsScanned, StaleOIDs, RemovedRefs, UpdatedIndexKeys]
      ),
      Acc;
    true->
      Acc
  end.

progress_log_step()->
  ?ENV(index_cleanup_progress_step, 10000).

db_acc_to_map(#db_acc{
  index_keys_scanned = IndexKeysScanned,
  oid_refs_scanned = OIDRefsScanned,
  existence_checks = ExistenceChecks,
  stale_oids = StaleOIDs,
  removed_refs = RemovedRefs,
  updated_index_keys = UpdatedIndexKeys,
  write_batches = WriteBatches
})->
  #{
    status => ok,
    index_keys_scanned => IndexKeysScanned,
    oid_refs_scanned => OIDRefsScanned,
    existence_checks => ExistenceChecks,
    stale_oids => StaleOIDs,
    removed_refs => RemovedRefs,
    updated_index_keys => UpdatedIndexKeys,
    write_batches => WriteBatches
  }.

build_totals(Reports)->
  maps:fold(fun(_DB, Report, Acc)->
    case Report of
      #{status := ok}->
        Acc#{
          processed_dbs => maps:get(processed_dbs, Acc) + 1,
          index_keys_scanned => maps:get(index_keys_scanned, Acc) + maps:get(index_keys_scanned, Report),
          oid_refs_scanned => maps:get(oid_refs_scanned, Acc) + maps:get(oid_refs_scanned, Report),
          existence_checks => maps:get(existence_checks, Acc) + maps:get(existence_checks, Report),
          stale_oids => maps:get(stale_oids, Acc) + maps:get(stale_oids, Report),
          removed_refs => maps:get(removed_refs, Acc) + maps:get(removed_refs, Report),
          updated_index_keys => maps:get(updated_index_keys, Acc) + maps:get(updated_index_keys, Report),
          write_batches => maps:get(write_batches, Acc) + maps:get(write_batches, Report)
        };
      #{status := skipped}->
        Acc#{ skipped_dbs => maps:get(skipped_dbs, Acc) + 1 };
      #{status := error}->
        Acc#{ errored_dbs => maps:get(errored_dbs, Acc) + 1 }
    end
  end, #{
    processed_dbs => 0,
    skipped_dbs => 0,
    errored_dbs => 0,
    index_keys_scanned => 0,
    oid_refs_scanned => 0,
    existence_checks => 0,
    stale_oids => 0,
    removed_refs => 0,
    updated_index_keys => 0,
    write_batches => 0
  }, Reports).
