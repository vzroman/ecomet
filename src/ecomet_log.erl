%%% +--------------------------------------------------------------+
%%% | Copyright (c) 2026, Faceplate LTD. All Rights Reserved.      |
%%% | Author: Tokenov Alikhan, alikhantokenov@gmail.com            |
%%% +--------------------------------------------------------------+

-module(ecomet_log).

-include("ecomet.hrl").
-include("ecomet_log.hrl").

%%% +--------------------------------------------------------------+
%%% |                         Service API                          |
%%% +--------------------------------------------------------------+

-export([
  create/1,
  open/1,
  close/1,
  remove/1
]).

%%% +--------------------------------------------------------------+
%%% |                        Transaction API                       |
%%% +--------------------------------------------------------------+

-export([
  rollback_recovery/2,
  rollback_prepare/5,
  rollback/2,
  commit/2
]).

%%% +--------------------------------------------------------------+
%%% |                    Service API Implementation                |
%%% +--------------------------------------------------------------+

% TODO: API DOC
create(#{dir := RootDirectory}) ->
  try
    try_create(RootDirectory)
  catch
    _Class:Error:Stacktrace ->
      ?LOGERROR("failed to create log in directory ~ts, reason: ~p, stacktrace: ~p", [
        RootDirectory,
        Error,
        Stacktrace
      ]),
      throw({
        create_failed,
        #{directory => RootDirectory, error => Error}
      })
  end;
create(InvalidParams) ->
  throw({
    dir_required,
    #{params => InvalidParams}
  }).
  
% TODO: API DOC
open(#{dir := RootDirectory}) ->
  try
    try_open(RootDirectory)
  catch
    _Class:Error:Stacktrace ->
      ?LOGERROR("failed to open log in directory ~ts, reason: ~p, stacktrace: ~p", [
        RootDirectory,
        Error,
        Stacktrace
      ]),
      throw({
        open_failed,
        #{directory => RootDirectory, error => Error}
      })
  end;
open(InvalidParams) ->
  throw({
    dir_required,
    #{params => InvalidParams}
  }).

% TODO: API DOC
close(#log{database = DB, directory = Directory}) ->
  case rocksdb:close(DB) of
    ok ->
      ok;
    {error, Error} ->
      throw({
        close_failed,
        #{error => Error, directory => Directory}
      })
  end.
  
% TODO: API DOC
remove(#{dir := Directory}) ->
 #{
    destroy_attempts := Attempts,
    rocksdb := #{options := Options}
  } = ?ENV(log, undefined),
  try_remove(Directory, Attempts, Options).

%%% +--------------------------------------------------------------+
%%% |                 Transaction API Implementation               |
%%% +--------------------------------------------------------------+

%% Create rollback before applying upcoming commits.
%%   1. For each storage, generate rollback data & index.
%%   2. Store rollback payload in RocksDB under a new TRef so it can be replayed on DB open.
%%   3. Return rollback reference to be used by commit or rollback.
rollback_prepare(
  #{
    log := #log{
      database = DB,
      write = WriteParams
    }
  } = Refs,
  Ordered,
  Write,
  Delete,
  IndexLog
) ->
  RollbackData =
    [begin
      {Module, StorageRef} = maps:get(StorageType, Refs),
      StorageData = maps:get(StorageType, Write, none),
      StorageDelete = maps:get(StorageType, Delete, none),
      {StorageType, prepare_rollback_data(Module, StorageRef, StorageData, StorageDelete)}
     end || StorageType <- Ordered],
  RollbackIndex = prepare_rollback_index(IndexLog),
  TRef = ?ENCODE_KEY(make_ref()),
  ok = rocksdb:write(DB, [{put, TRef, ?ENCODE_VALUE(RollbackData)}], WriteParams),
  #rollback{
    ref = TRef,
    index = RollbackIndex
  }.
  
%% Recovery from log when DB opened.
%%   1. Scan all rollback entries in RocksDB Log.
%%   2. Re-apply each storage rollback (no index log available during recovery).
%%   3. Delete the rollback entry after it is successfully executed.
rollback_recovery(
  #log{
    database = DB,
    read = ReadParams,
    write = WriteParams
  },
  Refs
) ->
  rocksdb:fold(
    DB,
    fun({TRef, Rollback}, _Acc)->
      [begin
        {Module, StorageRef} = maps:get(StorageType, Refs, undefined),
        commit_single_storage(StorageRef, Module, StorageRollback, _IndexLog = none)
       end || {StorageType, StorageRollback} <- ?DECODE_VALUE(Rollback)],
      ok = rocksdb:write(DB, [{delete, TRef}], WriteParams),
      ok
    end,
    ok,
    ReadParams
  ).

%% Rollback a previously stored transaction.
%%   1. Load rollback data from RocksDB using TRef
%%   2. For each storage, apply its rollback via storage commit
%%   3. Remove the rollback entry from RocksDB once finished
%%   4. If no rollback data exists, do nothing
rollback(
  #{
    log := #log{
      database = DB,
      read = ReadParams,
      write = WriteParams
    }
  } = Refs,
  #rollback{ref = TRef, index = IndexLog}
) ->
  case rocksdb:get(DB, TRef, ReadParams) of
    {ok, Rollback} ->
      [begin
        {Module, StorageRef} = maps:get(StorageType, Refs, undefined),
        StorageIndexLog = maps:get(StorageType, IndexLog),
        commit_single_storage(StorageRef, Module, StorageRollback, StorageIndexLog)
       end || {StorageType, StorageRollback} <- ?DECODE_VALUE(Rollback)],
      ok = rocksdb:write(DB, [{delete, TRef}], WriteParams);
    _Ignore ->
      ok
  end.

%% Finalize commit by removing rollback entry.
%% If all storages committed successfully.
commit(
  #{
    log := #log{
      database = Log,
      write = Write
    }
  },
  #rollback{
    ref = TRef
  }
) ->
  ok = rocksdb:write(Log, [{delete, TRef}], Write).

%%% +--------------------------------------------------------------+
%%% |                      Internal functions                      |
%%% +--------------------------------------------------------------+

try_create(RootDirectory) ->
  #{
    rocksdb := #{
      options := Options,
      read := Read,
      write := Write
    }
  } = ?ENV(log, undefined),
  ensure_dir(?LOG_DIRECTORY(RootDirectory)),
  Reference =
    #log{
      directory = RootDirectory,
      database = open_database(?LOG_DIRECTORY(RootDirectory), Options),
      read = maps:to_list(Read),
      write = maps:to_list(Write)
    },
  #{log => {?MODULE, Reference}}.

try_open(RootDirectory) ->
  #{
    rocksdb := #{
      options := Options,
      read := Read,
      write := Write
    }
  } = ?ENV(log, undefined),
  LogDirectory = ?LOG_DIRECTORY(RootDirectory),
  case filelib:is_dir(LogDirectory) of
    true ->
      ok;
    false ->
      ?LOGERROR("folder ~s not found or not a directory", [LogDirectory]),
      throw({directory_not_exists, #{directory => LogDirectory}})
  end,
  Reference =
    #log{
      directory = RootDirectory,
      database = open_database(LogDirectory, Options),
      read = maps:to_list(Read),
      write = maps:to_list(Write)
    },
  % TODO. rollback_log(Reference),
  #{log => {?MODULE, Reference}}.

try_remove(Directory, Attempts, Options) when Attempts > 0 ->
  try
    remove_recursive(Directory)
  catch
    _Class:Error:Stacktrace ->
      ?LOGERROR("attempt to remove ~s failed, error: ~p, stack: ~p", [
        Directory,
        Error,
        Stacktrace
      ]),
      try_remove(Directory, Attempts - 1, Options)
  end;
try_remove(Directory, 0, Options) ->
  ?LOGERROR("failed to remove directory: ~p, options: ~p", [Directory, Options]),
  throw({
    remove_failed,
    #{directory => Directory, options => Options}
  }).

open_database(Directory, Options) ->
  case rocksdb:open(Directory, maps:to_list(Options)) of
    {ok, Ref} ->
      Ref;
    {error, {db_open, Error}} ->
      case lists:prefix("IO error: lock ", Error) of
        true ->
          case file:delete(?LOCK(Directory)) of
            ok ->
              timer:sleep(?RETRY_TIMEOUT),
              open_database(Directory, Options);
            {error, UnlockError} ->
              ?LOGERROR("~s lock remove error ~p, try to remove it manually", [?LOCK(Directory), UnlockError]),
              throw({
                directory_locked,
                #{directory => Directory, error => UnlockError}
              })
          end;
        false ->
          try rocksdb:repair(Directory, [])
          catch
            _Class:RepError:RepStack ->
              ?LOGWARNING("failed to repair ~s, error: ~p stack: ~p", [
                Directory,
                RepError,
                RepStack
              ])
          end,
          timer:sleep(?RETRY_TIMEOUT),
          open_database(Directory, Options)
      end;
    {error, Error} ->
      ?LOGERROR("failed to open directory: ~s, error: ~p", [Directory, Error]),
      throw({
        database_open_failed,
        #{error => Error, directory => Directory}
      })
  end.

ensure_dir(Path) ->
  case filelib:is_file(Path) of
    false ->
      case filelib:ensure_dir(Path ++ "/") of
        ok -> ok;
        {error, CreateError} ->
          ?LOGERROR("~s create error ~p", [Path, CreateError]),
          throw({create_dir_error, CreateError})
      end;
    true ->
      remove_recursive(Path),
      ensure_dir(Path)
  end.

remove_recursive(Path) ->
  case filelib:is_dir(Path) of
    false ->
      case filelib:is_file(Path) of
        true ->
          case file:delete(Path) of
            ok -> ok;
            {error, DelError} ->
              ?LOGERROR("~s delete error ~p", [Path, DelError]),
              throw({delete_error, DelError})
          end;
        _ ->
          ok
      end;
    true ->
      case file:list_dir_all(Path) of
        {ok, Files} ->
          [remove_recursive(Path ++ "/" ++ F) || F <- Files],
          case file:del_dir(Path) of
            ok ->
              ok;
            {error, DelError} ->
              ?LOGERROR("~s delete error ~p", [Path, DelError]),
              throw({delete_error, DelError})
          end
      end
  end.

prepare_rollback_index(IndexLog) ->
  maps:fold(
    fun(Storage, Indexes, Acc) ->
      InverseIndexes =
        [begin
           NewValue =
             case Value of
               true -> false;
               false -> true
             end,
           {Key, NewValue}
         end || {Key, Value} <- Indexes],
      Acc#{Storage => InverseIndexes}
    end,
    #{},
    IndexLog
  ).

prepare_rollback_data(Module, StorageRef, Write, Delete) ->
  WriteKeys = [K || {K, _} <- Write],
  Keys = lists:usort(WriteKeys ++ Delete),
  
  % Read old values from storage, preserves the key order
  % Returns: [{Key1, Value1}, ..., {KeyN, ValueN}]
  ReadData = maps:from_list(Module:read(StorageRef, Keys)),
  
  % Undo for 'write' / 'put' operation
  UndoWrite =
    lists:foldl(
      fun({Key, Value}, Acc) ->
        case ReadData of
          #{Key := Value} ->
            Acc;
          #{Key := ReadValue} ->
            [{put, ?ENCODE_KEY(Key), ?ENCODE_VALUE(ReadValue)} | Acc];
          _Other ->
            [{delete, ?ENCODE_KEY(Key)}]
        end
      end,
      [],
      Write
    ),
  
  % Undo for 'delete' operation
  UndoDelete =
    lists:foldl(
      fun(Key, Acc) ->
        case maps:find(Key, ReadData) of
          #{Key := ReadValue} ->
            [{put, ?ENCODE_KEY(Key), ?ENCODE_VALUE(ReadValue)} | Acc];
          _Other ->
            Acc
        end
      end,
      UndoWrite,
      Delete
    ),
    
  UndoDelete.
  
% TODO. This is temporary placement of the function to test it. Move it to ecomet_db.
commit_single_storage(Ref, Module, Data, _IndexLog = none) ->
  Module:write(Ref, Data);
commit_single_storage(Ref, Module, _Data = none, IndexLog) ->
  {ok, Unlock} = elock:lock(?LOCKS, Ref, _IsShared = false, _Timeout = infinity),
  try
    case ecomet_index:prepare_write(Module, Ref, IndexLog) of
      {IndexWrite, IndexDel} when length(IndexWrite) > 0 ->
        Module:commit(Ref, IndexWrite, IndexDel);
      {_IndexWrite, IndexDel} ->
        Module:delete(Ref, IndexDel)
    end
  after
    Unlock()
  end;
commit_single_storage(Ref, Module, Data, IndexLog) ->
  {ok, Unlock} = elock:lock(?LOCKS, Ref, _IsShared = false, _Timeout = infinity),
  try
    {IndexWrite, IndexDel} = ecomet_index:prepare_write(Module, Ref, IndexLog),
    if
      length(IndexDel) =:= 0 ->
        Module:write(Ref, Data ++ IndexWrite);
      length(IndexWrite) =:= 0 ->
        Module:commit(Ref, Data, IndexDel);
      true ->
        Module:commit(Ref, Data ++ IndexWrite, IndexDel)
    end
  after
    Unlock()
  end.