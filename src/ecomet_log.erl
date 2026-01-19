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
  rollback_recovery/1,
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
      ?LOGERROR("failed to create log in directory ~ts, error: ~p, stacktrace: ~p", [
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
      ?LOGERROR("failed to open log in directory ~ts, error: ~p, stacktrace: ~p", [
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
      ?LOGERROR("failed to close log in directory: ~p, error: ~p", [
        Directory,
        Error
      ]),
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
  Refs,
  Storages,
  Write,
  Delete,
  IndexLog
) ->
  Log = get_ref(Refs),
  RollbackData = [prepare_rollback_data(StorageType, Refs, Write, Delete) || StorageType <- Storages],
  RollbackIndex = prepare_rollback_index(IndexLog),
  TRef = ?ENCODE_KEY(make_ref()),
  log_write(Log, [{put, TRef, ?ENCODE_VALUE(RollbackData)}]),
  #rollback{
    ref = TRef,
    index = RollbackIndex
  }.
  
%% Recovery from log when DB opened.
%%   1. Scan all rollback entries in RocksDB Log.
%%   2. Re-apply each storage rollback (no index log available during recovery).
%%   3. Delete the rollback entry after it is successfully executed.
rollback_recovery(Refs) ->
  #log{database = DB, read = ReadParams} = Log = get_ref(Refs),
  rocksdb:fold(
    DB,
    fun({TRef, Rollback}, _Acc)->
      [begin
        {Module, StorageRef} = maps:get(Type, Refs),
        ecomet_db:commit(StorageRef, Module, Write, Delete, _Index = none)
       end || #storage_rollback{type = Type, write = Write, delete = Delete} <- ?DECODE_VALUE(Rollback)],
      log_write(Log, [{delete, TRef}]),
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
  Refs,
  #rollback{ref = TRef, index = IndexLog}
) ->
  Log = get_ref(Refs),
  case log_get(Log, TRef) of
    {ok, Rollback} ->
      [begin
        {Module, StorageRef} = maps:get(Type, Refs),
        Index = maps:get(Type, IndexLog, none),
        ecomet_db:commit(StorageRef, Module, Write, Delete, Index)
       end || #storage_rollback{type = Type, write = Write, delete = Delete} <- ?DECODE_VALUE(Rollback)],
      log_write(Log, [{delete, TRef}]);
    _Ignore ->
      ok
  end.

%% Finalize commit by removing rollback entry.
%% If all storages committed successfully.
commit(
  Refs,
  #rollback{ref = TRef}
) ->
  Log = get_ref(Refs),
  log_write(Log, [{delete, TRef}]).

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

log_get(
  #log{
    directory = Directory,
    database = DB,
    read = ReadParams
  },
  TRef
) ->
  try
    case rocksdb:get(DB, TRef, ReadParams) of
      {ok, Result} ->
        {ok, Result};
      _Ignore ->
        ok
    end
  catch
    _:Error ->
      throw({
        log_read_failed,
        #{error => Error, directory => Directory}
      })
  end.

log_write(
  #log{
    directory = Directory,
    database = DB,
    write = Write
  },
  Data
) ->
  try
    ok = rocksdb:write(DB, Data, Write)
  catch
    _:Error ->
      throw({
        log_write_failed,
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

% Format of write: [{K1, V1}, ..., {KN, VN}]
% Format of delete: [K1, ..., KN]
prepare_rollback_data(StorageType, Refs, Write, Delete) ->
  {Module, StorageRef} = maps:get(StorageType, Refs),
  StorageWrite = maps:get(StorageType, Write, []),
  StorageDelete = maps:get(StorageType, Delete, []),
  
  WriteKeys = [K || {K, _} <- StorageWrite],
  Keys = lists:usort(WriteKeys ++ StorageDelete),
  
  % Read old values from storage, preserves the key order
  % Returns: [{Key1, Value1}, ..., {KeyN, ValueN}]
  ReadData = maps:from_list(Module:read(StorageRef, Keys)),
  
  % Undo for 'write' operation
  UndoWrite =
    lists:foldl(
      fun({Key, Value}, Acc) ->
        case ReadData of
          #{Key := Value} ->
            Acc;
          #{Key := ReadValue} ->
            [{Key, ReadValue} | Acc];
          _Other ->
            [Key | Acc]
        end
      end,
      [],
      StorageWrite
    ),
  
  % Undo for 'delete' operation
  UndoDelete =
    lists:foldl(
      fun(Key, Acc) ->
        case ReadData of
          #{Key := ReadValue} ->
            [{Key, ReadValue} | Acc];
          _Other ->
            Acc
        end
      end,
      UndoWrite,
      StorageDelete
    ),
  
  {Writes, Deletes} =
    lists:partition(
      fun
        ({_Key, _Value}) ->
          true;
        (_) ->
          false
      end,
      UndoDelete
    ),

  #storage_rollback{
    type = StorageType,
    write = Writes,
    delete = Deletes
  }.

get_ref(#{log := {?MODULE, Log}}) ->
  Log.