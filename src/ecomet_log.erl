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

create(#{dir := RootDirectory}) ->
  try_create(RootDirectory);
create(InvalidParams) ->
  throw({
    dir_required,
    #{params => InvalidParams}
  }).

open(#{dir := RootDirectory}) ->
  try_open(RootDirectory);
open(InvalidParams) ->
  throw({
    dir_required,
    #{params => InvalidParams}
  }).

close(#log{} = Log) ->
  try_close(Log);
close(InvalidArg) ->
  throw({
    invalid_args,
    #{args => InvalidArg}
  }).
  
remove(#{dir := Directory}) ->
 #{
    destroy_attempts := Attempts,
    rocksdb := #{options := Options}
  } = ?ENV(log, undefined),
  try_remove(Directory, Attempts, Options);
remove(InvalidParams) ->
  throw({
    dir_required,
    #{params => InvalidParams}
  }).

%%% +--------------------------------------------------------------+
%%% |                 Transaction API Implementation               |
%%% +--------------------------------------------------------------+

%% NOTE & TODO: For performance, we use RocksDB async writes.
%% This is crash-consistent for Erlang / OS process crashes (data reaches OS buffers),
%% but it is NOT fully durable across machine crashes or power loss: the latest
%% writes may be lost. (rocksdb/wiki/basic-operations)
%%
%% If stronger durability will be required, we may consider enabling sync=true
%% and / or periodically syncing the WAL in the future.

%% Create rollback before applying upcoming commits.
%%  - Generate a rollback for each storage.
%%  - Return the transaction reference (TRef) required to commit or rollback.
rollback_prepare(
  Refs,
  Storages,
  Write,
  Delete,
  Index
) ->
  Log = get_ref(Refs),
  RollbackList =
    [begin
      prepare_rollback(StorageType, Refs, Write, Delete, inverse_index_values(Index))
     end || StorageType <- Storages],
  TRef = ?ENCODE_KEY(make_ref()),
  log_write(Log, [{put, TRef, ?ENCODE_VALUE(RollbackList)}]),
  TRef.
  
%% Recover from the log after the DB is opened.
%%  - Scan all rollback entries in RocksDB Log.
%%  - Apply each rollback for each storage.
%%  - Delete the transaction reference (TRef) after successful execution.
rollback_recovery(Refs) ->
  #log{database = DB, read = ReadParams} = Log = get_ref(Refs),
  rocksdb:fold(
    DB,
    fun({TRef, Rollback}, _Acc)->
      [begin
        #storage_rollback{
          type = Type,
          write = Write,
          delete = Delete,
          index = Index
        } = StorageRollback,
        {Module, StorageRef} = maps:get(Type, Refs),
        ecomet_db:commit(StorageRef, Module, Write, Delete, Index)
       end || StorageRollback <- ?DECODE_VALUE(Rollback)],
      log_write(Log, [{delete, TRef}]),
      ok
    end,
    ok,
    ReadParams
  ).

%% Roll back a transaction by TRef.
%%  - Load rollback data from RocksDB.
%%  - Apply rollbacks for each storage.
%%  - Delete the transaction reference (TRef) on success.
rollback(Refs, TRef) ->
  Log = get_ref(Refs),
  case log_get(Log, TRef) of
    {ok, Rollback} ->
      [begin
        #storage_rollback{
          type = Type,
          write = Write,
          delete = Delete,
          index = Index
        } = StorageRollback,
        {Module, StorageRef} = maps:get(Type, Refs),
        ecomet_db:commit(StorageRef, Module, Write, Delete, Index)
       end || StorageRollback <- ?DECODE_VALUE(Rollback)],
      log_write(Log, [{delete, TRef}]);
    _Ignore ->
      ok
  end.

%% Finalize commit by removing transaction reference (TRef).
%% If all storages committed successfully.
commit(Refs, TRef) ->
  Log = get_ref(Refs),
  log_write(Log, [{delete, TRef}]).

%%% +--------------------------------------------------------------+
%%% |                      Internal functions                      |
%%% +--------------------------------------------------------------+

get_ref(#{log := {?MODULE, Log}}) ->
  Log.

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

%% Index log format:
%% #{
%%   StorageType1 => [
%%     {K1, V1},  %% V1 is a boolean
%%     ...
%%     {KN, VN}
%%   ],
%%   ...
%%   StorageTypeN => ...
%% }
inverse_index_values(IndexLog) ->
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

%% Format of write: [{K1, V1}, ..., {KN, VN}]
%% Format of delete: [K1, ..., KN]
prepare_rollback(StorageType, Refs, WriteIn, DeleteIn, IndexIn) ->
  {Module, StorageRef} = maps:get(StorageType, Refs),
  
  StorageWrite = maps:get(StorageType, WriteIn, []),
  StorageDelete = maps:get(StorageType, DeleteIn, []),
  
  Keys = lists:usort([K || {K, _} <- StorageWrite] ++ StorageDelete),
  
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
  
  {WriteOut, DeleteOut} =
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
    write = if length(WriteOut) > 0 -> WriteOut; true -> none end,
    delete = if length(DeleteOut) > 0 -> DeleteOut; true -> none end,
    index = maps:get(StorageType, IndexIn, none)
  }.

try_create(RootDirectory) ->
  try
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
    #{log => {?MODULE, Reference}}
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
  end.

try_open(RootDirectory) ->
  try
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
    #{log => {?MODULE, Reference}}
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
  end.
  
try_close(#log{database = DB, directory = Directory}) ->
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