-module(ecomet_log).

-include("ecomet.hrl").
-include("ecomet_log.hrl").

-export([
  create/1,
  open/1,
  close/1
]).

-export([
  prepare_rollback/3,
  execute_rollback/1,
  commit/2
]).

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

close(#reference{database = DB, directory = Directory}) ->
  case rocksdb:close(DB) of
    ok ->
      ok;
    {error, Error} ->
      throw({
        close_failed,
        #{error => Error, directory => Directory}
      })
  end.

prepare_rollback(Ref, Write, Delete) ->
  % TODO.
  Data = encode_data(Write, Delete),
  filter_data(Data, Ref).

commit(
  #reference{
    database = Log,
    write = Write
  },
  TRef
) ->
  case TRef of
    ignore -> ok;
    _Exists -> ok = rocksdb:write(Log, [{delete, TRef}], Write)
  end.
  
execute_rollback(#reference{database = DB, read = Read, write = Write}) ->
  rocksdb:fold(
    DB,
    fun({TRef, Rollback}, _Acc) ->
      % TODO. Apply rollback.
      rocksdb:write(DB, [{delete, TRef}], Write),
      ok
    end,
    ok,
    Read
  ).

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
    #reference{
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
    #reference{
      directory = RootDirectory,
      database = open_database(LogDirectory, Options),
      read = maps:to_list(Read),
      write = maps:to_list(Write)
    },
  % TODO. rollback_log(Reference),
  #{log => {?MODULE, Reference}}.

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
  
encode_data(_Write = [{K, V} | Rest], Delete) ->
  [{put, ?ENCODE_KEY(K), ?ENCODE_VALUE(V)} | encode_data(Rest, Delete)];
encode_data(_Write = [], _Delete = [K | Rest]) ->
  [{delete, ?ENCODE_KEY(K)} | encode_data([], Rest)];
encode_data(_Write = [], _Delete = []) ->
  [].

filter_data([{put, K, V} | Rest], #reference{database = DB, read = Params} = Ref) ->
  case rocksdb:get(DB, K, Params) of
    {ok, V} ->
      filter_data(Rest, Ref);
    {ok, V0} ->
      [{put, K, V0} | filter_data(Rest, Ref)];
    _ ->
      [{delete, K} | filter_data(Rest, Ref)]
  end;
filter_data([{delete, K} | Rest], #reference{database = DB, read = Params} = Ref) ->
  case rocksdb:get(DB, K, Params) of
    {ok, V} ->
      [{put, K, V} | filter_data(Rest, Ref)];
    _ ->
      filter_data(Rest, Ref)
  end;
filter_data([], _Ref) ->
  [].