-ifndef(ECOMET_DB).
-define(ECOMET_DB,1).

-define(LOG_DIRECTORY(RootDirectory), RootDirectory ++ "/log").

-define(DEFAULT_LOG_OPTIONS, #{
  write => #{sync => false}
}).

-record(database, {
  log,
  log_dir,
  storages
}).

-record(storage, {
  ref,
  type,
  module,
  commit
}).

-record(rollback, {
  ref,
  volatile
}).

-record(commit, {
  data,
  delete,
  index
}).

-endif.
