-ifndef(ECOMET_LOG).
-define(ECOMET_LOG,1).

-define(RETRY_TIMEOUT, 1000).
-define(LOCK(P), P ++ "/LOCK").
-define(LOG_DIRECTORY(RootDirectory), RootDirectory ++ "/log").

-define(DECODE_KEY(K), sext:decode(K)).
-define(ENCODE_KEY(K), sext:encode(K)).
-define(DECODE_VALUE(V), binary_to_term(V)).
-define(ENCODE_VALUE(V), term_to_binary(V)).

-define(GET_ENVIRONMENT(Key, Value, Default), application:get_env(Key, Value, Default)).

-record(reference, {
  directory,
  database,
  read,
  write
}).

-endif.
