-module(ecomet_subscription_routing_SUITE).

-include_lib("ecomet_test.hrl").

%% API
-export([
  all/0,
  groups/0,
  init_per_testcase/2,
  end_per_testcase/2,
  init_per_group/2,
  end_per_group/2,
  init_per_suite/1,
  end_per_suite/1
]).

-export([
  notify_local_worker_test/1,
  notify_remote_worker_test/1,
  nodes_request_pool_sizes_test/1,
  on_commit_routes_to_remote_and_local_workers_test/1
]).

-define(NAME(N), list_to_atom("ecomet_subscription_object_" ++ integer_to_list(N))).
-define(WORKER(OID, Size), ?NAME(erlang:phash2(OID, Size))).

all()->
  [
    notify_local_worker_test,
    notify_remote_worker_test,
    nodes_request_pool_sizes_test,
    on_commit_routes_to_remote_and_local_workers_test
  ].

groups()->
  [].

init_per_suite(Config)->
  Config.

end_per_suite(_Config)->
  ok.

init_per_group(_, Config)->
  Config.

end_per_group(_, _Config)->
  ok.

init_per_testcase(_, Config)->
  Config.

end_per_testcase(_, _Config)->
  catch meck:unload(ecall),
  catch meck:unload(ecomet_subscription_nodes),
  catch meck:unload(ecomet_subscription_pool),
  ok.

notify_local_worker_test(_Config) ->
  Size = 5,
  Log = #{oid => {oid, local}, action => update},
  Worker = ?WORKER(maps:get(oid, Log), Size),
  start_worker(Worker),

  meck:new(ecomet_subscription_nodes, [passthrough]),
  meck:expect(ecomet_subscription_nodes, get_active, fun() ->
    []
  end),

  meck:new(ecomet_subscription_pool, [passthrough]),
  meck:expect(ecomet_subscription_pool, get_size, fun() ->
    Size
  end),

  ok = ecomet_subscription_object:on_commit([Log]),

  ?assertEqual({notify, Log}, receive_worker_msg()).

notify_remote_worker_test(_Config) ->
  Owner = self(),
  RemoteNode = 'remote_subscription@host',
  Size = 7,
  Log = #{oid => {oid, remote}, action => update},
  Worker = ?WORKER(maps:get(oid, Log), Size),

  meck:new(ecall, [passthrough]),
  meck:expect(ecall, send, fun(To, Message) ->
    Owner ! {ecall_send, To, Message},
    ok
  end),

  meck:new(ecomet_subscription_nodes, [passthrough]),
  meck:expect(ecomet_subscription_nodes, get_active, fun() ->
    [{RemoteNode, Size}]
  end),

  meck:new(ecomet_subscription_pool, [passthrough]),
  meck:expect(ecomet_subscription_pool, get_size, fun() ->
    0
  end),

  ok = ecomet_subscription_object:on_commit([Log]),

  ?assertEqual(
    {ecall_send, {Worker, RemoteNode}, {notify, Log}},
    receive_ecall_send()
  ).

nodes_request_pool_sizes_test(_Config) ->
  meck:new(ecall, [passthrough]),
  meck:expect(ecall, call, fun(Node, ecomet_subscription_pool, get_size, []) ->
    {ok, maps:get(Node, #{node_a => 3, node_b => 5})}
  end),

  ?assertEqual(
    [{node_a, 3}, {node_b, 5}],
    lists:sort(ecomet_subscription_nodes:request_pool_sizes([node_b, node_a]))
  ).

on_commit_routes_to_remote_and_local_workers_test(_Config) ->
  Owner = self(),
  RemoteNode = 'remote_subscription@host',
  RemoteSize = 7,
  LocalSize = 5,
  Log = #{oid => {oid, commit}, action => update},
  LocalWorker = ?WORKER(maps:get(oid, Log), LocalSize),
  RemoteWorker = ?WORKER(maps:get(oid, Log), RemoteSize),
  start_worker(LocalWorker),

  meck:new(ecall, [passthrough]),
  meck:expect(ecall, send, fun(To, Message) ->
    Owner ! {ecall_send, To, Message},
    ok
  end),

  meck:new(ecomet_subscription_nodes, [passthrough]),
  meck:expect(ecomet_subscription_nodes, get_active, fun() ->
    [{RemoteNode, RemoteSize}]
  end),

  meck:new(ecomet_subscription_pool, [passthrough]),
  meck:expect(ecomet_subscription_pool, get_size, fun() ->
    LocalSize
  end),

  ok = ecomet_subscription_object:on_commit([Log]),

  ?assertEqual({notify, Log}, receive_worker_msg()),
  ?assertEqual(
    {ecall_send, {RemoteWorker, RemoteNode}, {notify, Log}},
    receive_ecall_send()
  ).

start_worker(Worker) ->
  Owner = self(),
  Pid = spawn_link(fun() ->
    true = register(Worker, self()),
    Owner ! {worker_ready, self()},
    receive
      Message ->
        Owner ! {worker_msg, Message}
    end
  end),
  receive
    {worker_ready, Pid}->
      ok
  after 1000 ->
    error({worker_not_ready, Worker})
  end.

receive_worker_msg() ->
  receive
    {worker_msg, Message}->
      Message
  after 1000 ->
    error(worker_message_timeout)
  end.

receive_ecall_send() ->
  receive
    {ecall_send, To, Message}->
      {ecall_send, To, Message}
  after 1000 ->
    error(ecall_send_timeout)
  end.
