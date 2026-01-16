
-module(ecomet_subscription_query_SUITE).

-include_lib("ecomet_test.hrl").
-include_lib("ecomet_subscription.hrl").

-record(state,{
  queries,
  key2ref,
  clients
}).

-record(query,{
  key,
  count,
  index
}).

-record(client,{
  monitor_ref,
  subs
}).

-record(index,{
  tag,
  '&',
  '!',
  db
}).

-define(key(S),#query_id{
  dbs = S#subscribe.dbs,
  fields = S#subscribe.deps,
  conditions = S#subscribe.conditions
}).

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
  transform_test/1
]).


all()->
  [
    transform_test
  ].

groups()->
  [].

%% Init system storages
init_per_suite(Config)->
  Config.
end_per_suite(_Config)->
  ok.

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

%--------------------------------------------------------------
% Set bit
%--------------------------------------------------------------
transform_test(_Config) ->

  meck:new(ecomet_subscription_pool, [passthrough]),
  meck:expect(ecomet_subscription_pool, get_workers, fun ?MODULE:pool_get_workers/0),
  meck:expect(ecomet_subscription_pool, get_size, fun ?MODULE:pool_get_size/0),

  meck:new(ecomet_query, [passthrough]),
  meck:expect(ecomet_query, get, fun ?MODULE:query_get/3),


  {ok, State0 = #state{
    queries = #{},
    key2ref = #{},
    clients = #{}
  }} = ecomet_subscription_query:init([]),

  Client1 = spawn_link(
    fun()->
      timer:sleep(infinity)
    end
  ),

  Conditions1 = {<<"f1">>,'=', value1},
  Read = fun maps:with/2,
  Subscribe1 = #subscribe{
    id = id1,
    client = Client1,
    usergroups = is_admin,
    dbs = [db1],
    read = Read,
    deps = [<<"f1">>,<<"f2">>],
    conditions = Conditions1,
    params = #{
      stateless => false,
      no_feedback => false
    }
  },
  Key1 = ?key(Subscribe1),


  State1 = #state{
    queries = Queries1,
    key2ref = KeyRef1,
    clients = Clients1
  } = ecomet_subscription_query:add_subscription(
    Subscribe1,
    State0
  ),
  #{Key1 := Ref1} = KeyRef1,

  ?assertEqual(
    #{Key1 => Ref1},
    KeyRef1
  ),
  #{
    Ref1 := Query1 = #query{
      key = Key1,
      count = 1,
      index = _
    }
  }= Queries1,

  ?assertEqual(
    #{ Ref1 => Query1},
    Queries1
  ),

  #{
    Client1 := #client{
      monitor_ref = Client1MRef
    }
  } = Clients1,
  ?assertEqual(
    #{
      Client1 => #client{
        monitor_ref = Client1MRef,
        subs = #{
          id1 => Ref1
        }
      }
    },
    Clients1
  ),

  ok.

%--------------------------------------------------------------
% Mocking
%--------------------------------------------------------------
pool_get_workers()->
  [].

pool_get_size()->
  0.

query_get(_DBs,_Fields, _Conditions)->
  ecomet_resultset:new().
