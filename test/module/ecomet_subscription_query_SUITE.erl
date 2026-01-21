
-module(ecomet_subscription_query_SUITE).

-include_lib("ecomet.hrl").
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
  transform_test/1,
  index_test/1
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


  meck:new(ecomet_query, [passthrough]),
  meck:expect(ecomet_query, get, fun ?MODULE:query_get/3),

  {ok, _} = ecomet_subscription_sup:start_link(),
  QueryServer = whereis(ecomet_subscription_query),
  State0 = sys:get_state(QueryServer),
  ?assertEqual(
    #state{
      queries = #{},
      key2ref = #{},
      clients = #{}
    },
    State0
  ),

  %---------------First subscription----------------------------
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

  ok = ecomet_subscription_query:subscribe(Subscribe1),

  State1 = sys:get_state(QueryServer),
  ?LOGDEBUG("State1 ~p",[State1]),

  #state{
    queries = Queries1,
    key2ref = KeyRef1,
    clients = Clients1
  } = State1,

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
    Client1 := C1 = #client{
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

  {monitors, Monitors1} = erlang:process_info(QueryServer, monitors),
  ?assertEqual(true, lists:member({process,Client1}, Monitors1)),

  %---------------Second subscription----------------------------
  Client2 = spawn_link(
    fun()->
      timer:sleep(infinity)
    end
  ),
  ok = ecomet_subscription_query:subscribe(Subscribe1#subscribe{
    client = Client2
  }),

  State2 = sys:get_state(QueryServer),
  ?LOGDEBUG("State2 ~p",[State2]),

  #state{
    queries = Queries2,
    key2ref = KeyRef1,
    clients = Clients2
  } = State2,

  ?assertEqual(
    #{
      Ref1 => Query1#query{
        count = 2
      }
    },
    Queries2
  ),

  #{
    Client2 := C2 = #client{
      monitor_ref = Client2MRef
    }
  } = Clients2,

  ?assertEqual(
    #{
      Client1 => C1,
      Client2 => #client{
        monitor_ref = Client2MRef,
        subs = #{
          id1 => Ref1
        }
      }
    },
    Clients2
  ),

  {monitors, Monitors2} = erlang:process_info(QueryServer, monitors),
  ?assertEqual(true, lists:member({process,Client1}, Monitors2)),
  ?assertEqual(true, lists:member({process,Client2}, Monitors2)),

  %---------------Third subscription----------------------------
  ?assertEqual(
    {error,{not_unique_subscription, id1}},
    ecomet_subscription_query:subscribe(Subscribe1#subscribe{
      client = Client2,
      conditions = {<<"f2">>,'=', value2}
    })
  ),
  Subscribe3 = Subscribe1#subscribe{
    id = id2,
    client = Client2,
    conditions = {<<"f2">>,'=', value2}
  },
  Key3 = ?key(Subscribe3),

  ok = ecomet_subscription_query:subscribe(Subscribe3),

  State3 = sys:get_state(QueryServer),
  ?LOGDEBUG("State3 ~p",[State3]),


  #state{
    queries = Queries3,
    key2ref = KeyRef3,
    clients = Clients3
  } = State3,

  #{Key3 := Ref3} = KeyRef3,
  ?assertEqual(
    #{
      Key1 => Ref1,
      Key3 => Ref3
    },
    KeyRef3
  ),

  #{
    Ref3 := Query3 = #query{
      key = Key3,
      count = 1,
      index = _
    }
  }= Queries3,

  ?assertEqual(
    #{
      Ref1 => Query1#query{
        count = 2
      },
      Ref3 => Query3
    },
    Queries3
  ),

  ?assertEqual(
    #{
      Client1 => #client{
        monitor_ref = Client1MRef,
        subs = #{
          id1 => Ref1
        }
      },
      Client2 => #client{
        monitor_ref = Client2MRef,
        subs = #{
          id1 => Ref1,
          id2 => Ref3
        }
      }
    },
    Clients3
  ),

  %------------------------unsubscribe-----------------------
  ecomet_subscription_query:unsubscribe( Client1, id1 ),

  State4 = sys:get_state(QueryServer),
  ?LOGDEBUG("State4 ~p",[State4]),

  #state{
    queries = Queries4,
    key2ref = KeyRef4,
    clients = Clients4
  } = State4,
  ?assertEqual(
    #{
      Ref1 => Query1#query{
        count = 1
      },
      Ref3 => Query3
    },
    Queries4
  ),
  ?assertEqual( KeyRef3, KeyRef4 ),

  ?assertEqual(
    #{
      Client2 => #client{
        monitor_ref = Client2MRef,
        subs = #{
          id1 => Ref1,
          id2 => Ref3
        }
      }
    },
    Clients4
  ),

  {monitors, Monitors4} = erlang:process_info(QueryServer, monitors),
  ?assertEqual(false, lists:member({process,Client1}, Monitors4)),
  ?assertEqual(true, lists:member({process,Client2}, Monitors4)),

  % Unsubscribe not existing subscription, nothing changes
  ecomet_subscription_query:unsubscribe( Client1, id1 ),
  State4 = sys:get_state(QueryServer),

  %----------------Unsubscribe id2-----------------------
  ecomet_subscription_query:unsubscribe( Client2, id2 ),
  State5 = sys:get_state(QueryServer),

  #state{
    queries = Queries5,
    key2ref = KeyRef5,
    clients = Clients5
  } = State5,

  ?assertEqual(
    #{
      Ref1 => Query1#query{
        count = 1
      }
    },
    Queries5
  ),
  ?assertEqual(
    #{
      Key1 => Ref1
    },
    KeyRef5
  ),

  ?assertEqual(
    #{
      Client2 => #client{
        monitor_ref = Client2MRef,
        subs = #{
          id1 => Ref1
        }
      }
    },
    Clients5
  ),

  {monitors, Monitors5} = erlang:process_info(QueryServer, monitors),
  ?assertEqual(true, lists:member({process,Client2}, Monitors5)),

  %----------------Unsubscribe client2 completely--------------
  ecomet_subscription_query:unsubscribe( Client2, id1 ),
  State6 = sys:get_state(QueryServer),

  ?assertEqual(State0, State6),

  {monitors, Monitors6} = erlang:process_info(QueryServer, monitors),
  ?assertEqual(false, lists:member({process,Client2}, Monitors6)),

  ok.

index_test(_Config) ->
  % TODO
  ok.

%--------------------------------------------------------------
% Mocking
%--------------------------------------------------------------
query_get(_DBs,_Fields, _Conditions)->
  ecomet_resultset:new().
