
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

-define(NAME(N),list_to_atom("ecomet_subscription_object_"++integer_to_list(N))).
-define(S_INDEX,ecomet_subscriptions_index).
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
  index_test/1,
  query_get/3
]).


all()->
  [
    transform_test,
    index_test
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
  meck:expect(ecomet_query, system, fun ?MODULE:query_get/3),

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
  Client1 = spawn(
    fun()->
      timer:sleep(infinity)
    end
  ),

  Conditions1 = {<<"f1">>,'=', value1},
  Read = fun maps:with/2,
  Subscribe1 = #subscribe{
    id = id1,
    client = Client1,
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
  Client2 = spawn(
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
    Client2 := #client{
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
  exit(Client1, shutdown),

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

  %----------------Kill client2--------------
  exit(Client2, shutdown),
  timer:sleep(100),
  State6 = sys:get_state(QueryServer),

  ?assertEqual(State0, State6),

  {monitors, Monitors6} = erlang:process_info(QueryServer, monitors),
  ?assertEqual(false, lists:member({process,Client2}, Monitors6)),

  ok.

index_test(_Config) ->
  catch meck:unload(ecomet_query),
  catch meck:unload(ecomet_schema),
  meck:new(ecomet_query, [passthrough]),
  meck:expect(ecomet_query, system, fun ?MODULE:query_get/3),
  meck:new(ecomet_schema, [passthrough]),
  meck:expect(ecomet_schema, get_db_tags, fun(_DB)->[] end),

  ok = ensure_subscription_sup_started(),

  QueryServer = whereis(ecomet_subscription_query),
  EmptyQueryState = #state{
    queries = #{},
    key2ref = #{},
    clients = #{}
  },
  ?assertEqual(EmptyQueryState, sys:get_state(QueryServer)),
  ?assertEqual([], ets:tab2list(?S_INDEX)),

  Client = spawn(fun()-> timer:sleep(infinity) end),
  Read = fun maps:with/2,

  SimpleSubscribe = #subscribe{
    id = simple_id,
    client = Client,
    dbs = [root],
    read = Read,
    deps = [<<"simple_f">>],
    conditions = {<<"simple_f">>,'=',<<"simple_v">>},
    params = #{
      stateless => true,
      no_feedback => false
    }
  },
  MediumSubscribe = #subscribe{
    id = medium_id,
    client = Client,
    dbs = [root],
    read = Read,
    deps = [<<"medium_a">>,<<"medium_b">>],
    conditions = {'AND',[
      {<<"medium_a">>,'=',<<"a">>},
      {<<"medium_b">>,'=',<<"b">>}
    ]},
    params = #{
      stateless => true,
      no_feedback => false
    }
  },
  ComplexSubscribe = #subscribe{
    id = complex_id,
    client = Client,
    dbs = [root],
    read = Read,
    deps = [<<"complex_x">>,<<"complex_y">>,<<"complex_not">>,<<"complex_direct">>],
    conditions = {'ANDNOT',
      {'AND',[
        {'OR',[
          {<<"complex_x">>,'=',<<"x">>},
          {<<"complex_y">>,'=',<<"y">>}
        ]},
        {<<"complex_direct">>,':>',10}
      ]},
      {<<"complex_not">>,'=',<<"deny">>}
    },
    params = #{
      stateless => true,
      no_feedback => false
    }
  },

  ok = ecomet_subscription_query:subscribe(SimpleSubscribe),
  ok = ecomet_subscription_query:subscribe(MediumSubscribe),
  ok = ecomet_subscription_query:subscribe(ComplexSubscribe),

  SimpleKey = ?key(SimpleSubscribe),
  MediumKey = ?key(MediumSubscribe),
  ComplexKey = ?key(ComplexSubscribe),

  #state{
    queries = Queries1,
    key2ref = Key2Ref1,
    clients = Clients1
  } = sys:get_state(QueryServer),

  SimpleRef = maps:get(SimpleKey, Key2Ref1),
  MediumRef = maps:get(MediumKey, Key2Ref1),
  ComplexRef = maps:get(ComplexKey, Key2Ref1),

  #{
    Client := #client{
      monitor_ref = ClientMonitor,
      subs = ClientSubs
    }
  } = Clients1,

  ?assertEqual(
    #{
      simple_id => SimpleRef,
      medium_id => MediumRef,
      complex_id => ComplexRef
    },
    ClientSubs
  ),

  {monitors, Monitors1} = erlang:process_info(QueryServer, monitors),
  ?assertEqual(true, lists:member({process,Client}, Monitors1)),

  SimpleIndex = expected_index(SimpleSubscribe#subscribe.conditions, SimpleSubscribe#subscribe.dbs),
  MediumIndex = expected_index(MediumSubscribe#subscribe.conditions, MediumSubscribe#subscribe.dbs),
  ComplexIndex = expected_index(ComplexSubscribe#subscribe.conditions, ComplexSubscribe#subscribe.dbs),

  assert_query_index(Queries1, SimpleRef, SimpleIndex),
  assert_query_index(Queries1, MediumRef, MediumIndex),
  assert_query_index(Queries1, ComplexRef, ComplexIndex),

  assert_indexes_present(SimpleIndex, SimpleRef),
  assert_indexes_present(MediumIndex, MediumRef),
  assert_indexes_present(ComplexIndex, ComplexRef),

  AllComplexTags = lists:append([
    gb_sets:to_list(?SET_OR(And, Not))
    || #index{'&' = And, '!' = Not} <- ComplexIndex
  ]),
  ?assertEqual(
    [],
    [Tag || {Field, _, _} = Tag <- AllComplexTags, Field =:= <<"complex_direct">>]
  ),

  Global1 = indexes_global([SimpleIndex, MediumIndex, ComplexIndex]),
  assert_workers_global(Global1),

  SimpleMask = (hd(SimpleIndex))#index.'&',
  MediumMask = (hd(MediumIndex))#index.'&',
  ComplexEntry = hd(ComplexIndex),
  ComplexMask = ComplexEntry#index.'&',
  ComplexNot = ComplexEntry#index.'!',

  assert_find_refs(
    #{
      action => create,
      db => root,
      tags => SimpleMask
    },
    Global1,
    [SimpleRef]
  ),
  assert_find_refs(
    #{
      action => create,
      db => root,
      tags => MediumMask
    },
    Global1,
    [MediumRef]
  ),
  assert_find_refs(
    #{
      action => create,
      db => root,
      tags => remove_one_tag(MediumMask)
    },
    Global1,
    []
  ),
  assert_find_refs(
    #{
      action => create,
      db => root,
      tags => ComplexMask
    },
    Global1,
    [ComplexRef]
  ),
  assert_find_refs(
    #{
      action => create,
      db => root,
      tags => ?SET_OR(ComplexMask, ComplexNot)
    },
    Global1,
    []
  ),
  assert_find_refs(
    #{
      action => create,
      db => other_db,
      tags => SimpleMask
    },
    Global1,
    []
  ),

  assert_find_refs(
    #{
      action => update,
      db => root,
      tags => {?EMPTY_SET, MediumMask, ?EMPTY_SET}
    },
    Global1,
    [MediumRef]
  ),
  assert_find_refs(
    #{
      action => update,
      db => root,
      tags => {?EMPTY_SET, ?SET_OR(ComplexMask, ComplexNot), ?EMPTY_SET}
    },
    Global1,
    []
  ),
  assert_find_refs(
    #{
      action => update,
      db => root,
      tags => {?EMPTY_SET, ComplexMask, ComplexNot}
    },
    Global1,
    [ComplexRef]
  ),

  ecomet_subscription_query:unsubscribe(Client, complex_id),
  timer:sleep(100),

  #state{
    queries = Queries2,
    key2ref = Key2Ref2,
    clients = Clients2
  } = sys:get_state(QueryServer),
  ?assertEqual(false, maps:is_key(ComplexRef, Queries2)),
  ?assertEqual(false, maps:is_key(ComplexKey, Key2Ref2)),
  assert_indexes_absent(ComplexIndex),
  ?assertEqual(
    #{
      Client => #client{
        monitor_ref = ClientMonitor,
        subs = #{
          simple_id => SimpleRef,
          medium_id => MediumRef
        }
      }
    },
    Clients2
  ),

  Global2 = indexes_global([SimpleIndex, MediumIndex]),
  assert_workers_global(Global2),

  ecomet_subscription_query:unsubscribe(Client, medium_id),
  timer:sleep(100),

  #state{
    queries = Queries3,
    key2ref = Key2Ref3,
    clients = Clients3
  } = sys:get_state(QueryServer),
  ?assertEqual(false, maps:is_key(MediumRef, Queries3)),
  ?assertEqual(false, maps:is_key(MediumKey, Key2Ref3)),
  assert_indexes_absent(MediumIndex),
  ?assertEqual(
    #{
      Client => #client{
        monitor_ref = ClientMonitor,
        subs = #{
          simple_id => SimpleRef
        }
      }
    },
    Clients3
  ),

  Global3 = indexes_global([SimpleIndex]),
  assert_workers_global(Global3),

  ecomet_subscription_query:unsubscribe(Client, simple_id),
  timer:sleep(100),

  ?assertEqual(EmptyQueryState, sys:get_state(QueryServer)),
  assert_indexes_absent(SimpleIndex),
  ?assertEqual([], ets:tab2list(?S_INDEX)),
  assert_workers_global(?EMPTY_SET),

  exit(Client, shutdown),

  catch meck:unload(ecomet_schema),
  catch meck:unload(ecomet_query),
  ok.

%--------------------------------------------------------------
% Mocking
%--------------------------------------------------------------
query_get(_DBs,_Fields, _Conditions)->
  ecomet_resultset:new().

ensure_subscription_sup_started()->
  case whereis(ecomet_subscription_sup) of
    undefined ->
      {ok, _} = ecomet_subscription_sup:start_link(),
      ok;
    _->
      ok
  end.

expected_index(Conditions, DBs)->
  IndexDBs =
    if
      is_list(DBs)-> ordsets:from_list(DBs);
      true -> DBs
    end,
  compile_expected_index(
    ecomet_resultset:subscription_prepare(Conditions),
    IndexDBs
  ).

compile_expected_index([{[Tag|_] = And, Not}|Rest], DBs)->
  [#index{
    tag = Tag,
    '&' = ?NEW_SET(And),
    '!' = ?NEW_SET(Not),
    db = DBs
  } | compile_expected_index(Rest, DBs)];
compile_expected_index([], _DBs)->
  [].

assert_query_index(Queries, Ref, ExpectedIndex)->
  #query{
    count = 1,
    index = ActualIndex
  } = maps:get(Ref, Queries),
  ?assertEqual(lists:sort(ExpectedIndex), lists:sort(ActualIndex)).

assert_indexes_present(Indexes, Ref)->
  lists:foreach(
    fun(#index{tag = Tag} = Index)->
      [{_, TagIndexes}] = ets:lookup(?S_INDEX, {tag, Tag}),
      ?assertEqual(true, maps:is_key(Index, TagIndexes)),
      Subscribers = maps:get(Index, TagIndexes),
      ?assertEqual(true, ordsets:is_element(Ref, Subscribers))
    end,
    Indexes
  ).

assert_indexes_absent(Indexes)->
  lists:foreach(
    fun(#index{tag = Tag})->
      ?assertEqual([], ets:lookup(?S_INDEX, {tag, Tag}))
    end,
    Indexes
  ).

indexes_global(IndexGroups)->
  ?NEW_SET([Tag || #index{tag = Tag} <- lists:append(IndexGroups)]).

assert_workers_global(Global)->
  lists:foreach(
    fun(N)->
      Worker = whereis(?NAME(N)),
      {state, _, _, _, WorkerGlobal, _WorkerVersion} = sys:get_state(Worker),
      ?assertEqual(gb_sets:to_list(Global), gb_sets:to_list(WorkerGlobal))
    end,
    ecomet_subscription_pool:get_workers()
  ).

assert_find_refs(Log, Global, ExpectedRefs)->
  ?assertEqual(
    ordsets:from_list(ExpectedRefs),
    ordsets:from_list(ecomet_subscription_query:find(Log, Global))
  ).

remove_one_tag(Set)->
  [_Tag|Rest] = gb_sets:to_list(Set),
  ?NEW_SET(Rest).
