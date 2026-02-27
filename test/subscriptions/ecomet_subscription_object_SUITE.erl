
-module(ecomet_subscription_object_SUITE).

-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").
-include_lib("ecomet_subscription.hrl").

-define(NAME(N),list_to_atom("ecomet_subscription_object_"++integer_to_list(N))).
-define(WORKER(OID), ?NAME(erlang:phash2(OID, ecomet_subscription_pool:get_size()))).

-record(state, {
  objects,
  clients,
  queries,
  global,
  global_version = 0
}).

-record(object,{
  instance,
  clients,
  queries,
  fields,
  fields_ref
}).

-record(query,{
  conditions,
  fields,
  clients,
  set
}).

-record(wait_query,{
  set
}).

-record(client,{
  monitor,
  subs
}).

-record(o_client,{
  access,
  subs
}).

-record(q_client,{
  no_feedback,
  read
}).

-record(o_sub,{
  fields,
  read,
  no_feedback,
  oid
}).

-record(notification,{
  oid,
  client_id,
  subs_id,
  actor,
  action,
  no_feedback,
  updates,
  subs_fields,
  read,
  fields
}).


all()->
  [
    not_exists_test,
    {group,object_subscribe},
    {group,query_subscribe}
  ].

groups()->
  [
    {object_subscribe,
      [sequence],
      [
        object_subscribe_test,
        object_stateless_test,
        object_no_feedback_test,
        object_delete_test,
        object_update_rights_test
      ]
    },
    {query_subscribe,
      [sequence],
      [
        query_subscribe_test,
        query_additive_membership_test,
        query_multi_subscriptions_per_client_test,
        query_same_test,
        query_light_update_test,
        query_stateless_test,
        query_no_feedback_test,
        query_update_rights_test,
        query_global_sync_gap_test,
        query_wait_test
      ]
    }
  ].

init_per_suite(Config)->
  ?BACKEND_INIT(),
  SuitePID=?SUITE_PROCESS_START(),

  ecomet:dirty_login(<<"system">>),

  P1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"p1">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
    <<".folder">> => ?OID(<<"/root/.patterns">>),
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.object">>)
  })),

  ecomet:create_object(#{
    <<".name">> => <<"f1">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.field">>),
    <<".folder">> => P1,
    <<"type">> => string,
    <<"index">> => [simple,'3gram']
  }),

  ecomet:create_object(#{
    <<".name">> => <<"f2">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.field">>),
    <<".folder">> => P1,
    <<"type">> => string,
    <<"index">> => [simple,'3gram']
  }),

  ecomet:create_object(#{
    <<".name">> => <<"f3">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.field">>),
    <<".folder">> => P1,
    <<"type">> => integer,
    <<"index">> => [simple]
  }),

  ecomet:create_object(#{
    <<".name">> => <<"f4">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.field">>),
    <<".folder">> => P1,
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.object">>),
    <<"type">> => string,
    <<"index">> => none
  }),

  ecomet:create_object(#{
    <<".name">> => <<"f5">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.field">>),
    <<".folder">> => P1,
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.object">>),
    <<"type">> => integer,
    <<"index">> => none
  }),

  F1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"F1">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  UG1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"user_group1">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.usergroup">>),
    <<".folder">> => ?OID(<<"/root/.usergroups">>)
  })),

  UG2 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"user_group2">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.usergroup">>),
    <<".folder">> => ?OID(<<"/root/.usergroups">>)
  })),

  U1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"user1">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.user">>),
    <<".folder">> => ?OID(<<"/root/.users">>),
    <<"usergroups">> => [UG1],
    <<"password">> => <<"test">>
  })),

  U2 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"user2">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.user">>),
    <<".folder">> => ?OID(<<"/root/.users">>),
    <<"usergroups">> => [UG2],
    <<"password">> => <<"test">>
  })),

  [
    {suite_pid,SuitePID},
    {p1,P1},
    {f1,F1},
    {ug1,UG1},
    {ug2,UG2},
    {u1,U1},
    {u2,U2}
    |Config
  ].

end_per_suite(Config)->
  ?SUITE_PROCESS_STOP(?GET(suite_pid,Config)),
  ?BACKEND_STOP(),
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
not_exists_test(_Config)->

  State0 = #state{
    objects = #{},
    clients = #{},
    queries = #{},
    global = ?EMPTY_SET
  },

  AllWorkers = [whereis(?NAME(N)) || N <-ecomet_subscription_pool:get_workers()],
  [?assertEqual(State0, sys:get_state(W)) || W <- AllWorkers],

  Self = self(),

  Subscribe0 = #subscribe{
    id = id1,
    client = Self,
    dbs = [root],
    read = fun maps:with/2,
    deps = [<<"f1">>,<<"f2">>],
    conditions = undefined,
    params = #{
      stateless => false,
      no_feedback => false
    }
  },

  % add subscription to not existing object
  NotExistsOID = {3,99999999999},
  ?assertEqual(
    {error,{not_exists, NotExistsOID}},
    ecomet_subscription_object:subscribe(Subscribe0#subscribe{
      conditions = NotExistsOID
    })
  ),
  [?assertEqual(State0, sys:get_state(W)) || W <- AllWorkers],

  % add subscription to invalid oid
  InvalidOID = {999999999999,3},
  ?assertEqual(
    {error,{not_exists, InvalidOID}},
    ecomet_subscription_object:subscribe(Subscribe0#subscribe{
      conditions = InvalidOID
    })
  ),
  [?assertEqual(State0, sys:get_state(W)) || W <- AllWorkers],

  ok.

object_subscribe_test(Config) ->
  P1 = ?GET(p1,Config),
  F1 = ?GET(f1,Config),

  ecomet:dirty_login(<<"system">>),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F1,
    <<"f1">> => <<"object1 f1 value">>,
    <<"f2">> => <<"object1 f2 value">>,
    <<"f3">> => 1
  })),

  Client1 = start_client(<<"system">>),
  timer:sleep(100),

  {F1_F2, ReadF1F2} = ecomet_query:compile_subscribe_read(
    [<<"f1">>,<<"f2">>],
    undefined
  ),

  Subscribe1 = #subscribe{
    id = id1,
    client = Client1,
    dbs = [db1],
    read = ReadF1F2,
    deps = F1_F2,
    conditions = O,
    params = #{
      stateless => false,
      no_feedback => false
    }
  },

  ok = ecomet_subscription_object:subscribe(Subscribe1),

  ?SUBSCRIPTION(id1,create,O,Fields1) = from_client(Client1),
  ?assertEqual(
    #{
      <<"f1">> => <<"object1 f1 value">>,
      <<"f2">> => <<"object1 f2 value">>
    },
    Fields1
  ),

  W1 = whereis(?WORKER(O)),
  W1_State1 = sys:get_state(W1),
  ?LOGDEBUG("W1_State1 ~p",[W1_State1]),

  #state{
    objects = W1_S1_Objects,
    clients = W1_S1_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W1_State1,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client1 => #o_client{
            access = true,
            subs = ordsets:from_list([id1])
          }
        },
        queries = [],
        fields = #{
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>
        },
        fields_ref = #{
          <<"f1">> => 1,
          <<"f2">> => 1
        }
      }
    },
    W1_S1_Objects
  ),

  #{
    Client1 := #client{
      monitor = W1_S1_C1_MRef
    }
  } = W1_S1_Clients,

  ?assertEqual(
    #{
      Client1 => #client{
        monitor = W1_S1_C1_MRef,
        subs = #{
          id1 => #o_sub{
            fields = F1_F2,
            read = ReadF1F2,
            no_feedback = false,
            oid = O
          }
        }
      }
    },
    W1_S1_Clients
  ),

  {monitors, W1_S1_Monitors} = erlang:process_info(W1, monitors),
  ?assertEqual(true, lists:member({process,Client1}, W1_S1_Monitors)),

  %-------------------Client 2--------------------------------------
  Client2 = start_client(<<"system">>),
  timer:sleep(100),

  {F2_F3, ReadF2F3} = ecomet_query:compile_subscribe_read(
    [<<"f2">>,<<"f3">>],
    fun ecomet:to_string/2
  ),

  ok = ecomet_subscription_object:subscribe(Subscribe1#subscribe{
    client = Client2,
    read = ReadF2F3,
    deps = F2_F3
  }),

  ?SUBSCRIPTION(id1,create,O,Fields23) = from_client(Client2),
  ?assertEqual(
    #{
      <<"f2">> => <<"object1 f2 value">>,
      <<"f3">> => <<"1">>
    },
    Fields23
  ),

  W1_State2 = sys:get_state(W1),
  ?LOGDEBUG("W1_State2 ~p",[W1_State2]),

  #state{
    objects = W1_S2_Objects,
    clients = W1_S2_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W1_State2,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client1 => #o_client{
            access = true,
            subs = ordsets:from_list([id1])
          },
          Client2 => #o_client{
            access = true,
            subs = ordsets:from_list([id1])
          }
        },
        queries = [],
        fields = #{
          <<".object">> => ecomet_object:construct(O),  % Because last has formatter
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>,
          <<"f3">> => 1
        },
        fields_ref = #{
          <<".object">> => 1,
          <<"f1">> => 1,
          <<"f2">> => 2,
          <<"f3">> => 1
        }
      }
    },
    W1_S2_Objects
  ),

  #{
    Client2 := #client{
      monitor = W1_S2_C2_MRef
    }
  } = W1_S2_Clients,

  ?assertEqual(
    #{
      Client1 => #client{
        monitor = W1_S1_C1_MRef,
        subs = #{
          id1 => #o_sub{
            fields = F1_F2,
            read = ReadF1F2,
            no_feedback = false,
            oid = O
          }
        }
      },
      Client2 => #client{
        monitor = W1_S2_C2_MRef,
        subs = #{
          id1 => #o_sub{
            fields = F2_F3,
            read = ReadF2F3,
            no_feedback = false,
            oid = O
          }
        }
      }
    },
    W1_S2_Clients
  ),

  {monitors, W1_S2_Monitors} = erlang:process_info(W1, monitors),
  ?assertEqual(true, lists:member({process,Client1}, W1_S2_Monitors)),
  ?assertEqual(true, lists:member({process,Client2}, W1_S2_Monitors)),

  %-------------------Client 2 subscription 2-------------------------
  {F1_F3, ReadF1F3} = ecomet_query:compile_subscribe_read(
    [<<"f1">>,<<"f3">>],
    undefined
  ),

  ok = ecomet_subscription_object:subscribe(Subscribe1#subscribe{
    id = id2,
    client = Client2,
    read = ReadF1F3,
    deps = F1_F3,
    params = #{
      stateless => true,
      no_feedback => true
    }
  }),

  message_timeout = from_client(Client2, 2000),

  W1_State3 = sys:get_state(W1),
  ?LOGDEBUG("W1_State3 ~p",[W1_State3]),

  #state{
    objects = W1_S3_Objects,
    clients = W1_S3_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W1_State3,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client1 => #o_client{
            access = true,
            subs = ordsets:from_list([id1])
          },
          Client2 => #o_client{
            access = true,
            subs = ordsets:from_list([id1,id2])
          }
        },
        queries = [],
        fields = #{
          <<".object">> => ecomet_object:construct(O),
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>,
          <<"f3">> => 1
        },
        fields_ref = #{
          <<".object">> => 1,
          <<"f1">> => 2,
          <<"f2">> => 2,
          <<"f3">> => 2
        }
      }
    },
    W1_S3_Objects
  ),

  ?assertEqual(
    #{
      Client1 => #client{
        monitor = W1_S1_C1_MRef,
        subs = #{
          id1 => #o_sub{
            fields = F1_F2,
            read = ReadF1F2,
            no_feedback = false,
            oid = O
          }
        }
      },
      Client2 => #client{
        monitor = W1_S2_C2_MRef,
        subs = #{
          id1 => #o_sub{
            fields = F2_F3,
            read = ReadF2F3,
            no_feedback = false,
            oid = O
          },
          id2 => #o_sub{
            fields = F1_F3,
            read = ReadF1F3,
            no_feedback = true,
            oid = O
          }
        }
      }
    },
    W1_S3_Clients
  ),

  %-------------------------update object-------------------------------
  client_run(
    Client2,
    fun()->
      ecomet:edit_object(
        ecomet:open(O),
        #{
          <<"f3">> => 2
        }
      )
    end
  ),

  ?LOGDEBUG("check updates"),
  message_timeout = from_client(Client1, 2000),
  ?assertEqual(
    ?SUBSCRIPTION(id1, update, O, #{ <<"f3">> => <<"2">> }),
    from_client(Client2, 2000)
  ),

  % No notification by id2 because it's no_feedback = true
  message_timeout = from_client(Client2, 2000),

  W1_State4 = sys:get_state(W1),
  ?LOGDEBUG("W1_State4 ~p",[W1_State4]),

  #state{
    objects = W1_S4_Objects,
    clients = W1_S4_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W1_State4,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client1 => #o_client{
            access = true,
            subs = ordsets:from_list([id1])
          },
          Client2 => #o_client{
            access = true,
            subs = ordsets:from_list([id1,id2])
          }
        },
        queries = [],
        fields = #{
          <<".object">> => ecomet_object:construct(O),
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>,
          <<"f3">> => 2
        },
        fields_ref = #{
          <<".object">> => 1,
          <<"f1">> => 2,
          <<"f2">> => 2,
          <<"f3">> => 2
        }
      }
    },
    W1_S4_Objects
  ),

  ?assertEqual(
    W1_S3_Clients,
    W1_S4_Clients
  ),

  %----------------------------------unsubscribe-----------------------------------
  ok = ecomet_subscription_object:unsubscribe(Client2, id1),
  timer:sleep(100),

  W1_State5 = sys:get_state(W1),
  ?LOGDEBUG("W1_State5 ~p",[W1_State5]),

  #state{
    objects = W1_S5_Objects,
    clients = W1_S5_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W1_State5,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client1 => #o_client{
            access = true,
            subs = ordsets:from_list([id1])
          },
          Client2 => #o_client{
            access = true,
            subs = ordsets:from_list([id2])
          }
        },
        queries = [],
        fields = #{
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>,
          <<"f3">> => 2
        },
        fields_ref = #{
          <<"f1">> => 2,
          <<"f2">> => 1,
          <<"f3">> => 1
        }
      }
    },
    W1_S5_Objects
  ),

  ?assertEqual(
    #{
      Client1 => #client{
        monitor = W1_S1_C1_MRef,
        subs = #{
          id1 => #o_sub{
            fields = F1_F2,
            read = ReadF1F2,
            no_feedback = false,
            oid = O
          }
        }
      },
      Client2 => #client{
        monitor = W1_S2_C2_MRef,
        subs = #{
          id2 => #o_sub{
            fields = F1_F3,
            read = ReadF1F3,
            no_feedback = true,
            oid = O
          }
        }
      }
    },
    W1_S5_Clients
  ),

  {monitors, W1_S5_Monitors} = erlang:process_info(W1, monitors),
  ?assertEqual(true, lists:member({process,Client1}, W1_S5_Monitors)),
  ?assertEqual(true, lists:member({process,Client2}, W1_S5_Monitors)),

  %-------------------remove client-----------------------------------
  ok = ecomet_subscription_object:unsubscribe(Client1, id1),
  timer:sleep(100),

  W1_State6 = sys:get_state(W1),
  ?LOGDEBUG("W1_State6 ~p",[W1_State6]),

  #state{
    objects = W1_S6_Objects,
    clients = W1_S6_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W1_State6,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client2 => #o_client{
            access = true,
            subs = ordsets:from_list([id2])
          }
        },
        queries = [],
        fields = #{
          <<"f1">> => <<"object1 f1 value">>,
          <<"f3">> => 2
        },
        fields_ref = #{
          <<"f1">> => 1,
          <<"f3">> => 1
        }
      }
    },
    W1_S6_Objects
  ),

  ?assertEqual(
    #{
      Client2 => #client{
        monitor = W1_S2_C2_MRef,
        subs = #{
          id2 => #o_sub{
            fields = F1_F3,
            read = ReadF1F3,
            no_feedback = true,
            oid = O
          }
        }
      }
    },
    W1_S6_Clients
  ),

  {monitors, W1_S6_Monitors} = erlang:process_info(W1, monitors),
  ?assertEqual(false, lists:member({process,Client1}, W1_S6_Monitors)),
  ?assertEqual(true, lists:member({process,Client2}, W1_S6_Monitors)),

  exit(Client1, stop),

  %------------------remove all clients------------------------------
  exit(Client2, stop),
  timer:sleep(100),

  ?assertEqual(
    #state{
      objects = #{},
      clients = #{},
      queries = #{},
      global = ?EMPTY_SET
    },
    sys:get_state(W1)
  ),

  {monitors, W1_S7_Monitors} = erlang:process_info(W1, monitors),
  ?assertEqual(false, lists:member({process,Client2}, W1_S7_Monitors)),

  ok.

object_stateless_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object_stateless_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"stateless_test f1 value">>,
    <<"f2">> => <<"stateless_test f2 value">>,
    <<"f3">> => 23
  })),

  Client1 = start_client(<<"system">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {<<".oid">>,'=',O},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"stateless_test f1 value">>,
        <<"f2">> => <<"stateless_test f2 value">>
      }
    ),
    from_client(Client1)
  ),

  ok = ecomet_query:subscribe(
    id2,
    [root],
    [<<"f3">>],
    {<<".oid">>,'=',O},
    #{
      stateless => true,
      no_feedback => false,
      client => Client1
    }
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client1, 1000)
  ),

  exit(Client1, stop),
  timer:sleep(100),

  ok.

object_no_feedback_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object_no_feedback_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"object_no_feedback_test f1 value">>,
    <<"f2">> => <<"object_no_feedback_test f2 value">>,
    <<"f3">> => 34
  })),

  %-------------------no_feedback = false-------------------------
  Client1 = start_client(<<"system">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {<<".oid">>,'=',O},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"object_no_feedback_test f1 value">>,
        <<"f2">> => <<"object_no_feedback_test f2 value">>
      }
    ),
    from_client(Client1)
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<"f1">> => <<"object_no_feedback_test f1 value 2">>
  }),
  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f1">> => <<"object_no_feedback_test f1 value 2">>
      }
    ),
    from_client(Client1)
  ),

  client_run(
    Client1,
    fun()->
      ecomet:edit_object(ecomet:open(O),#{
        <<"f2">> => <<"object_no_feedback_test f2 value 2">>
      })
    end
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f2">> => <<"object_no_feedback_test f2 value 2">>
      }
    ),
    from_client(Client1)
  ),

  %-------------------no_feedback = true-------------------------
  Client2 = start_client(<<"system">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {<<".oid">>,'=',O},
    #{
      stateless => false,
      no_feedback => true,
      client => Client2
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"object_no_feedback_test f1 value 2">>,
        <<"f2">> => <<"object_no_feedback_test f2 value 2">>
      }
    ),
    from_client(Client2)
  ),

  client_run(
    Client1,
    fun()->
      ecomet:edit_object(ecomet:open(O),#{
        <<"f2">> => <<"object_no_feedback_test f2 value 3">>
      })
    end
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f2">> => <<"object_no_feedback_test f2 value 3">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f2">> => <<"object_no_feedback_test f2 value 3">>
      }
    ),
    from_client(Client2)
  ),

  client_run(
    Client2,
    fun()->
      ecomet:edit_object(ecomet:open(O),#{
        <<"f1">> => <<"object_no_feedback_test f1 value 3">>
      })
    end
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f1">> => <<"object_no_feedback_test f1 value 3">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client2, 1000)
  ),

  exit(Client1, stop),
  exit(Client2, stop),

  timer:sleep(100),

  ok.

object_delete_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object_delete_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"object_delete_test f1 value">>,
    <<"f2">> => <<"object_delete_test f2 value">>,
    <<"f3">> => 34
  })),

  Client1 = start_client(<<"system">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {<<".oid">>,'=',O},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"object_delete_test f1 value">>,
        <<"f2">> => <<"object_delete_test f2 value">>
      }
    ),
    from_client(Client1)
  ),

  W = whereis(?WORKER(?OID(O))),

  W_State1 = sys:get_state(W),
  ?LOGDEBUG("W_State1 ~p",[W_State1]),

  #state{
    objects = W_S1_Objects,
    clients = W_S1_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W_State1,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client1 => #o_client{
            access = true,
            subs = ordsets:from_list([id1])
          }
        },
        queries = [],
        fields = #{
          <<"f1">> => <<"object_delete_test f1 value">>,
          <<"f2">> => <<"object_delete_test f2 value">>
        },
        fields_ref = #{
          <<"f1">> => 1,
          <<"f2">> => 1
        }
      }
    },
    W_S1_Objects
  ),

  #{
    Client1 := #client{
      subs = W_S1_C1_Subs
    }
  } = W_S1_Clients,


  ?assertEqual(
    [id1],
    maps:keys(W_S1_C1_Subs)
  ),

  {monitors, W_S1_Monitors} = erlang:process_info(W, monitors),
  ?assertEqual(true, lists:member({process,Client1}, W_S1_Monitors)),

  ok = ecomet:delete_object(ecomet:open(O)),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      delete,
      O,
      #{}
    ),
    from_client(Client1)
  ),

  W_State2 = sys:get_state(W),
  ?LOGDEBUG("W_State2 ~p",[W_State2]),

  ?assertEqual(
    #state{
      objects = #{},
      clients = #{},
      queries = #{},
      global = ?EMPTY_SET
    },
    W_State2
  ),


  exit(Client1, stop),
  timer:sleep(100),

  ok.

object_update_rights_test(Config)->
  P1 = ?GET(p1,Config),

  UG1 = ?GET(ug1,Config),
  UG2 = ?GET(ug2,Config),


  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object_update_rights_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<".readgroups">> => [UG1],
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 34
  })),

  Client1 = start_client(<<"user1">>),
  Client2 = start_client(<<"user2">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {<<".oid">>,'=',O},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {<<".oid">>,'=',O},
    #{
      stateless => false,
      no_feedback => false,
      client => Client2
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client2, 1000)
  ),

  W = whereis(?WORKER(?OID(O))),

  W_State1 = sys:get_state(W),
  ?LOGDEBUG("W_State1 ~p",[W_State1]),

  #state{
    objects = W_S1_Objects,
    clients = _W_S1_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W_State1,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client1 => #o_client{
            access = true,
            subs = ordsets:from_list([id1])
          },
          Client2 => #o_client{
            access = false,
            subs = ordsets:from_list([id1])
          }
        },
        queries = [],
        fields = #{
          <<"f1">> => <<"f1 value">>,
          <<"f2">> => <<"f2 value">>
        },
        fields_ref = #{
          <<"f1">> => 2,
          <<"f2">> => 2
        }
      }
    },
    W_S1_Objects
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<".readgroups">> => [],
    <<"f1">> => <<"f1 value 2">>
  }),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      delete,
      O,
      #{}
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client2, 1000)
  ),

  W_State2 = sys:get_state(W),
  ?LOGDEBUG("W_State2 ~p",[W_State2]),

  #state{
    objects = W_S2_Objects,
    clients = _W_S2_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W_State2,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client1 => #o_client{
            access = false,
            subs = ordsets:from_list([id1])
          },
          Client2 => #o_client{
            access = false,
            subs = ordsets:from_list([id1])
          }
        },
        queries = [],
        fields = #{
          <<"f1">> => <<"f1 value 2">>,
          <<"f2">> => <<"f2 value">>
        },
        fields_ref = #{
          <<"f1">> => 2,
          <<"f2">> => 2
        }
      }
    },
    W_S2_Objects
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<".readgroups">> => [UG2],
    <<"f2">> => <<"f2 value 2">>
  }),

  ?assertEqual(
    message_timeout,
    from_client(Client1, 1000)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"f1 value 2">>,
        <<"f2">> => <<"f2 value 2">>
      }
    ),
    from_client(Client2)
  ),

  W_State3 = sys:get_state(W),
  ?LOGDEBUG("W_State3 ~p",[W_State3]),

  #state{
    objects = W_S3_Objects,
    clients = _W_S3_Clients,
    queries = #{},
    global = ?EMPTY_SET
  } = W_State3,

  ?assertEqual(
    #{
      O => #object{
        instance = ecomet_object:construct(O),
        clients = #{
          Client1 => #o_client{
            access = false,
            subs = ordsets:from_list([id1])
          },
          Client2 => #o_client{
            access = true,
            subs = ordsets:from_list([id1])
          }
        },
        queries = [],
        fields = #{
          <<"f1">> => <<"f1 value 2">>,
          <<"f2">> => <<"f2 value 2">>
        },
        fields_ref = #{
          <<"f1">> => 2,
          <<"f2">> => 2
        }
      }
    },
    W_S3_Objects
  ),

  exit(Client1, stop),
  exit(Client2, stop),
  timer:sleep(100),

  ok.

query_subscribe_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"query_subscribe_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 12
  })),

  O2 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object2">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 23
  })),

  W1 = whereis(?WORKER(?OID(O1))),
  W2 = whereis(?WORKER(?OID(O2))),

  Client1 = start_client(<<"system">>),
  Client2 = start_client(<<"system">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<"f1">>,'=',<<"f1 value">>},
      {<<"f3">>,'=',12}
    ]},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f2">>, <<"f3">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<"f1">>,'=',<<"f1 value">>},
      {<<"f3">>,'=',23}
    ]},
    #{
      stateless => false,
      no_feedback => false,
      client => Client2
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O1,
      #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client1, 1000)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O2,
      #{
        <<"f2">> => <<"f2 value">>,
        <<"f3">> => 23
      }
    ),
    from_client(Client2)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client2, 1000)
  ),

  #state{global = S1_Global} = sys:get_state(W1),
  [Q1_ref] = ecomet_subscription_query:find(
    #{
      action => create,
      db => root,
      tags => ?NEW_SET([
        {<<".folder">>,F,simple},
        {<<"f1">>,<<"f1 value">>,simple},
        {<<"f3">>,12,simple}
      ])
    },
    S1_Global
  ),

  [Q2_ref] = ecomet_subscription_query:find(
    #{
      action => create,
      db => root,
      tags => ?NEW_SET([
        {<<".folder">>,F,simple},
        {<<"f1">>,<<"f1 value">>,simple},
        {<<"f3">>,23,simple}
      ])
    },
    S1_Global
  ),

  [ begin
      ?LOGDEBUG("check state worker ~p",[N]),
      W = whereis(?NAME(N)),
      S = sys:get_state(W),
      ?LOGDEBUG("worker ~p State1 ~p",[N,S]),
      #state{
        queries = Queries,
        global = S1_Global
      } = S,

      #{
        Q1_ref := #query{
          conditions = {'AND',[
            {<<".folder">>,'=',F},
            {<<"f1">>,'=',<<"f1 value">>},
            {<<"f3">>,'=',12}
          ]},
          fields = [<<"f1">>, <<"f2">>],
          clients = #{
            Client1 := #{
              id1 := #q_client{
                no_feedback = false,
                read = _
              }
            }
          },
          set = Q1_Set
        },
        Q2_ref := #query{
          conditions = {'AND',[
            {<<".folder">>,'=',F},
            {<<"f1">>,'=',<<"f1 value">>},
            {<<"f3">>,'=',23}
          ]},
          fields = [<<"f2">>, <<"f3">>],
          clients = #{
            Client2 := #{
              id1 := #q_client{
                no_feedback = false,
                read = _
              }
            }
          },
          set = Q2_Set
        }
      } = Queries,

      if
        W =:= W1 ->
          ?assertEqual(
            ecomet_resultset:add_oid(O1, ecomet_resultset:new()),
            Q1_Set
          );
        true ->
          ?assertEqual(
            ecomet_resultset:new(),
            Q1_Set
          )
      end,

      if
        W =:= W2 ->
          ?assertEqual(
            ecomet_resultset:add_oid(O2, ecomet_resultset:new()),
            Q2_Set
          );
        true ->
          ?assertEqual(
            ecomet_resultset:new(),
            Q2_Set
          )
      end

    end || N <-ecomet_subscription_pool:get_workers() ],

  W1_State1 = sys:get_state(W1),
  ?LOGDEBUG("W1_State1 ~p",[W1_State1]),

  #state{
    objects = W1_S1_Objects,
    clients = W1_S1_Clients,
    queries = _W1_S1_Queries,
    global = _W1_S1_Global
  } = W1_State1,

  ?assertEqual(
    #{},
    W1_S1_Clients
  ),

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O1),
      clients = #{},
      queries = [Q1_ref],
      fields = #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      },
      fields_ref = #{
        <<"f1">> => 1,
        <<"f2">> => 1
      }
    },
    maps:get(O1, W1_S1_Objects)
  ),

  W2_State1 = sys:get_state(W2),
  ?LOGDEBUG("W2_State1 ~p",[W2_State1]),

  #state{
    objects = W2_S1_Objects,
    clients = W2_S1_Clients,
    queries = _W2_S1_Queries,
    global = _W2_S1_Global
  } = W2_State1,

  ?assertEqual(
    #{},
    W2_S1_Clients
  ),

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O2),
      clients = #{},
      queries = [Q2_ref],
      fields = #{
        <<"f2">> => <<"f2 value">>,
        <<"f3">> => 23
      },
      fields_ref = #{
        <<"f2">> => 1,
        <<"f3">> => 1
      }
    },
    maps:get(O2, W2_S1_Objects)
  ),

  %------------------add new object-----------------------------------
  O3 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object3">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 12
  })),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O3,
      #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client1,1000)
  ),

  W3 = whereis(?WORKER(?OID(O3))),
  W3_State2 = sys:get_state(W3),
  ?LOGDEBUG("W3_State2 ~p",[W3_State2]),

  #state{
    objects = W3_S3_Objects,
    clients = _W3_S2_Clients,
    queries = W3_S2_Queries,
    global = _W3_S2_Global
  } = W3_State2,

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O3),
      clients = #{},
      queries = [Q1_ref],
      fields = #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      },
      fields_ref = #{
        <<"f1">> => 1,
        <<"f2">> => 1
      }
    },
    maps:get(O3, W3_S3_Objects)
  ),

  #query{
    set = W3_S2_Q1_Set
  } = maps:get(Q1_ref, W3_S2_Queries),

  ?assertEqual(
    true,
    ecomet_resultset:contains(O3, W3_S2_Q1_Set)
  ),

  %-----------------------Object doesn't meet the conditions----------------------------
  ecomet:edit_object(ecomet:open(O1), #{<<"f3">> => 22}),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      delete,
      O1,
      #{}
    ),
    from_client(Client1)
  ),

  W1_State4 = sys:get_state(W1),
  ?LOGDEBUG("W1_State4 ~p",[W1_State4]),

  #state{
    objects = W1_S4_Objects,
    clients = _W1_S4_Clients,
    queries = W1_S4_Queries,
    global = _W1_S4_Global
  } = W1_State4,

  ?assertEqual(
    undefined,
    maps:get(O1, W1_S4_Objects, undefined)
  ),

  #query{
    set = W1_S4_Q1_Set
  } = maps:get(Q1_ref, W1_S4_Queries),

  ?assertEqual(
    false,
    ecomet_resultset:contains(O1, W1_S4_Q1_Set)
  ),

  ecomet:edit_object(ecomet:open(O1), #{<<"f3">> => 23}),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O1,
      #{
        <<"f2">> => <<"f2 value">>,
        <<"f3">> => 23
      }
    ),
    from_client(Client2)
  ),

  W1_State5 = sys:get_state(W1),
  ?LOGDEBUG("W1_State5 ~p",[W1_State5]),

  #state{
    objects = W1_S5_Objects,
    clients = _W1_S5_Clients,
    queries = W1_S5_Queries,
    global = _W1_S5_Global
  } = W1_State5,

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O1),
      clients = #{},
      queries = [Q2_ref],
      fields = #{
        <<"f2">> => <<"f2 value">>,
        <<"f3">> => 23
      },
      fields_ref = #{
        <<"f2">> => 1,
        <<"f3">> => 1
      }
    },
    maps:get(O1, W1_S5_Objects)
  ),

  #query{
    set = W1_S5_Q2_Set
  } = maps:get(Q2_ref, W1_S5_Queries),

  ?assertEqual(
    true,
    ecomet_resultset:contains(O1, W1_S5_Q2_Set)
  ),

  %------------------delete object------------------------------
  ecomet:delete_object(ecomet:open(O1)),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      delete,
      O1,
      #{}
    ),
    from_client(Client2)
  ),

  W1_State6 = sys:get_state(W1),
  ?LOGDEBUG("W1_State6 ~p",[W1_State6]),

  #state{
    objects = W1_S6_Objects,
    clients = _W1_S6_Clients,
    queries = W1_S6_Queries,
    global = _W1_S6_Global
  } = W1_State6,

  ?assertEqual(
    undefined,
    maps:get(O1, W1_S6_Objects, undefined)
  ),

  #query{
    set = W1_S6_Q2_Set
  } = maps:get(Q2_ref, W1_S6_Queries),

  ?assertEqual(
    false,
    ecomet_resultset:contains(O1, W1_S6_Q2_Set)
  ),

  %------------------------------unsubscribe------------------------------
  ecomet_query:unsubscribe(Client1, id1),
  timer:sleep(100),

  #state{global = S7_Global} = sys:get_state(W1),

  [ begin
      ?LOGDEBUG("check state7 worker ~p",[N]),
      W = whereis(?NAME(N)),
      S = sys:get_state(W),
      ?LOGDEBUG("worker ~p State7 ~p",[N,S]),
      #state{
        queries = Queries,
        global = S7_Global
      } = S,

      ?assertEqual(
        false,
        maps:is_key(Q1_ref, Queries)
      )

    end || N <-ecomet_subscription_pool:get_workers() ],

  exit(Client1, stop),

  exit(Client2, stop),
  timer:sleep(100),

  [ begin
      ?LOGDEBUG("check state8 worker ~p",[N]),
      W = whereis(?NAME(N)),
      S = sys:get_state(W),
      ?LOGDEBUG("worker ~p State8 ~p",[N,S]),
      #state{
        objects = #{},
        clients = #{},
        queries = #{},
        global = ?EMPTY_SET
      } = S

    end || N <-ecomet_subscription_pool:get_workers() ],

  ok.

query_additive_membership_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"query_additive_membership_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 12,
    <<"f4">> => <<"f4 value">>
  })),

  W = whereis(?WORKER(O)),

  Client1 = start_client(<<"system">>),
  Client2 = start_client(<<"system">>),
  timer:sleep(100),

  Conditions1 = {'AND',[
    {<<".folder">>,'=',F},
    {<<"f1">>,'=',<<"f1 value">>}
  ]},
  Conditions2 = {'AND',[
    {<<".folder">>,'=',F},
    {<<"f3">>,'=',23}
  ]},

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f4">>],
    Conditions1,
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f3">>, <<"f4">>],
    Conditions2,
    #{
      stateless => false,
      no_feedback => false,
      client => Client2
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f4">> => <<"f4 value">>
      }
    ),
    from_client(Client1)
  ),
  ?assertEqual(
    message_timeout,
    from_client(Client2, 1000)
  ),

  #state{
    objects = Objects1,
    queries = Queries1
  } = sys:get_state(W),
  #object{
    queries = ObjectQueries1
  } = maps:get(O, Objects1),
  ?assertEqual(1, length(ObjectQueries1)),

  [Q1_ref] = [
    Ref || {Ref, #query{conditions = QueryConditions}} <- maps:to_list(Queries1),
      QueryConditions =:= Conditions1
  ],
  [Q2_ref] = [
    Ref || {Ref, #query{conditions = QueryConditions}} <- maps:to_list(Queries1),
      QueryConditions =:= Conditions2
  ],

  ecomet:edit_object(ecomet:open(O), #{ <<"f3">> => 23 }),

  ?assertEqual(
    message_timeout,
    from_client(Client1, 1000)
  ),
  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f3">> => 23,
        <<"f4">> => <<"f4 value">>
      }
    ),
    from_client(Client2)
  ),

  #state{
    objects = Objects2,
    queries = Queries2
  } = sys:get_state(W),
  #object{
    queries = ObjectQueries2
  } = maps:get(O, Objects2),

  ?assertEqual(
    ordsets:from_list([Q1_ref, Q2_ref]),
    ObjectQueries2
  ),

  #query{
    set = Q1_Set
  } = maps:get(Q1_ref, Queries2),
  #query{
    set = Q2_Set
  } = maps:get(Q2_ref, Queries2),

  ?assertEqual(true, ecomet_resultset:contains(O, Q1_Set)),
  ?assertEqual(true, ecomet_resultset:contains(O, Q2_Set)),

  ecomet:edit_object(ecomet:open(O), #{ <<"f4">> => <<"f4 value 2">> }),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f4">> => <<"f4 value 2">>
      }
    ),
    from_client(Client1)
  ),
  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f4">> => <<"f4 value 2">>
      }
    ),
    from_client(Client2)
  ),

  exit(Client1, stop),
  exit(Client2, stop),
  timer:sleep(100),

  ok.

query_multi_subscriptions_per_client_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"query_multi_subscriptions_per_client_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 12
  })),

  Client = start_client(<<"system">>),
  timer:sleep(100),

  Conditions = {'AND',[
    {<<".folder">>,'=',F},
    {<<".name">>,'=',<<"object1">>}
  ]},

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>],
    Conditions,
    #{
      stateless => false,
      no_feedback => false,
      client => Client
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"f1 value">>
      }
    ),
    from_client(Client)
  ),

  ok = ecomet_query:subscribe(
    id2,
    [root],
    [<<"f1">>],
    Conditions,
    #{
      stateless => false,
      no_feedback => false,
      client => Client
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id2,
      create,
      O,
      #{
        <<"f1">> => <<"f1 value">>
      }
    ),
    from_client(Client)
  ),

  ecomet:edit_object(ecomet:open(O), #{
    <<"f1">> => <<"f1 value 2">>
  }),

  UpdateID1 = ?SUBSCRIPTION(
    id1,
    update,
    O,
    #{
      <<"f1">> => <<"f1 value 2">>
    }
  ),
  UpdateID2 = ?SUBSCRIPTION(
    id2,
    update,
    O,
    #{
      <<"f1">> => <<"f1 value 2">>
    }
  ),

  Received1 = from_client(Client, 1000),
  Received2 = from_client(Client, 1000),

  ?assertEqual(
    lists:sort([UpdateID1, UpdateID2]),
    lists:sort([Received1, Received2])
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client, 300)
  ),

  ok = ecomet_query:unsubscribe(Client, id2),
  timer:sleep(100),

  ecomet:edit_object(ecomet:open(O), #{
    <<"f1">> => <<"f1 value 3">>
  }),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f1">> => <<"f1 value 3">>
      }
    ),
    from_client(Client, 1000)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client, 300)
  ),

  ok = ecomet_query:unsubscribe(Client, id1),
  timer:sleep(100),

  exit(Client, stop),
  timer:sleep(100),

  ok.

query_same_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"query_same_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 12
  })),

  W1 = whereis(?WORKER(O1)),

  Client1 = start_client(<<"system">>),
  Client2 = start_client(<<"system">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<".name">>,'=',<<"object1">>},
      {<<"f3">>,'=',12}
    ]},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ok = ecomet_query:subscribe(
    id2,
    [root],
    [<<"f1">>, <<"f2">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<".name">>,'=',<<"object1">>},
      {<<"f3">>,'=',12}
    ]},
    #{
      stateless => false,
      no_feedback => false,
      client => Client2
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O1,
      #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id2,
      create,
      O1,
      #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      }
    ),
    from_client(Client2)
  ),

  W1_State1 = sys:get_state(W1),
  ?LOGDEBUG("W1_State1 ~p",[W1_State1]),

  #state{
    objects = W1_S1_Objects,
    clients = _W1_S1_Clients,
    queries = W1_S1_Queries,
    global = _W1_S1_Global
  } = W1_State1,

  [Q1_ref] = maps:keys(W1_S1_Queries),

  ?assertMatch(
    #{
      Q1_ref := #query{
        conditions = {'AND',[
          {<<".folder">>,'=',F},
          {<<".name">>,'=',<<"object1">>},
          {<<"f3">>,'=',12}
        ]},
        fields = [<<"f1">>, <<"f2">>],
        clients = #{
          Client1 := #{
            id1 := #q_client{
              no_feedback = false,
              read = _
            }
          },
          Client2 := #{
            id2 := #q_client{
              no_feedback = false,
              read = _
            }
          }
        },
        set = _W1_S1_Q1_Set
      }
    },
    W1_S1_Queries
  ),

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O1),
      clients = #{},
      queries = [Q1_ref],
      fields = #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      },
      fields_ref = #{
        <<"f1">> => 1,
        <<"f2">> => 1
      }
    },
    maps:get(O1, W1_S1_Objects)
  ),

  %-------------------update object--------------------
  ecomet:edit_object(ecomet:open(O1), #{ <<"f1">> => <<"f1 value 2">> }),

  % Client 1 doesn't receive the update because it's not subscribed to f4 field
  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O1,
      #{
        <<"f1">> => <<"f1 value 2">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id2,
      update,
      O1,
      #{
        <<"f1">> => <<"f1 value 2">>
      }
    ),
    from_client(Client2)
  ),

  W1_State2 = sys:get_state(W1),
  ?LOGDEBUG("W1_State2 ~p",[W1_State2]),

  #state{
    objects = W1_S2_Objects,
    clients = _W1_S2_Clients,
    queries = W1_S2_Queries,
    global = _W1_S2_Global
  } = W1_State2,

  ?assertEqual(
    W1_S1_Queries,
    W1_S2_Queries
  ),

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O1),
      clients = #{},
      queries = [Q1_ref],
      fields = #{
        <<"f1">> => <<"f1 value 2">>,
        <<"f2">> => <<"f2 value">>
      },
      fields_ref = #{
        <<"f1">> => 1,
        <<"f2">> => 1
      }
    },
    maps:get(O1, W1_S2_Objects)
  ),

  exit(Client1, stop),
  timer:sleep(100),

  W1_State3 = sys:get_state(W1),
  ?LOGDEBUG("W1_State3 ~p",[W1_State3]),

  #state{
    objects = W1_S3_Objects,
    clients = _W1_S3_Clients,
    queries = W1_S3_Queries,
    global = _W1_S3_Global
  } = W1_State3,

  #{
    Q1_ref := #query{
      conditions = {'AND',[
        {<<".folder">>,'=',F},
        {<<".name">>,'=',<<"object1">>},
        {<<"f3">>,'=',12}
      ]},
      fields = [<<"f1">>, <<"f2">>],
      clients = W1_S3_Q1_Clients,
      set = _W1_S3_Q1_Set
    }
  } = W1_S3_Queries,

  ?assertEqual(
    W1_S2_Objects,
    W1_S3_Objects
  ),

  ?assertEqual(
    [Client2],
    maps:keys(W1_S3_Q1_Clients)
  ),


  exit(Client2, stop),
  timer:sleep(100),

  #state{
    objects = #{},
    clients = #{},
    queries = #{},
    global = ?EMPTY_SET
  } = sys:get_state(W1),

  ok.

query_light_update_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"query_light_update_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 12,
    <<"f4">> => <<"f4 value">>,
    <<"f5">> => 23
  })),

  W1 = whereis(?WORKER(O1)),

  Client1 = start_client(<<"system">>),
  Client2 = start_client(<<"system">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f5">>],
    {<<".oid">>,'=',O1},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f4">>, <<"f5">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<"f1">>,'=',<<"f1 value">>},
      {<<"f3">>,'=',12}
    ]},
    #{
      stateless => false,
      no_feedback => false,
      client => Client2
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O1,
      #{
        <<"f1">> => <<"f1 value">>,
        <<"f5">> => 23
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O1,
      #{
        <<"f4">> => <<"f4 value">>,
        <<"f5">> => 23
      }
    ),
    from_client(Client2)
  ),

  W1_State1 = sys:get_state(W1),
  ?LOGDEBUG("W1_State1 ~p",[W1_State1]),

  #state{
    objects = W1_S1_Objects,
    clients = _W1_S1_Clients,
    queries = W1_S1_Queries,
    global = _W1_S1_Global
  } = W1_State1,

  [Q1_ref] = maps:keys(W1_S1_Queries),

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O1),
      clients = #{
        Client1 => #o_client{
          access = true,
          subs = [id1]
        }
      },
      queries = [Q1_ref],
      fields = #{
        <<"f1">> => <<"f1 value">>,
        <<"f4">> => <<"f4 value">>,
        <<"f5">> => 23
      },
      fields_ref = #{
        <<"f1">> => 1,
        <<"f4">> => 1,
        <<"f5">> => 2
      }
    },
    maps:get(O1, W1_S1_Objects)
  ),

  %-------------------light update (no index changes)--------------------
  ecomet:edit_object(ecomet:open(O1), #{ <<"f4">> => <<"f4 value 2">> }),

  % Client 1 doesn't receive the update because it's not subscribed to f4 field
  ?assertEqual(
    message_timeout,
    from_client(Client1)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O1,
      #{
        <<"f4">> => <<"f4 value 2">>
      }
    ),
    from_client(Client2)
  ),

  W1_State2 = sys:get_state(W1),
  ?LOGDEBUG("W1_State2 ~p",[W1_State2]),

  #state{
    objects = W1_S2_Objects,
    clients = _W1_S2_Clients,
    queries = _W1_S2_Queries,
    global = _W1_S2_Global
  } = W1_State2,

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O1),
      clients = #{
        Client1 => #o_client{
          access = true,
          subs = [id1]
        }
      },
      queries = [Q1_ref],
      fields = #{
        <<"f1">> => <<"f1 value">>,
        <<"f4">> => <<"f4 value 2">>,
        <<"f5">> => 23
      },
      fields_ref = #{
        <<"f1">> => 1,
        <<"f4">> => 1,
        <<"f5">> => 2
      }
    },
    maps:get(O1, W1_S2_Objects)
  ),

  ecomet:edit_object(ecomet:open(O1), #{ <<"f5">> => 34 }),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O1,
      #{
        <<"f5">> => 34
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O1,
      #{
        <<"f5">> => 34
      }
    ),
    from_client(Client2)
  ),

  W1_State3 = sys:get_state(W1),
  ?LOGDEBUG("W1_State3 ~p",[W1_State3]),

  #state{
    objects = W1_S3_Objects,
    clients = _W1_S3_Clients,
    queries = _W1_S3_Queries,
    global = _W1_S3_Global
  } = W1_State3,

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O1),
      clients = #{
        Client1 => #o_client{
          access = true,
          subs = [id1]
        }
      },
      queries = [Q1_ref],
      fields = #{
        <<"f1">> => <<"f1 value">>,
        <<"f4">> => <<"f4 value 2">>,
        <<"f5">> => 34
      },
      fields_ref = #{
        <<"f1">> => 1,
        <<"f4">> => 1,
        <<"f5">> => 2
      }
    },
    maps:get(O1, W1_S3_Objects)
  ),

  exit(Client1, stop),
  exit(Client2, stop),
  timer:sleep(100),

  #state{
    objects = #{},
    clients = #{},
    queries = #{},
    global = ?EMPTY_SET
  } = sys:get_state(W1),
  ok.

query_stateless_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"query_stateless_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 12,
    <<"f4">> => <<"f4 value">>,
    <<"f5">> => 23
  })),

  W1 = whereis(?WORKER(O1)),

  Client1 = start_client(<<"system">>),
  Client2 = start_client(<<"system">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f4">>, <<"f5">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<"f1">>,'=',<<"f1 value">>},
      {<<"f3">>,'=',12}
    ]},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f4">>, <<"f5">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<"f1">>,'=',<<"f1 value">>},
      {<<"f3">>,'=',12}
    ]},
    #{
      stateless => true,
      no_feedback => false,
      client => Client2
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O1,
      #{
        <<"f4">> => <<"f4 value">>,
        <<"f5">> => 23
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client2)
  ),

  W1_State1 = sys:get_state(W1),
  ?LOGDEBUG("W1_State1 ~p",[W1_State1]),

  #state{
    objects = W1_S1_Objects,
    clients = _W1_S1_Clients,
    queries = W1_S1_Queries,
    global = _W1_S1_Global
  } = W1_State1,

  [Q1_ref] = maps:keys(W1_S1_Queries),

  ?assertMatch(
    #query{
      conditions = _,
      fields = _,
      clients = #{
        Client1 := _,
        Client2 := _
      },
      set = _
    },
    maps:get(Q1_ref, W1_S1_Queries)
  ),

  ?assertEqual(
    #object{
      instance = ecomet_object:construct(O1),
      clients = #{},
      queries = [Q1_ref],
      fields = #{
        <<"f4">> => <<"f4 value">>,
        <<"f5">> => 23
      },
      fields_ref = #{
        <<"f4">> => 1,
        <<"f5">> => 1
      }
    },
    maps:get(O1, W1_S1_Objects)
  ),

  exit(Client1, stop),
  exit(Client2, stop),
  timer:sleep(100),

  #state{
    objects = #{},
    clients = #{},
    queries = #{},
    global = ?EMPTY_SET
  } = sys:get_state(W1),
  ok.

query_no_feedback_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"query_no_feedback_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 12,
    <<"f4">> => <<"f4 value">>,
    <<"f5">> => 23
  })),

  W1 = whereis(?WORKER(O1)),

  Client1 = start_client(<<"system">>),
  Client2 = start_client(<<"system">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f4">>, <<"f5">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<"f1">>,'=',<<"f1 value">>},
      {<<"f3">>,'=',12}
    ]},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f4">>, <<"f5">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<"f1">>,'=',<<"f1 value">>},
      {<<"f3">>,'=',12}
    ]},
    #{
      stateless => false,
      no_feedback => true,
      client => Client2
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O1,
      #{
        <<"f4">> => <<"f4 value">>,
        <<"f5">> => 23
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O1,
      #{
        <<"f4">> => <<"f4 value">>,
        <<"f5">> => 23
      }
    ),
    from_client(Client2)
  ),

  W1_State1 = sys:get_state(W1),
  ?LOGDEBUG("W1_State1 ~p",[W1_State1]),

  #state{
    objects = _W1_S1_Objects,
    clients = _W1_S1_Clients,
    queries = W1_S1_Queries,
    global = _W1_S1_Global
  } = W1_State1,

  [Q1_ref] = maps:keys(W1_S1_Queries),

  ?assertMatch(
    #query{
      conditions = _,
      fields = _,
      clients = #{
        Client1 := #{
          id1 := #q_client{
            no_feedback = false,
            read = _
          }
        },
        Client2 := #{
          id1 := #q_client{
            no_feedback = true,
            read = _
          }
        }
      },
      set = _
    },
    maps:get(Q1_ref, W1_S1_Queries)
  ),

  client_run(
    Client1,
    fun()->
      ecomet:edit_object(ecomet:open(O1), #{
        <<"f4">> => <<"f4 value 2">>
      })
    end
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O1,
      #{
        <<"f4">> => <<"f4 value 2">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O1,
      #{
        <<"f4">> => <<"f4 value 2">>
      }
    ),
    from_client(Client2)
  ),

  client_run(
    Client2,
    fun()->
      ecomet:edit_object(ecomet:open(O1), #{
        <<"f4">> => <<"f4 value 3">>
      })
    end
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O1,
      #{
        <<"f4">> => <<"f4 value 3">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client2, 1000)
  ),

  exit(Client1, stop),
  exit(Client2, stop),
  timer:sleep(100),

  ok.

query_update_rights_test(Config)->
  P1 = ?GET(p1,Config),

  UG1 = ?GET(ug1,Config),
  UG2 = ?GET(ug2,Config),


  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"query_update_rights_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<".readgroups">> => [UG1],
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 34
  })),

  Client1 = start_client(<<"user1">>),
  Client2 = start_client(<<"user2">>),
  timer:sleep(100),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<"f3">>,'=',34}
    ]},
    #{
      stateless => false,
      no_feedback => false,
      client => Client1
    }
  ),

  ok = ecomet_query:subscribe(
    id1,
    [root],
    [<<"f1">>, <<"f2">>],
    {'AND',[
      {<<".folder">>,'=',F},
      {<<"f3">>,'=',34}
    ]},
    #{
      stateless => false,
      no_feedback => false,
      client => Client2
    }
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      }
    ),
    from_client(Client1)
  ),

  % Client2 doesn't have rights
  ?assertEqual(
    message_timeout,
    from_client(Client2, 1000)
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<"f1">> => <<"f1 value 2">>
  }),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f1">> => <<"f1 value 2">>
      }
    ),
    from_client(Client1,1000)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client2,1000)
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<".readgroups">> => [UG1, UG2]
  }),

  % Client1 has rights but the update doesn't contain the fields it subscribed
  ?assertEqual(
    message_timeout,
    from_client(Client1,1000)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"f1 value 2">>,
        <<"f2">> => <<"f2 value">>
      }
    ),
    from_client(Client2)
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<".readgroups">> => [UG2]
  }),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      delete,
      O,
      #{}
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client2, 1000)
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<".readgroups">> => none
  }),

  ?assertEqual(
    message_timeout,
    from_client(Client1, 1000)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      delete,
      O,
      #{}
    ),
    from_client(Client2, 1000)
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<"f2">> => <<"f2 value 2">>
  }),

  ?assertEqual(
    message_timeout,
    from_client(Client1, 1000)
  ),

  ?assertEqual(
    message_timeout,
    from_client(Client2, 1000)
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<".readgroups">> => [UG2]
  }),

  ?assertEqual(
    message_timeout,
    from_client(Client1, 1000)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"f1 value 2">>,
        <<"f2">> => <<"f2 value 2">>
      }
    ),
    from_client(Client2)
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<".readgroups">> => [UG1]
  }),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O,
      #{
        <<"f1">> => <<"f1 value 2">>,
        <<"f2">> => <<"f2 value 2">>
      }
    ),
    from_client(Client1)
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      delete,
      O,
      #{}
    ),
    from_client(Client2)
  ),

  exit(Client1, stop),
  exit(Client2, stop),
  timer:sleep(100),

  ok.

query_wait_test(Config)->

  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"query_wait_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  Client1 = start_client(<<"user1">>),
  timer:sleep(100),

  meck:new(ecomet_subscription_query, [passthrough]),
  meck:expect(ecomet_subscription_query, find, fun(_Log, _Global)->[make_ref()] end),

  O1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 12,
    <<"f4">> => <<"f4 value">>,
    <<"f5">> => 23
  })),

  W1 = whereis(?WORKER(O1)),

  W1_State1 = sys:get_state(W1),
  ?LOGDEBUG("W1_State1 ~p",[W1_State1]),

  #state{
    objects = W1_S1_Objects,
    clients = W1_S1_Clients,
    queries = W1_S1_Queries,
    global = W1_S1_Global
  } = W1_State1,

  [Q1_ref] = maps:keys(W1_S1_Queries),

  ?assertEqual(
    #{},
    W1_S1_Objects
  ),

  ?assertEqual(
    #{},
    W1_S1_Clients
  ),

  ?assertEqual(
    #{
      Q1_ref => #wait_query{
        set = ecomet_resultset:add_oid(O1, ecomet_resultset:new())
      }
    },
    W1_S1_Queries
  ),

  ?assertEqual(
    ?EMPTY_SET,
    W1_S1_Global
  ),

  Deps = [<<"f1">>,<<"f2">>],
  Read =
    fun(Changes, Fields)->
      maps:with(Changes -- (Changes -- Deps),Fields)
    end,

  %---------------Object meets the conditions---------------------------
  ok = ecomet_subscription_object:init_query(
    Q1_ref,
    #subscribe{
      id = id1,
      client = Client1,
      dbs = [root],
      read = Read,
      deps = Deps,
      conditions = {'AND',[
        {<<".folder">>,'=',F},
        {<<"f3">>,'=',12}
      ]},
      params = #{
        stateless => false,
        no_feedback => false
      }
    },
    ecomet_resultset:new()
  ),
  timer:sleep(100),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      create,
      O1,
      #{
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      }
    ),
    from_client(Client1)
  ),

  W1_State2 = sys:get_state(W1),
  ?LOGDEBUG("W1_State2 ~p",[W1_State2]),

  #state{
    objects = W1_S2_Objects,
    clients = W1_S2_Clients,
    queries = W1_S2_Queries,
    global = W1_S2_Global
  } = W1_State2,

  ?assertEqual(
    W1_S1_Clients,
    W1_S2_Clients
  ),

  ?assertEqual(
    W1_S1_Global,
    W1_S2_Global
  ),

  ?assertEqual(
    #{
      O1 => #object{
        instance = ecomet_object:construct(O1),
        clients = #{},
        queries = [Q1_ref],
        fields = #{
          <<"f1">> => <<"f1 value">>,
          <<"f2">> => <<"f2 value">>
        },
        fields_ref = #{
          <<"f1">> => 1,
          <<"f2">> => 1
        }
      }
    },
    W1_S2_Objects
  ),

  ?assertEqual(
    #{
      Q1_ref => #query{
        conditions = {'AND',[
          {<<".folder">>,'=',F},
          {<<"f3">>,'=',12}
        ]},
        fields = Deps,
        clients = #{
          Client1 => #{
            id1 => #q_client{
              no_feedback = false,
              read = Read
            }
          }
        },
        set = ecomet_resultset:add_oid(O1, ecomet_resultset:new())
      }
    },
    W1_S2_Queries
  ),

  O2 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object2">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"f1 value">>,
    <<"f2">> => <<"f2 value">>,
    <<"f3">> => 34
  })),

  W2 = whereis(?WORKER(O2)),

  W2_State3 = sys:get_state(W2),
  ?LOGDEBUG("W2_State3 ~p",[W2_State3]),

  #state{
    objects = W2_S3_Objects,
    clients = W2_S3_Clients,
    queries = W2_S3_Queries,
    global = W2_S3_Global
  } = W2_State3,

  [Q2_ref] = maps:keys(maps:remove(Q1_ref,W2_S3_Queries)),

  ?assertEqual(
    #wait_query{
      set = ecomet_resultset:add_oid(O2, ecomet_resultset:new())
    },
    maps:get(Q2_ref, W2_S3_Queries)
  ),

  %---------------Object doesn't meet the conditions---------------------------
  ok = ecomet_subscription_object:init_query(
    Q2_ref,
    #subscribe{
      id = id2,
      client = Client1,
      dbs = [root],
      read = Read,
      deps = Deps,
      conditions = {'AND',[
        {<<".folder">>,'=',F},
        {<<"f2">>,'=',<<"another value">>}
      ]},
      params = #{
        stateless => false,
        no_feedback => false
      }
    },
    ecomet_resultset:new()
  ),
  timer:sleep(100),

  ?assertEqual(
    message_timeout,
    from_client(Client1, 1000)
  ),

  W2_State4 = sys:get_state(W2),
  ?LOGDEBUG("W2_State4 ~p",[W2_State4]),

  #state{
    objects = W2_S4_Objects,
    clients = W2_S4_Clients,
    queries = W2_S4_Queries,
    global = W2_S4_Global
  } = W2_State4,

  ?assertEqual(
    W2_S3_Objects,
    W2_S4_Objects
  ),

  ?assertEqual(
    W2_S3_Clients,
    W2_S4_Clients
  ),

  ?assertEqual(
    W2_S3_Global,
    W2_S4_Global
  ),

  ?assertEqual(
    #query{
      conditions = {'AND',[
        {<<".folder">>,'=',F},
        {<<"f2">>,'=',<<"another value">>}
      ]},
      fields = Deps,
      clients = #{
        Client1 => #{
          id2 => #q_client{
            no_feedback = false,
            read = Read
          }
        }
      },
      set = ecomet_resultset:new()
    },
    maps:get(Q2_ref, W2_S4_Queries)
  ),

  exit(Client1, stop),
  timer:sleep(100),
  ok.

query_global_sync_gap_test(_Config)->
  ecomet:dirty_login(<<"system">>),

  Unique = integer_to_binary(erlang:unique_integer([positive, monotonic])),
  Tag = {<<"f1">>, Unique, simple},
  SubsID = {id_gap_sync, make_ref()},
  Client = spawn(fun()-> timer:sleep(infinity) end),

  {Version0, Global0} = ecomet_subscription_query:global_snapshot(),

  ok = ecomet_subscription_query:subscribe(#subscribe{
    id = SubsID,
    client = Client,
    dbs = [root],
    read = fun maps:with/2,
    deps = [<<"f1">>],
    conditions = {<<"f1">>,'=',Unique},
    params = #{
      stateless => true,
      no_feedback => false
    }
  }),

  {Version1, Global1} = ecomet_subscription_query:global_snapshot(),
  ?assertEqual(true, Version1 >= (Version0 + 1)),
  ?assertEqual(true, gb_sets:is_member(Tag, Global1)),
  ?assertEqual(
    gb_sets:to_list(?SET_ADD(Tag, Global0)),
    gb_sets:to_list(Global1)
  ),

  Worker = whereis(?NAME(0)),
  #state{
    global = WorkerGlobal1,
    global_version = WorkerVersion1
  } = wait_worker_version(Worker, Version1, 2000),
  ?assertEqual(Version1, WorkerVersion1),
  ?assertEqual(
    gb_sets:to_list(Global1),
    gb_sets:to_list(WorkerGlobal1)
  ),

  StaleTag = {<<"stale_tag">>,Unique,simple},
  gen_server:cast(Worker, {global_set, StaleTag, Version1}),
  timer:sleep(100),

  #state{
    global = WorkerGlobal2,
    global_version = WorkerVersion2
  } = sys:get_state(Worker),
  ?assertEqual(Version1, WorkerVersion2),
  ?assertEqual(false, gb_sets:is_member(StaleTag, WorkerGlobal2)),
  ?assertEqual(
    gb_sets:to_list(Global1),
    gb_sets:to_list(WorkerGlobal2)
  ),

  GapTag = {<<"gap_tag">>,Unique,simple},
  gen_server:cast(Worker, {global_set, GapTag, Version1 + 50}),
  timer:sleep(100),

  #state{
    global = WorkerGlobal3,
    global_version = WorkerVersion3
  } = sys:get_state(Worker),
  ?assertEqual(Version1, WorkerVersion3),
  ?assertEqual(false, gb_sets:is_member(GapTag, WorkerGlobal3)),
  ?assertEqual(
    gb_sets:to_list(Global1),
    gb_sets:to_list(WorkerGlobal3)
  ),

  gen_server:cast(Worker, {global_reset, Tag, Version1 + 51}),
  timer:sleep(100),

  #state{
    global = WorkerGlobal4,
    global_version = WorkerVersion4
  } = sys:get_state(Worker),
  ?assertEqual(Version1, WorkerVersion4),
  ?assertEqual(true, gb_sets:is_member(Tag, WorkerGlobal4)),
  ?assertEqual(
    gb_sets:to_list(Global1),
    gb_sets:to_list(WorkerGlobal4)
  ),

  ok = ecomet_subscription_query:unsubscribe(Client, SubsID),
  timer:sleep(100),

  {Version2, Global2} = ecomet_subscription_query:global_snapshot(),
  ?assertEqual(true, Version2 >= (Version1 + 1)),
  ?assertEqual(
    gb_sets:to_list(Global0),
    gb_sets:to_list(Global2)
  ),

  #state{
    global = WorkerGlobal5,
    global_version = WorkerVersion5
  } = wait_worker_version(Worker, Version2, 2000),
  ?assertEqual(Version2, WorkerVersion5),
  ?assertEqual(
    gb_sets:to_list(Global2),
    gb_sets:to_list(WorkerGlobal5)
  ),

  exit(Client, shutdown),
  ok.


%%-------------client loop--------------------
start_client(User)->
  Self = self(),
  spawn(fun()->
    ecomet:dirty_login(User),
    ?LOGDEBUG("started client, user ~p",[User]),
    client_loop(Self)
  end).
client_loop(Self)->
  receive
    {Self, client_run, Fun}->
      ?LOGDEBUG("execute client_run"),
      Res = Fun(),
      Self ! {self(), result, Res},
      client_loop(Self);
    Any ->
      Self ! {self(), Any},
      client_loop(Self)
  end.

from_client(Client)->
  from_client(Client, _Timeout=100).
from_client(Client, Timeout)->
  receive
    {Client, Message} -> Message
  after
    Timeout -> message_timeout
  end.

client_run(Client, Fun)->
  Client ! {self(), client_run, Fun},
  receive
    {Client, result, Result} -> Result
  end.

wait_worker_version(Worker, ExpectedVersion, Timeout) when Timeout > 0->
  State = #state{
    global_version = Version
  } = sys:get_state(Worker),
  case Version >= ExpectedVersion of
    true ->
      State;
    false ->
      timer:sleep(50),
      wait_worker_version(Worker, ExpectedVersion, Timeout - 50)
  end;
wait_worker_version(Worker, _ExpectedVersion, _Timeout)->
  sys:get_state(Worker).
