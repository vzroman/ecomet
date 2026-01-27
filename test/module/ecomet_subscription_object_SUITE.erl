
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
  global
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
  add,
  remove
}).

-record(client,{
  monitor,
  usergroups,
  subs
}).

-record(o_client,{
  access,
  subs
}).

-record(q_client,{
  usergroups,
  subs_id,
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
  access,
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
    {group,object_subscribe}
  ].

groups()->
  [{object_subscribe,
    [sequence],
    [
      subscribe_object_test,
      stateless_test,
      no_feedback_test,
      delete_object_test
    ]
  }].

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
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.object">>),
    <<"type">> => string,
    <<"index">> => [simple,'3gram']
  }),

  ecomet:create_object(#{
    <<".name">> => <<"f2">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.field">>),
    <<".folder">> => P1,
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.object">>),
    <<"type">> => string,
    <<"index">> => [simple,'3gram']
  }),

  ecomet:create_object(#{
    <<".name">> => <<"f3">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.field">>),
    <<".folder">> => P1,
    <<"parent_pattern">> => ?OID(<<"/root/.patterns/.object">>),
    <<"type">> => integer,
    <<"index">> => [simple]
  }),

  F1 = ?OID(ecomet:create_object(#{
    <<".name">> => <<"F1">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  [
    {suite_pid,SuitePID},
    {p1,P1},
    {f1,F1}
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
    usergroups = is_admin,
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
      conditions = {<<".oid">>,'=',NotExistsOID}
    })
  ),
  [?assertEqual(State0, sys:get_state(W)) || W <- AllWorkers],

  % add subscription to invalid oid
  InvalidOID = {999999999999,3},
  ?assertEqual(
    {error,{not_exists, InvalidOID}},
    ecomet_subscription_object:subscribe(Subscribe0#subscribe{
      conditions = {<<".oid">>,'=',InvalidOID}
    })
  ),
  [?assertEqual(State0, sys:get_state(W)) || W <- AllWorkers],

  ok.

subscribe_object_test(Config) ->
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

  Client1 = start_client(),
  {F1_F2, ReadF1F2} = ecomet_query:compile_subscribe_read(
    [<<"f1">>,<<"f2">>],
    undefined
  ),

  Subscribe1 = #subscribe{
    id = id1,
    client = Client1,
    usergroups = is_admin,
    dbs = [db1],
    read = ReadF1F2,
    deps = F1_F2,
    conditions = {<<".oid">>,'=',O},
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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [],
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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
        usergroups = is_admin,
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
  Client2 = start_client(),
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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [],
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>,
          <<"f3">> => 1
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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
        usergroups = is_admin,
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
        usergroups = is_admin,
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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [],
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>,
          <<"f3">> => 1
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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
        usergroups = is_admin,
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
        usergroups = is_admin,
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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [],
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>,
          <<"f3">> => 2
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [],
          <<"f1">> => <<"object1 f1 value">>,
          <<"f2">> => <<"object1 f2 value">>,
          <<"f3">> => 2
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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
        usergroups = is_admin,
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
        usergroups = is_admin,
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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [],
          <<"f1">> => <<"object1 f1 value">>,
          <<"f3">> => 2
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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
        usergroups = is_admin,
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

stateless_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"stateless_test">>,
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

  Client1 = start_client(),
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

no_feedback_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"no_feedback_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"no_feedback_test f1 value">>,
    <<"f2">> => <<"no_feedback_test f2 value">>,
    <<"f3">> => 34
  })),

  %-------------------no_feedback = false-------------------------
  Client1 = start_client(),
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
        <<"f1">> => <<"no_feedback_test f1 value">>,
        <<"f2">> => <<"no_feedback_test f2 value">>
      }
    ),
    from_client(Client1)
  ),

  ecomet:edit_object(ecomet:open(O),#{
    <<"f1">> => <<"no_feedback_test f1 value 2">>
  }),
  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f1">> => <<"no_feedback_test f1 value 2">>
      }
    ),
    from_client(Client1)
  ),

  client_run(
    Client1,
    fun()->
      ecomet:edit_object(ecomet:open(O),#{
        <<"f2">> => <<"no_feedback_test f2 value 2">>
      })
    end
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f2">> => <<"no_feedback_test f2 value 2">>
      }
    ),
    from_client(Client1)
  ),

  %-------------------no_feedback = true-------------------------
  Client2 = start_client(),
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
        <<"f1">> => <<"no_feedback_test f1 value 2">>,
        <<"f2">> => <<"no_feedback_test f2 value 2">>
      }
    ),
    from_client(Client2)
  ),

  client_run(
    Client1,
    fun()->
      ecomet:edit_object(ecomet:open(O),#{
        <<"f2">> => <<"no_feedback_test f2 value 3">>
      })
    end
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f2">> => <<"no_feedback_test f2 value 3">>
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
        <<"f2">> => <<"no_feedback_test f2 value 3">>
      }
    ),
    from_client(Client2)
  ),

  client_run(
    Client2,
    fun()->
      ecomet:edit_object(ecomet:open(O),#{
        <<"f1">> => <<"no_feedback_test f1 value 3">>
      })
    end
  ),

  ?assertEqual(
    ?SUBSCRIPTION(
      id1,
      update,
      O,
      #{
        <<"f1">> => <<"no_feedback_test f1 value 3">>
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

delete_object_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"delete_object_test">>,
    <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>),
    <<".folder">> => ?OID(<<"/root">>)
  })),

  O = ?OID(ecomet:create_object(#{
    <<".name">> => <<"object1">>,
    <<".pattern">> => P1,
    <<".folder">> => F,
    <<"f1">> => <<"delete_object_test f1 value">>,
    <<"f2">> => <<"delete_object_test f2 value">>,
    <<"f3">> => 34
  })),

  Client1 = start_client(),
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
        <<"f1">> => <<"delete_object_test f1 value">>,
        <<"f2">> => <<"delete_object_test f2 value">>
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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [],
          <<"f1">> => <<"delete_object_test f1 value">>,
          <<"f2">> => <<"delete_object_test f2 value">>
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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

%%-------------client loop--------------------
start_client()->
  Self = self(),
  spawn(fun()->
    ecomet:dirty_login(<<"system">>),
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
