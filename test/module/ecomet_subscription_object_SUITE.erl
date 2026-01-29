
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
%%    not_exists_test,
%%    {group,object_subscribe},
    {group,query_subscribe}
  ].

groups()->
  [
    {object_subscribe,
      [sequence],
      [
        subscribe_object_test,
        stateless_test,
        no_feedback_test,
        delete_object_test,
        update_object_rights_test
      ]
    },
    {query_subscribe,
      [sequence],
      [
        subscribe_query_test
%%        wait_query_test,
%%        stateless_test,
%%        no_feedback_test,
%%        delete_object_test,
%%        update_object_rights_test
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

  Client1 = start_client(<<"system">>),
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
  Client2 = start_client(<<"system">>),
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

update_object_rights_test(Config)->
  P1 = ?GET(p1,Config),

  UG1 = ?GET(ug1,Config),
  UG2 = ?GET(ug2,Config),


  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"update_object_rights_test">>,
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
  timer:sleep(100),

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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [UG1],
          <<"f1">> => <<"f1 value">>,
          <<"f2">> => <<"f2 value">>
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [],
          <<"f1">> => <<"f1 value 2">>,
          <<"f2">> => <<"f2 value">>
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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
          <<".oid">> => O,
          object => ecomet_object:construct(O),
          <<".readgroups">> => [UG2],
          <<"f1">> => <<"f1 value 2">>,
          <<"f2">> => <<"f2 value 2">>
        },
        fields_ref = #{
          <<".oid">> => 1,
          object => 1,
          <<".readgroups">> => 1,
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

subscribe_query_test(Config)->
  P1 = ?GET(p1,Config),

  ecomet:dirty_login(<<"system">>),

  F = ?OID(ecomet:create_object(#{
    <<".name">> => <<"subscribe_query_test">>,
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

  QS = whereis(ecomet_subscription_query),
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
            Client1 := #q_client{
              usergroups = is_admin,
              subs_id = id1,
              no_feedback = false,
              read = _
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
            Client2 := #q_client{
              usergroups = is_admin,
              subs_id = id1,
              no_feedback = false,
              read = _
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
        <<".oid">> => O1,
        object => ecomet_object:construct(O1),
        <<".readgroups">> => [],
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      },
      fields_ref = #{
        <<".oid">> => 1,
        object => 1,
        <<".readgroups">> => 1,
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
        <<".oid">> => O2,
        object => ecomet_object:construct(O2),
        <<".readgroups">> => [],
        <<"f2">> => <<"f2 value">>,
        <<"f3">> => 23
      },
      fields_ref = #{
        <<".oid">> => 1,
        object => 1,
        <<".readgroups">> => 1,
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
        <<".oid">> => O3,
        object => ecomet_object:construct(O3),
        <<".readgroups">> => [],
        <<"f1">> => <<"f1 value">>,
        <<"f2">> => <<"f2 value">>
      },
      fields_ref = #{
        <<".oid">> => 1,
        object => 1,
        <<".readgroups">> => 1,
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
