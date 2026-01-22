
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
      subscribe_object_test       % Build new fields
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

  ok.

%%-------------client loop--------------------
start_client()->
  Self = self(),
  spawn(fun()->client_loop(Self) end).
client_loop(Self)->
  receive
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
    Timeout -> throw(message_timeout)
  end.