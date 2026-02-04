

-module(ecomet_subscription_object).

-include("ecomet.hrl").
-include("ecomet_subscription.hrl").

-define(CALL_TIMEOUT, 60000).
-define(NAME(N),list_to_atom("ecomet_subscription_object_"++integer_to_list(N))).
-define(WORKER(OID), ?NAME(erlang:phash2(OID, ecomet_subscription_pool:get_size()))).

%%=================================================================
%%        API
%%=================================================================
-export([
  subscribe/1,
  unsubscribe/2
]).

%%=================================================================
%%        Transaction API
%%=================================================================
-export([
  on_commit/1,
  notify/1
]).

%%=================================================================
%%        Query API
%%=================================================================
-export([
  init_query/3,
  add_query_client/2,
  remove_query_client/2,
  global_set/1,
  global_reset/1
]).

%%=================================================================
%%        OTP API
%%=================================================================
-export([
  start_link/1,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

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
  actor,
  action,
  no_feedback,
  updates,
  subs_fields,
  read,
  fields
}).

%%=================================================================
%%        API
%%=================================================================
subscribe(
    Subscription=#subscribe{
      conditions = OID
    }
)->
  gen_server:call(?WORKER(OID), Subscription, ?CALL_TIMEOUT).

unsubscribe(Client, SubsID)->
  [gen_server:cast(?NAME(N), {unsubscribe, Client, SubsID}) || N <-ecomet_subscription_pool:get_workers()],
  ok.

%%=================================================================
%%        Transaction API
%%=================================================================
on_commit( Log ) when length(Log) > 0->
  Nodes = ecomet_subscription_nodes:get_active(),
  ecall:cast_all(Nodes, ?MODULE , notify,[ Log ] ),
  notify( Log );
on_commit( _Log )->
  ignore.

notify([#{oid := OID} = Log | Rest] )->
  gen_server:cast(?WORKER(OID), {notify, Log}),
  notify( Rest );
notify([])->
  ok.

%%=================================================================
%%        Query API
%%=================================================================
init_query(Ref, Subscription, InitSet)->
  % Group the set by workers
  ByWorkers =
    ecomet_resultset:foldl(
      fun(OID, Acc)->
        Worker = ?WORKER(OID),
        WorkerAcc0= maps:get(Worker, Acc),
        Acc#{
          Worker => ecomet_resultset:add_oid(OID, WorkerAcc0)
        }
      end,
      maps:from_list([{?NAME(N), ecomet_resultset:new()}|| N <- ecomet_subscription_pool:get_workers()]),
      InitSet
    ),

  maps:foreach(
    fun(Worker, Set)->
      gen_server:cast(Worker, {init_query, Ref, Subscription, Set})
    end,
    ByWorkers),
  ok.

add_query_client(Ref, Subscription)->
  [gen_server:cast(?NAME(N), {add_query_client, Ref, Subscription}) || N <-ecomet_subscription_pool:get_workers()],
  ok.

remove_query_client(Ref, ClientID)->
  [gen_server:cast(?NAME(N), {remove_query_client, Ref, ClientID}) || N <-ecomet_subscription_pool:get_workers()],
  ok.

global_set(Tag)->
  Ref = make_ref(),
  ReplyTo = self(),
  [gen_server:cast(?NAME(N), {global_set, Ref, Tag, ReplyTo}) || N <-ecomet_subscription_pool:get_workers()],
  wait_confirm(ecomet_subscription_pool:get_size(), Ref).

global_reset(Tag)->
  Ref = make_ref(),
  ReplyTo = self(),
  [gen_server:cast(?NAME(N), {global_reset, Ref, Tag, ReplyTo}) || N <-ecomet_subscription_pool:get_workers()],
  wait_confirm(ecomet_subscription_pool:get_size(), Ref).

wait_confirm(Rest, Ref) when Rest > 0->
  receive
    {confirm, Ref} -> wait_confirm(Rest-1, Ref)
  end;
wait_confirm(_Rest, _Ref)->
  ok.

%%=================================================================
%%        OTP
%%=================================================================
start_link(N)->
  gen_server:start_link({local,?NAME(N)}, ?MODULE, [], []).


init([]) ->

  {ok, #state{
    objects = #{},
    clients = #{},
    queries = #{},
    global = ?EMPTY_SET
  }}.

%%===================================================================
%% OTP generic server callbacks
%%===================================================================
handle_call(#subscribe{} = Subscription, _From, State0) ->
  ?LOGDEBUG("subscribe ~p",[Subscription]),
  try
    State = add_subscription(Subscription, State0),
    {reply, ok, State}
  catch
    _:E:S->
      ?LOGERROR("add object subscription error: ~p, stack ~p",[E,S]),
      {reply, {error, E}, State0}
  end;
handle_call(Request, _From, State) ->
  ?LOGWARNING("unexpected call request ~p", [Request]),
  {noreply, State}.

handle_cast({notify, Log}, State0) ->
  ?LOGDEBUG("notify ~p",[Log]),
  try
    State = notify(Log, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("notify error: log ~p, error ~p, stack ~p",[Log,E,S]),
      {noreply, State0}
  end;

handle_cast({unsubscribe, Client, SubsID}, State0) ->
  ?LOGDEBUG("unsubscribe ~p ~p",[Client, SubsID]),
  try
    State = unsubscribe(Client, SubsID, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("remove object subscription error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast({init_query, Ref, Subscription, Set}, State0) ->
  ?LOGDEBUG("init_query: Ref ~p, Subscription ~p",[Ref, Subscription]),
  try
    State = add_query(Ref, Subscription, Set, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("add new query error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast({add_query_client, Ref, Subscription}, State0) ->
  ?LOGDEBUG("add_query_client: Ref ~p, Subscription ~p",[Ref, Subscription]),
  try
    State = add_query_client(Ref, Subscription, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("add query client error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast({remove_query_client, Ref, ClientID}, State0) ->
  ?LOGDEBUG("remove_query_client: Ref ~p, ClientID ~p",[Ref, ClientID]),
  try
    State = remove_query_client(Ref, ClientID, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("remove query client error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast({global_set, Ref, Tag, ReplyTo}, State0) ->
  ?LOGDEBUG("global_set: Tag ~p",[Tag]),
  try
    State = global_set(Tag, State0),
    catch ReplyTo ! {confirm, Ref},
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("add new query error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast({global_reset, Ref, Tag, ReplyTo}, State0) ->
  ?LOGDEBUG("global_set: Tag ~p",[Tag]),
  try
    State = global_reset(Tag, State0),
    catch ReplyTo ! {confirm, Ref},
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("add new query error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast(Request,State) ->
  ?LOGWARNING("unexpected cast request ~p", [Request]),
  {noreply, State}.

handle_info({'DOWN', _Ref, process, Client, Reason}, State0) ->
  ?LOGDEBUG("destroy_client ~p, reason ~p",[Client, Reason]),
  try
    State = destroy_client(Client, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("remove object subscription error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_info(Info, State) ->
  ?LOGWARNING("unexpected info event ~p", [Info]),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%-------------------------------------------------------------------
%  Object subscriptions
%-------------------------------------------------------------------
add_subscription(
    Subscription = #subscribe{
      client = ClientID,
      id = SubsID
    },
    State0 = #state{
      clients = Clients
    }
)->

  case Clients of
    #{
      ClientID := #client{
        subs = #{
          SubsID := _
        }
      }
    } ->
      % The subscription already exists, ignore
      throw({not_unique_subscription, SubsID});
    _->
      State1 = add_object(Subscription, State0),
      State = add_client(Subscription, State1),
      init_subscription(Subscription, State),
      State
  end.

unsubscribe(
    ClientID,
    SubsID,
    State0 = #state{
      clients = Clients
    }
)->

  case Clients of
    #{
      ClientID := #client{
        subs = #{
          SubsID := _
        }
      }
    }->
      State1 = remove_object_subscription(ClientID, SubsID, State0),
      remove_client_subs(ClientID, [SubsID], State1);
    _->
      State0
  end.

destroy_client(
    ClientID,
    State0 = #state{
      clients = Clients
    }
)->
  case Clients of
    #{
      ClientID := #client{
        subs = ClientSubs
      }
    } ->
      lists:foldl(
        fun(SubsID, Acc)->
          unsubscribe(ClientID, SubsID, Acc)
        end,
        State0,
        maps:keys(ClientSubs)
      );
    _->
      State0
  end.

%-------------------------------------------------------------------
%  Query subscriptions
%-------------------------------------------------------------------
add_query(
    Ref,
    Subscription = #subscribe{
      conditions = Conditions,
      deps = Fields,
      params = #{
        stateless := Stateless
      }
    },
    Set0,
    State0 = #state{
      queries = Queries0,
      objects = Objects0
    }
)->
  Set = init_query_set(maps:get(Ref, Queries0, undefined), Subscription, Set0),

  Query0 = #query{
    conditions = Conditions,
    fields = Fields,
    clients = #{},
    set = Set
  },

  Objects = add_query_to_objects(Ref, Query0, Objects0),
  Query = add_client_to_query(Subscription, Query0),

  Queries = Queries0#{
    Ref => Query
  },

  State = State0#state{
    queries = Queries,
    objects = Objects
  },

  if
    Stateless =:= false ->
      init_query_client(Ref, Subscription, State);
    true ->
      ignore
  end,

  State.

remove_query(
    Ref,
    State0 = #state{
      queries = Queries0,
      objects = Objects0
    }
)->
  Query = maps:get(Ref, Queries0),
  Objects = remove_query_from_objects(Ref, Query, Objects0),

  Queries = maps:remove(Ref, Queries0),
  State0#state{
    queries = Queries,
    objects = Objects
  }.

init_query_set(
    #wait_query{
      set = WaitSet
    },
    #subscribe{
      conditions = Conditions
    },
    Set0
)->
  ecomet_resultset:foldl(
    fun(OID, RS)->
      try
        Object = ecomet_object:construct(OID),
        Fields = ecomet_object:read_all(Object),
        case ecomet_resultset:direct(Conditions, Fields) of
          true ->
            ecomet_resultset:add_oid(OID, RS);
          _->
            ecomet_resultset:remove_oid(OID, RS)
        end
      catch
        _:_->
          ecomet_resultset:remove_oid(OID, RS)
      end
    end,
    Set0,
    WaitSet
  );
init_query_set(_NoWaitQuery, _Subscription, Set)->
  Set.

add_query_client(
    Ref,
    Subscription = #subscribe{
      params = #{
        stateless := Stateless
      }
    },
    State0 = #state{
      queries = Queries0
    }
)->
  case Queries0 of
    #{ Ref := Query0}->
      Query = add_client_to_query(Subscription, Query0),
      Queries = Queries0#{
        Ref => Query
      },
      State = State0#state{
        queries = Queries
      },

      if
        Stateless =:= false ->
          init_query_client(Ref, Subscription, State);
        true ->
          ignore
      end,

      State;
    _->
      ?LOGWARNING("attempt to add client to unknown query ~p",[Ref]),
      State0
  end.

remove_query_client(
    Ref,
    ClientID,
    State0 = #state{
      queries = Queries0
    }
)->
  case Queries0 of
    #{ Ref := Query0}->
      Query = remove_client_from_query(ClientID, Query0),
      case has_clients( Query ) of
        true ->
          Queries = Queries0#{
            Ref => Query
          },
          State0#state{
            queries = Queries
          };
        _->
          remove_query(Ref, State0)
      end;
    _->
      ?LOGWARNING("attempt to remove client from unknown query ~p",[Ref]),
      State0
  end.

add_client_to_query(
    #subscribe{
      client = ClientID,
      id = SubsID,
      read = Read,
      params = #{
        no_feedback := NoFeedback
      }
    },
    Query0 = #query{
      clients = Clients0
    }
)->
  Client = #q_client{
    subs_id = SubsID,
    no_feedback = NoFeedback,
    read = Read
  },

  Clients = Clients0#{
    ClientID => Client
  },

  Query0#query{
    clients = Clients
  }.

remove_client_from_query(
    ClientID,
    Query0 = #query{
      clients = QueryClients0
    }
)->
  QueryClients = maps:remove(ClientID, QueryClients0),
  Query0#query{
    clients = QueryClients
  }.


init_query_client(
    Ref,
    #subscribe{
      client = ClientID
    },
    #state{
      queries = Queries,
      objects = Objects
    }
)->

  #query{
    clients = QueryClients,
    set = Set
  } = maps:get(Ref, Queries),

  #q_client{
    subs_id = SubsID,
    read = Read
  } = maps:get(ClientID, QueryClients),

  Notification = #notification{
    client_id = ClientID,
    subs_id = SubsID,
    action = create,
    read = Read
  },

  ecomet_resultset:foldl(
    fun(OID, Acc)->
      #object{
        fields = Fields
      } = maps:get(OID, Objects),
      send_notification(Notification#notification{
        oid = OID,
        fields = Fields
      }),
      Acc
    end,
    undefined,
    Set
  ),

  ok.

%-------------------------------------------------------------------
%  State transformations
%-------------------------------------------------------------------
add_object(
    Subscription = #subscribe{
      conditions = OID
    },
    State = #state{
      objects = Objects
    }
)->
  case maps:is_key(OID, Objects) of
    true ->
      add_object_sub(Subscription, State);
    _->
      init_object( Subscription, State)
  end.

add_object_sub(
    #subscribe{
      conditions = OID,
      client = ClientID,
      id = SubsID,
      deps = SubsFields
    },
    State0 = #state{
      objects = Objects0
    }
)->

  %---------Update already existing object---------------
  Object0 = maps:get(OID, Objects0),

  Object1 = add_fields(SubsFields, _UpdateFields = #{},Object0),
  Object = add_object_client(ClientID, SubsID, Object1),

  Objects = Objects0#{
    OID => Object
  },

  State0#state{
    objects = Objects
  }.

init_object(
    #subscribe{
      conditions = OID,
      client = ClientID,
      id = SubsID,
      deps = SubsFields
    },
    State0 = #state{
      objects = Objects0
    }
)->
  %---------Init new object---------------
  Object0 = init_new_object(OID, SubsFields, _UpdateFields = #{}),

  Object = add_object_client(ClientID, SubsID, Object0),

  Objects = Objects0#{
    OID => Object
  },

  State0#state{
    objects = Objects
  }.

init_new_object(OID, SubsFields, UpdateFields)->
  case ecomet_object:exists(OID) of
    false -> throw({not_exists, OID});
    true -> ok
  end,
  Instance = ecomet_object:construct( OID ),
  RealSubsFields = real_subs_fields(SubsFields, OID),

  InitFields =
    lists:foldl(
      fun(F, Acc)->
        case Acc of
          #{F := _}-> Acc;
          _->
            Acc#{ F => none }
        end
      end,
      #{},
      RealSubsFields
    ),

  Fields0 = maps:with(maps:keys(InitFields), UpdateFields),
  Fields =
    case maps:keys(InitFields) -- maps:keys(Fields0) of
      [] ->
        Fields0;
      ToReadFields->
        maps:merge(
          ecomet:read_fields(Instance, maps:with(ToReadFields,InitFields)),
          Fields0
        )
    end,

  FieldsRef = maps:map(fun(_F,_V)->1 end, Fields),

  #object{
    instance = Instance,
    fields = Fields,
    fields_ref = FieldsRef,
    clients = #{},
    queries = ordsets:new()
  }.

remove_object_subscription(
    ClientID,
    SubsID,
    State0 = #state{
      clients = Clients,
      objects = Objects0
    }
)->

  #client{
    subs = #{
      SubsID:=#o_sub{
        fields = SubsFields,
        oid = OID
      }
    }
  } = maps:get(ClientID, Clients),
  Object0 = maps:get(OID, Objects0),

  Object1 = remove_object_client(ClientID, SubsID, Object0),
  Object = remove_fields(SubsFields, Object1),

  Objects =
    case has_clients(Object) of
      true ->
        Objects0#{
          OID => Object
        };
      _->
        maps:remove(OID, Objects0)
    end,

  State0#state{
    objects = Objects
  }.


add_fields(
    SubsFields,
    UpdateFields,
    Object0 = #object{
      instance = Instance,
      fields = Fields0,
      fields_ref = FieldsRef0
    }
)->

  OID = ecomet_object:get_oid(Instance),
  RealSubsFields = real_subs_fields(SubsFields, OID),

  Fields =
    case RealSubsFields -- maps:keys( Fields0 ) of
      [] ->
        Fields0;
      NewFields->
        Fields1 = maps:merge(Fields0, maps:with(NewFields, UpdateFields)),
        case NewFields -- maps:keys(UpdateFields) of
          []->
            Fields1;
          ToReadFields->
            maps:merge(
              Fields1,
              ecomet_object:read_fields(Instance, ToReadFields)
            )
        end
    end,

  FieldsRef =
    lists:foldl(
      fun(F, Acc)->
        case Acc of
          #{F:=Count} ->
            Acc#{ F=> Count + 1 };
          _->
            Acc#{ F=>1 }
        end
      end,
      FieldsRef0,
      RealSubsFields
    ),

  Object0#object{
    fields = Fields,
    fields_ref = FieldsRef
  }.

remove_fields(
    SubsFields,
    Object0 = #object{
      instance = Instance,
      fields = Fields0,
      fields_ref = FieldsRef0
    }
)->

  OID = ecomet_object:get_oid(Instance),
  RealSubsFields = real_subs_fields(SubsFields, OID),

  FieldsRef =
    lists:foldl(
      fun(F, Acc)->
        case Acc of
          #{ F:= Count0 }->
            Count = Count0 - 1,
            if
              Count > 0->
                Acc#{ F=>Count };
              true->
                maps:remove(F, Acc)
            end;
          _->
            Acc
        end
      end,
      FieldsRef0,
      RealSubsFields
    ),

  Fields = maps:with(maps:keys(FieldsRef), Fields0),
  Object0#object{
    fields = Fields,
    fields_ref = FieldsRef
  }.

real_subs_fields(SubsFields, OID)->
  case lists:member('*', SubsFields) of
    true ->
      PatternID = ecomet_object:get_pattern_oid(OID),
      AllFields = maps:keys(ecomet_pattern:get_fields(PatternID)),
      ServiceFields = ecomet_object:service_fields(),
      ordsets:from_list(AllFields ++ ServiceFields);
    _->
      [F || F <- SubsFields, is_binary(F)]
  end.

add_object_client(
    ClientID,
    SubsID,
    Object = #object{
      clients = ObjectClients
    }
)->

  case maps:is_key(ClientID, ObjectClients) of
    true ->
      add_object_client_sub(ClientID, SubsID, Object);
    _->
      init_object_client(ClientID, SubsID, Object)
  end.

add_object_client_sub(
    ClientID,
    SubsID,
    Object0 = #object{
      clients = ObjectClients0
    }
)->
  Client0 = #o_client{
    subs = Subs0
  } = maps:get(ClientID, ObjectClients0),

  %-----------Add subscription to the existing client------------
  Subs = ordsets:add_element(SubsID, Subs0),
  Client = Client0#o_client{
    subs = Subs
  },
  Clients = ObjectClients0#{
    ClientID => Client
  },
  Object0#object{
    clients = Clients
  }.

init_object_client(
    ClientID,
    SubsID,
    Object0 = #object{
      instance = Object,
      clients = Clients0
    }
)->
  %-----------Add new client-------------------
  {ok,UG} = ecomet_user:get_user_rights(ClientID),
  {ok,RG} = ecomet:read_field(Object,<<".readgroups">>),
  Access = check_access(UG, RG),
  Client = #o_client{
    access = Access,
    subs = ordsets:from_list([SubsID])
  },

  Clients = Clients0#{
    ClientID => Client
  },
  Object0#object{
    clients = Clients
  }.

remove_object_client(
    ClientID,
    SubsID,
    Object0 = #object{
      clients = Clients0
    }
)->

  Client0 = #o_client{
    subs = Subs0
  } = maps:get(ClientID, Clients0),

  Subs = ordsets:del_element(SubsID, Subs0),
  Clients =
    case ordsets:size(Subs) of
      0 ->
        maps:remove(ClientID, Clients0);
      _->
        Client = Client0#o_client{
          subs = Subs
        },
        Clients0#{
          ClientID => Client
        }
    end,

  Object0#object{
    clients = Clients
  }.

add_client(
    Subscription = #subscribe{
      client = ClientID
    },
    State = #state{
      clients = Clients
    }
)->

  case maps:is_key(ClientID, Clients) of
    true ->
      add_client_sub(Subscription, State);
    _->
      init_client(Subscription, State)
  end.

add_client_sub(
    #subscribe{
      conditions = OID,
      client = ClientID,
      id = SubsID,
      deps = Fields,
      read = Read,
      params = #{
        no_feedback := NoFeedback
      }
    },
    State0 = #state{
      clients = Clients0
    }
)->

  %----------add subscription to the existing client--------------
  Client0 = #client{
    subs = Subs0
  } = maps:get(ClientID, Clients0),

  Sub = #o_sub{
    fields = Fields,
    read = Read,
    no_feedback = NoFeedback,
    oid = OID
  },

  Subs = Subs0#{
    SubsID => Sub
  },

  Client = Client0#client{
    subs = Subs
  },

  Clients = Clients0#{
    ClientID => Client
  },
  State0#state{
    clients = Clients
  }.

init_client(
   #subscribe{
     conditions = OID,
     client = ClientID,
     id = SubsID,
     deps = Fields,
     read = Read,
     params = #{
       no_feedback := NoFeedback
     }
    },
    State0 = #state{
      clients = Clients0
    }
)->
  %-------------Add new client------------------
  Sub = #o_sub{
    fields = Fields,
    read = Read,
    no_feedback = NoFeedback,
    oid = OID
  },
  Subs = #{
    SubsID => Sub
  },

  Ref = erlang:monitor(process, ClientID),
  Client = #client{
    monitor = Ref,
    subs = Subs
  },

  Clients = Clients0#{
    ClientID => Client
  },
  State0#state{
    clients = Clients
  }.

remove_client_subs(
    ClientID,
    SubsIDs,
    State0 = #state{
      clients = Clients0
    }
)->

  case Clients0 of
    #{
      ClientID := Client0 = #client{
        monitor = MonitorRef,
        subs = Subs0
      }
    }->
      Subs = maps:without(SubsIDs, Subs0),
      Clients =
        if
          map_size(Subs) > 0->
            Client = Client0#client{
              subs = Subs
            },
            Clients0#{
              ClientID => Client
            };
          true->
            erlang:demonitor(MonitorRef),
            maps:remove(ClientID, Clients0)
        end,

      State0#state{
        clients = Clients
      };
    _->
      State0
  end.

init_subscription(
    #subscribe{
      params = #{
        stateless := true
      }
    },
    _State
)->
  % The subscription is stateless
  ignore;

init_subscription(
    #subscribe{
      conditions = OID,
      client = ClientID,
      id = SubsID
    },
    #state{
      objects = Objects,
      clients = Clients
    }
)->
  #object{
    fields = Fields,
    clients = #{
      ClientID := #o_client{
        access = HasAccess
      }
    }
  } = maps:get(OID, Objects),

  if
    HasAccess ->
      % Send create to the client
      #client{
        subs = #{
          SubsID:=#o_sub{
            read = Read
          }
        }
      } = maps:get(ClientID, Clients),

      send_notification(#notification{
        oid = OID,
        client_id = ClientID,
        subs_id = SubsID,
        action = create,
        read = Read,
        fields = Fields

      });
    true->
      ignore
  end.

add_query_to_objects(
    Ref,
    #query{
      fields = SubsFields,
      set = Set
    },
    Objects0
)->
  ecomet_resultset:foldl(
    fun(OID, Acc)->
      try
        Object1 =
          case Acc of
            #{ OID := Object0 }->
              add_fields(SubsFields, _UpdateFields = #{}, Object0);
            _->
              init_new_object(OID, SubsFields, _UpdateFields = #{})
          end,
        Object = add_query_to_object(Ref, Object1),
        Acc#{
          OID => Object
        }
      catch
        _:R->
          ?LOGINFO("~p ignore to add object to query, reason ~p",[OID, R]),
          Acc
      end
    end,
    Objects0,
    Set
  ).

remove_query_from_objects(
    Ref,
    #query{
      fields = SubsFields,
      set = Set
    },
    Objects0
)->
  ecomet_resultset:foldl(
    fun(OID, Acc)->
      case Acc of
        #{ OID := Object0 }->
          Object1 = remove_fields(SubsFields, Object0),
          Object= remove_query_from_object(Ref, Object1),
          case has_clients(Object) of
            true ->
              Acc#{
                OID => Object
              };
            _->
              maps:remove(OID, Acc)
          end;
        _->
          ?LOGWARNING("attempt to remove query ~p from unknown object ~p",[Ref, OID]),
          Acc
      end
    end,
    Objects0,
    Set
  ).


add_query_to_object(
    Ref,
    Object0 = #object{
      queries = Queries0
    }
)->
  Queries = ordsets:add_element(Ref, Queries0),
  Object0#object{
    queries = Queries
  }.

remove_query_from_object(
    Ref,
    Object0 = #object{
      queries = Queries0
    }
)->
  Queries = ordsets:del_element(Ref, Queries0),
  Object0#object{
    queries = Queries
  }.

global_set(
    Tag,
    State0 = #state{
      global = Global0
    }
)->
  Global = ?SET_ADD(Tag, Global0),
  State0#state{
    global = Global
  }.

global_reset(
    Tag,
    State0 = #state{
      global = Global0
    }
)->
  Global = ?SET_DEL( Tag, Global0),
  State0#state{
    global = Global
  }.


check_access(is_admin, _RG)->
  true;
check_access(UG, RG) when is_list(RG)->
  case ordsets:intersection(UG, ordsets:from_list(RG)) of
    [] -> false;
    _->true
  end;
check_access(_UG, _RG)->
  false.

has_clients(#object{
  clients = Clients,
  queries = Queries
})->
  map_size(Clients) > 0 orelse (not ordsets:is_empty(Queries));

has_clients(#query{
  clients = Clients
})->
  map_size(Clients) > 0.


%-------------------------------------------------------------------
%  Notify
%-------------------------------------------------------------------
% Light update
notify(
    Log = #{
      action := light_update,
      oid := OID
    },
    State0 = #state{
      objects = Objects
    }
)->
  State =
    case maps:is_key(OID, Objects) of
      true ->
        notify_update(Log, State0);
      _->
        State0
    end,
  State;

notify(
    Log = #{
      action := create,
      oid := OID,
      fields := Fields
    },
    State0 = #state{
      global = Global,
      queries = Queries0,
      objects = Objects0
    }
)->
  QueriesToCheck = ecomet_subscription_query:find(Log, Global),
  #{
    wait := Wait,
    add := Add
  } = check_queries(OID, Fields, Queries0, QueriesToCheck),

  Queries =
    lists:foldl(
      fun(Qs, Acc)-> maps:merge(Acc,Qs) end,
      Queries0,
      [Wait, Add]
    ),

  Objects = add_queries_to_object(Add, OID, Fields, Objects0),

  State = State0#state{
    queries = Queries,
    objects = Objects
  },

  LogCreate = Log#{ action => create },
  queries_notify(maps:keys(Add), LogCreate, State),

  State;

notify(
    Log = #{
      oid := OID,
      action := update,
      fields := Fields1,
      fields0 := Fields0
    },
    State0 = #state{
      global = Global,
      queries = Queries0,
      objects = Objects0
    }
)->

  Fields = maps:merge(Fields0, Fields1),
  QueriesToCheck = ecomet_subscription_query:find(Log, Global),
  #{
    wait := Wait,
    add := Add,
    del := Del
  } = check_queries(OID, Fields, Queries0, QueriesToCheck),

  LogDelete = Log#{ action => delete },
  queries_notify(maps:keys(Del), LogDelete, State0),

  Queries1 = maps:merge(Queries0, Del),
  Objects1 = remove_queries_from_object(Del, OID, Objects0),

  State1 = State0#state{
    queries = Queries1,
    objects = Objects1
  },

  State2 =
    case maps:is_key(OID, Objects1) of
      true ->
        notify_update(Log, State1);
      _->
        State1
    end,

  #state{
    queries = Queries2,
    objects = Objects2
  } = State2,

  Queries =
    lists:foldl(
      fun(Qs, Acc)-> maps:merge(Acc,Qs) end,
      Queries2,
      [Wait, Add]
    ),

  Objects = add_queries_to_object(Add, OID, Fields, Objects2),

  State = State2#state{
    queries = Queries,
    objects = Objects
  },

  LogCreate = Log#{ action => create },
  queries_notify(maps:keys(Add), LogCreate, State),

  State;

notify(
    Log = #{
      action := delete,
      oid := OID
    },
    State0 = #state{
      objects = Objects
    }
)->
  case Objects of
    #{
      OID := Object = #object{
        queries = ObjectQueries
      }
    }->
      queries_notify(ObjectQueries, Log, State0),
      clients_notify(Log, Object, State0),

      State1 = delete_object_from_queries(Object, State0),
      State = delete_object_from_clients(Object, State1),

      State#state{
        objects = maps:remove(OID, Objects)
      };
    _->
      State0
  end.

%-------------------------------------------------
% UPDATE OBJECT RIGHTS
%-------------------------------------------------
notify_update(
    Log = #{
      oid := OID,
      self := Actor,
      action := Action,
      fields := ObjectUpdates = #{
        <<".readgroups">> := RG
      }
    },
    State0 = #state{
      objects = Objects0,
      queries = Queries,
      clients = Clients
    }
)->
  Object0 = #object{
    fields = Fields0,
    clients = ObjectClients0
  }= maps:get(OID, Objects0),


  Fields = maps:merge(
    Fields0,
    maps:with(maps:keys(Fields0), ObjectUpdates)
  ),

  Object1 = Object0#object{
    fields = Fields
  },


  Updates = ordsets:from_list(maps:keys(ObjectUpdates)),

  Notification0 = #notification{
    oid = OID,
    actor = Actor,
    action = Action,
    updates = Updates,
    fields = Fields
  },

  %---------Notify Object Clients----------------------
  ObjectClients =
    maps:fold(
      fun(
          ClientID,
          ObjectClient0 = #o_client{
            access = HasAccess0,
            subs = ClientObjectSubs
          },
          Acc
      )->
        #client{
          subs = ClientSubs
        } = maps:get(ClientID, Clients),
        {ok, UG} = ecomet_user:get_user_rights(ClientID),
        HasAccess = check_access(UG, RG),
        Notification1 =
          if
            HasAccess =:= true, HasAccess0 =:= false ->
              Notification0#notification{
                action = create
              };
            HasAccess =:= false, HasAccess0 =:= true ->
              Notification0#notification{
                action = delete
              };
            HasAccess =:= true->
              Notification0;
            true ->
              ignore
          end,

        if
          Notification1 =/= ignore ->
            [begin
               #o_sub{
                 fields = SubsFields,
                 read = Read,
                 no_feedback = NoFeedback
               } = maps:get(SubsID, ClientSubs),

               send_notification(Notification1#notification{
                 client_id = ClientID,
                 subs_id = SubsID,
                 no_feedback = NoFeedback,
                 subs_fields = SubsFields,
                 read = Read
               })
             end || SubsID <- ClientObjectSubs];
          true ->
            ignore
        end,

        ObjectClient = ObjectClient0#o_client{
          access = HasAccess
        },
        Acc#{
          ClientID => ObjectClient
        }
      end,
      #{},
      ObjectClients0
    ),

  Object = Object1#object{
    clients = ObjectClients
  },

  Objects = Objects0#{
    OID => Object
  },

  State = State0#state{
    objects = Objects
  },

  %--------Notify Query Clients--------------------------
  queries_notify(Queries, Log, State),

  State;

%-------------------------------------------------
% simple update
%-------------------------------------------------
notify_update(
    Log = #{
      oid := OID,
      fields := FieldsUpdates
    },
    State0 = #state{
      objects = Objects0
    }
)->

  Object0 = #object{
    fields = Fields0,
    queries = Queries
  } = maps:get(OID, Objects0),

  Fields = maps:merge(
    Fields0,
    maps:with(maps:keys(Fields0), FieldsUpdates)
  ),

  Object = Object0#object{
    fields = Fields
  },

  Objects = Objects0#{
    OID => Object
  },

  State = State0#state{
    objects = Objects
  },

  clients_notify(Log, Object, State),
  queries_notify(Queries, Log, State),

  State.

clients_notify(
    Log = #{
      oid := OID,
      self := Actor,
      action := Action
    },
    #object{
      clients = ObjectClients,
      fields = Fields
    },
    #state{
      clients = Clients
    }
)->
  Updates =
    case Log of
      #{ fields := ObjectUpdates }->
        ordsets:from_list(maps:keys(ObjectUpdates));
      _->
        []
    end,

  Notification = #notification{
    oid = OID,
    actor = Actor,
    action = Action,
    updates = Updates,
    fields = Fields
  },

  maps:foreach(
    fun(ClientID, #o_client{
      access = HasAccess,
      subs = ClientObjectSubs
    })->
      if
        HasAccess =:= true ->
          #client{
            subs = ClientSubs
          } = maps:get(ClientID, Clients),

          [begin
             #o_sub{
               fields = SubsFields,
               read = Read,
               no_feedback = NoFeedback
             } = maps:get(SubsID, ClientSubs),

             send_notification(Notification#notification{
               client_id = ClientID,
               subs_id = SubsID,
               no_feedback = NoFeedback,
               subs_fields = SubsFields,
               read = Read
             })
           end || SubsID <- ClientObjectSubs],
          ok;
        true ->
          ignore
      end
    end,
    ObjectClients
  ),
  ok.

send_notification(#notification{
  oid = OID,
  client_id = ClientID,
  subs_id = SubsID,
  actor = Actor,
  action = Action,
  no_feedback = NoFeedback,
  updates = Updates,
  subs_fields = SubsFields,
  read = Read,
  fields = Fields
})->
  ?LOGDEBUG("send_notification: ~p",[#{
    oid => OID,
    client_id => ClientID,
    subs_id => SubsID,
    actor => Actor,
    action => Action,
    no_feedback => NoFeedback,
    updates => Updates,
    subs_fields => SubsFields,
    fields => Fields
  }]),
  if
    ClientID =:= Actor, NoFeedback =:= true->
      ignore;
    true->
      if
        Action=:=light_update; Action =:= update ->
          SubscriptionUpdates =
            case ordsets:is_element('*', SubsFields) of
              true ->
                Updates;
              _->
                ordsets:intersection(Updates, SubsFields)
            end,
          if
            length(SubscriptionUpdates) > 0->
              Update = Read( Updates, Fields ),
              ?LOGDEBUG("send update: action update, Client ~p, SubsID ~p, OID ~p, Update ~p",[ClientID, SubsID, OID, Update]),
              catch ClientID ! ?SUBSCRIPTION(SubsID, update, OID, Update);
            true ->
              ignore
          end;
        Action =:= create ->
          AllFields = ordsets:from_list(maps:keys(Fields)),
          Update = Read(AllFields, Fields),
          ?LOGDEBUG("send update: action create Client ~p, SubsID ~p, OID ~p, Update ~p",[ClientID, SubsID, OID, Update]),
          catch ClientID ! ?SUBSCRIPTION(SubsID, create, OID, Update);
        Action =:= delete ->
          ?LOGDEBUG("send update: action delete Client ~p, SubsID ~p, OID ~p",[ClientID, SubsID, OID]),
          catch ClientID ! ?SUBSCRIPTION(SubsID, delete, OID, #{})
      end
  end,
  ok.


queries_notify(
    NotifyQueries,
    Log = #{
      oid := OID,
      self := Actor,
      action := Action
    },
    #state{
      objects = Objects,
      queries = Queries
    }
) when length(NotifyQueries) > 0->

  case maps:get(OID, Objects, undefined) of
    #object{
      fields = Fields
    }->
      Updates =
        case Log of
          #{ fields := ObjectUpdates } ->
            ordsets:from_list(maps:keys(ObjectUpdates));
          _->
            []
        end,

      Notification = #notification{
        oid = OID,
        actor = Actor,
        action = Action,
        fields = Fields,
        updates = Updates
      },
      [ begin
          #query{
            clients = Clients,
            fields = SubsFields
          } = maps:get(Ref, Queries),
          maps:foreach(
            fun(
                ClientID,
                #q_client{
                  subs_id = SubsID,
                  no_feedback = NoFeedback,
                  read = Read
                }
            )->
              send_notification(Notification#notification{
                client_id = ClientID,
                subs_id = SubsID,
                no_feedback = NoFeedback,
                read = Read,
                subs_fields = SubsFields
              })
            end,
            Clients
          )
        end || Ref <- NotifyQueries];
    _->
      ignore
  end,
  ok;
queries_notify(_Queries, _Log, _State)->
  ok.


check_queries(OID, Fields, Queries, QueriesToCheck)->
  lists:foldl(
    fun(Ref, Acc = #{wait := WaitAcc, add := AddAcc, del := DelAcc})->
      case Queries of
        #{ Ref := Query0 = #query{ conditions = Conditions, set = Set0} }->
          case {ecomet_resultset:direct(Conditions, Fields), ecomet_resultset:contains(OID, Set0)} of
            {true, false}->
              Set = ecomet_resultset:add_oid(OID, Set0),
              Query = Query0#query{
                set = Set
              },
              Acc#{
                add => AddAcc#{ Ref => Query }
              };
            {false, true}->
              Set = ecomet_resultset:remove_oid(OID, Set0),
              Query = Query0#query{
                set = Set
              },
              Acc#{
                del => DelAcc#{ Ref => Query }
              };
            _->
              Acc
          end;
        #{ Ref := WQ0 = #wait_query{ set = Set0 } }->
          Set = ecomet_resultset:add_oid(OID, Set0),
          WQ = WQ0#wait_query{ set = Set },
          Acc#{
            wait => WaitAcc#{ Ref => WQ }
          };
        _->
          Set = ecomet_resultset:add_oid(OID, ecomet_resultset:new()),
          WQ = #wait_query{
            set = Set
          },
          Acc#{
            wait => WaitAcc#{ Ref => WQ }
          }
      end
    end,
    #{
      wait => #{},
      add => #{},
      del => #{}
    },
    QueriesToCheck
  ).

add_queries_to_object(
    AddQueries,
    OID,
    UpdateFields,
    Objects0
) when map_size(AddQueries) > 0->
  SubsFields =
    maps:fold(
      fun(_Ref, #query{fields = QueryFields}, Acc)->
        ordsets:union(
          ordsets:from_list(QueryFields),
          Acc
        )
      end,
      [],
      AddQueries
    ),

  Object0 =
    case Objects0 of
      #{OID := ExistingObject} ->
        add_fields(SubsFields, UpdateFields, ExistingObject);
      _->
        init_new_object(OID, SubsFields, UpdateFields)
    end,

  Object = Object0#object{
    queries = ordsets:from_list(maps:keys(AddQueries))
  },

  Objects0#{
    OID => Object
  };
add_queries_to_object(
    _AddQueries,
    _OID,
    _UpdateFields,
    Objects
)->
  Objects.


remove_queries_from_object(
    RemoveQueries,
    OID,
    Objects0
) when map_size(RemoveQueries) > 0->
  case Objects0 of
    #{OID := Object0}->
      Object =
        maps:fold(
          fun(Ref, #query{fields = SubsFields}, Acc)->
            Acc1 = remove_fields(SubsFields, Acc),
            remove_query_from_object(Ref, Acc1)
          end,
          Object0,
          RemoveQueries
        ),
      case has_clients(Object) of
        true ->
          Objects0#{
            OID => Object
          };
        _->
          maps:remove(OID, Objects0)
      end;
    _->
      Objects0
  end;
remove_queries_from_object(
    _RemoveQueries,
    _OID,
    Objects
)->
  Objects.

delete_object_from_queries(
    #object{
      instance = Instance,
      queries = ObjectQueries
    },
    State0 = #state{
      queries = Queries0
    }
)->

  OID = ecomet_object:get_oid(Instance),

  Queries =
    lists:foldl(
      fun(Q, Acc)->
        Query0 = #query{
          set = Set0
        } = maps:get(Q, Acc),
        Set = ecomet_resultset:remove_oid(OID, Set0),
        Query = Query0#query{
          set = Set
        },
        Acc#{
          Q => Query
        }
      end,
      Queries0,
      ObjectQueries
    ),

  State0#state{
    queries = Queries
  }.

delete_object_from_clients(
    #object{
      clients = ObjectClients
    },
    State0
)->
  maps:fold(
    fun(
        ClientID,
        #o_client{
          subs = Subs
        },
        Acc
    )->
      remove_client_subs(ClientID, Subs, Acc)
    end,
    State0,
    ObjectClients
  ).














