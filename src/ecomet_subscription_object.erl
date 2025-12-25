

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

%%=================================================================
%%        API
%%=================================================================
subscribe(
    Subscription=#subscribe{
      conditions = {<<".oid">>,'=',OID}
    }
)->
  gen_server:call(?WORKER(OID), Subscription, ?CALL_TIMEOUT).

unsubscribe(Client, SubsID)->
  [gen_server:cast(?NAME(N), {unsubscribe, Client, SubsID}) || N <-ecomet_subscription_pool:get_workers()],
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
        WorkerAcc0=
          case Acc of
            #{Worker := _WorkerAcc} ->
              _WorkerAcc;
            _->
              ecomet_resultset:new()
          end,
        Acc#{
          Worker => ecomet_resultset:add_oid(OID, WorkerAcc0)
        }
      end,
      _Acc = undefined,
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
  gen_server:start_link(?NAME(N), ?MODULE, [], []).


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

handle_cast({unsubscribe, Client, SubsID}, State0) ->
  try
    State = unsubscribe(Client, SubsID, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("remove object subscription error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast({init_query, Ref, Subscription, Set}, State0) ->
  try
    State = add_query(Ref, Subscription, Set, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("add new query error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast({add_query_client, Ref, Subscription}, State0) ->
  try
    State = add_query_client(Ref, Subscription, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("add query client error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast({remove_query_client, Ref, ClientID}, State0) ->
  try
    State = remove_query_client(Ref, ClientID, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("add query client error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast({global_set, Ref, Tag, ReplyTo}, State0) ->
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
      State0;
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
      State1 = remove_object(ClientID, SubsID, State0),
      remove_client(ClientID, SubsID, State1);
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
  Set = init_query_set(maps:get(Ref, Queries0, undefined), Set0),

  Query0 = #query{
    conditions = Conditions,
    fields = Fields,
    clients = #{},
    set = Set
  },

  Objects = add_query_to_objects(Ref, Query0, Objects0),
  Query = add_client_to_query(Subscription, Query0),

  Queries = #{
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
      add = AddObjects,
      remove = RemoveObjects
    },
    Set0
)->
  Set1 = lists:foldl(
    fun ecomet_resultset:remove_oid/2,
    Set0,
    RemoveObjects
  ),
  lists:foldl(
    fun ecomet_resultset:add_oid/2,
    Set1,
    AddObjects
  );
init_query_set(_NoWaitQuery, Set)->
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
      usergroups = UG,
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
    usergroups = UG,
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
  QueryClients0 = maps:remove(ClientID, QueryClients0),
  Query0#query{
    clients = QueryClients0
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

  QueryClient = maps:get(ClientID, QueryClients),

  ecomet_resultset:foldl(
    fun(OID, Acc)->
      Object = maps:get(OID, Objects),
      trigger_object_create(Object, ClientID, QueryClient),
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
      conditions = {<<".oid">>,'=',OID}
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
      conditions = {<<".oid">>,'=',OID},
      client = ClientID,
      id = SubsID,
      usergroups = UserGroups,
      deps = SubsFields
    },
    State0 = #state{
      objects = Objects0
    }
)->

  %---------Update already existing object---------------
  Object0 = maps:get(OID, Objects0),

  Object1 = add_fields(SubsFields, Object0),
  Object = add_object_client(ClientID, SubsID, UserGroups, Object1),

  Objects = Objects0#{
    OID => Object
  },

  State0#state{
    objects = Objects
  }.

init_object(
    #subscribe{
      conditions = {<<".oid">>,'=',OID},
      client = ClientID,
      id = SubsID,
      usergroups = UserGroups,
      deps = SubsFields
    },
    State0 = #state{
      objects = Objects0
    }
)->
  %---------Init new object---------------
  Object0 = init_new_object(OID, SubsFields),

  Object = add_object_client(ClientID, SubsID, UserGroups, Object0),

  Objects = Objects0#{
    OID => Object
  },

  State0#state{
    objects = Objects
  }.

init_new_object(OID, SubsFields)->
  Instance = ecomet_object:construct( OID ),
  InitFields =
    lists:foldl(
      fun(F, Acc)->
        case Acc of
          #{F := _}-> Acc;
          _->
            Acc#{ F => none }
        end
      end,
      #{<<".readgroups">> => []},
      SubsFields),

  Fields = ecomet:read_fields(Instance, InitFields),
  FieldsRef = maps:map(fun(_F,_V)->1 end, Fields),

  #object{
    instance = Instance,
    fields = Fields,
    fields_ref = FieldsRef,
    clients = #{},
    queries = ordsets:new()
  }.

remove_object(
    ClientID,
    SubsID,
    State0 = #state{
      clients = Clients,
      objects = Objects0
    }
)->

  #{
    ClientID := #client{
      subs = #{
        SubsID:=#o_sub{
          fields = SubsFields,
          oid = OID
        }
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
    Object0 = #object{
      instance = Instance,
      fields = Fields0,
      fields_ref = FieldsRef0
    }
)->

  Fields =
    case SubsFields -- maps:keys( Fields0 ) of
      [] ->
        Fields0;
      NewFields->
        maps:merge(
          Fields0,
          ecomet_object:read_fields(Instance, NewFields)
        )
    end,

  FieldsRef =
    lists:foldl(
      fun
        (<<".readgroups">>,Acc)->
          % readgroups is always active
          Acc;
        (F, Acc)->
        case Acc of
          #{F:=Count} ->
            Acc#{ F=> Count + 1 };
          _->
            Acc#{ F=>1 }
        end
      end,
      FieldsRef0,
      SubsFields),

  Object0#object{
    fields = Fields,
    fields_ref = FieldsRef
  }.

remove_fields(
    SubsFields,
    Object0 = #object{
      fields = Fields0,
      fields_ref = FieldsRef0
    }
)->

  FieldsRef =
    lists:foldl(
      fun
        (<<".readgroups">>, Acc)->
          % readgroups is always active
          Acc;
        (F, Acc)->
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
      SubsFields),

  Fields = maps:with(maps:keys(FieldsRef), Fields0),
  Object0#object{
    fields = Fields,
    fields_ref = FieldsRef
  }.


add_object_client(
    ClientID,
    SubsID,
    UserGroups,
    Object = #object{
      clients = ObjectClients
    }
)->

  case maps:is_key(ClientID, ObjectClients) of
    true ->
      add_object_client_sub(ClientID, SubsID, Object);
    _->
      init_object_client(ClientID, SubsID, UserGroups, Object)
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
    UserGroups,
    Object0 = #object{
      fields = #{
        <<".readgroups">> := RG
      },
      clients = Clients0
    }
)->
  %-----------Add new client-------------------
  Access = check_access(UserGroups, RG),
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
      conditions = {<<".oid">>,'=',OID},
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
     conditions = {<<".oid">>,'=',OID},
     client = ClientID,
     id = SubsID,
     usergroups = UG,
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
    usergroups = UG,
    subs = Subs
  },

  Clients = Clients0#{
    ClientID => Client
  },
  State0#state{
    clients = Clients
  }.

remove_client(
    ClientID,
    SubsID,
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
      Subs = maps:remove(SubsID, Subs0),
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
      conditions = {<<".oid">>,'=',OID},
      client = ClientID,
      id = SubsID
    },
    #state{
      objects = Objects,
      clients = Clients
    }
)->
  #object{
    instance = Instance,
    fields = Fields,
    clients = #{
      ClientID := #o_client{
        access = HasAccess
      }
    }
  } = maps:get(OID, Objects),

  if
    HasAccess =:= true ->
      % Send create to the client
      #client{
        subs = #{
          SubsID:=#o_sub{
            read = Read
          }
        }
      } = maps:get(ClientID, Clients),

      ActualValues = ecomet_query:query_object(Instance, Fields),
      Update = Read( ActualValues ),

      catch ClientID ! ?SUBSCRIPTION(SubsID, create, OID, Update),
      ok;
    true->
      % The the client has no access to the object
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
      Object1 =
        case Acc of
          #{ OID := Object0 }->
            add_fields(SubsFields, Object0);
          _->
            init_new_object(OID, SubsFields)
        end,
      Object = add_query_to_object(Ref, Object1),
      Acc#{
        OID => Object
      }
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


trigger_object_create(
    #object{
      instance = Instance,
      fields = Fields = #{
        <<".readgroups">> := RG
      }
    },
    ClientID,
    #q_client{
      subs_id = SubsID,
      read = Read,
      usergroups = UG
    }
)->
  case check_access(UG, RG) of
    true->
      ActualValues = ecomet_query:query_object(Instance, Fields),
      Update = Read( ActualValues ),
      OID = ecomet_object:get_oid(Instance),

      catch ClientID ! ?SUBSCRIPTION(SubsID, create, OID, Update);
    _->
      % The the client has no access to the object
      ignore
  end.

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
check_access(UG, RG)->
  case ordsets:intersection(UG, RG) of
    [] -> false;
    _->true
  end.

has_clients(#object{
  clients = Clients,
  queries = Queries
})->
  map_size(Clients) > 0 orelse (not ordsets:is_empty(Queries));

has_clients(#query{
  clients = Clients
})->
  map_size(Clients) > 0.











