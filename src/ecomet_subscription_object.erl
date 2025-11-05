

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
  add_subscription_call/1,
  add_subscription_cast/2,
  remove_subscription/2
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
  access_denied
}).

-record(object,{
  instance,
  read_groups,
  fields
}).

-record(field,{
  value,
  clients
}).

-record(client,{
  monitor,
  usergroups,
  subs
}).

-record(sub,{
  fields,
  read,
  no_feedback,
  objects
}).

%%=================================================================
%%        API
%%=================================================================
add_subscription_call(
    Subscription=#subscription{
      conditions = {<<".oid">>,'=',OID}
    }
)->
  gen_server:call(?WORKER(OID), {add_subscription, Subscription, _Updpate=#{}}, ?CALL_TIMEOUT).

add_subscription_cast(
    Subscription=#subscription{
      conditions = {<<".oid">>,'=',OID}
    },
    Updates
)->
  gen_server:cast(?WORKER(OID), {add_subscription, Subscription, Updates}).

remove_subscription(Client, SubsID)->
  PoolSize = ecomet_subscription_pool:get_size(),
  [gen_server:cast(?NAME(N), {remove_subscription, Client, SubsID}) || N <- lists:seq(0, PoolSize-1)],
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
    access_denied = #{}
  }}.

%%===================================================================
%% OTP generic server callbacks
%%===================================================================
handle_call({add_subscription, Subscription, Update}, _From, State0) ->

  try
    State = add_subscription(Subscription, Update, State0),
    {reply, ok, State}
  catch
    _:E:S->
      ?LOGERROR("add object subscription error: ~p, stack ~p",[E,S]),
      {reply, {error, E}, State}
  end;
handle_call(Request, _From, State) ->
  ?LOGWARNING("unexpected call request ~p", [Request]),
  {noreply, State}.

handle_cast({add_subscription, Subscription, Updates}, State0) ->
  try
    State = add_subscription(Subscription, Updates, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("add object subscription error: ~p, stack ~p",[E,S]),
      {noreply, State}
  end;

handle_cast({remove_subscription, Client, SubsID}, State0) ->
  try
    State = remove_subscription(Client, SubsID, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("remove object subscription error: ~p, stack ~p",[E,S]),
      {noreply, State}
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
%  Add new subscription
%-------------------------------------------------------------------
add_subscription(
    _Update,
    #subscription{
      conditions = {<<".oid">>,'=',OID},
      id = SubsID,
      client = Client
    },
    #state{
      clients = #{ Client := #client{
        subs = #{
          SubsID := #{ OID := _ }
        }
      } }
    } =State)->

  % The subscription already exists, ignore
  State;

add_subscription(
    Subscription,
    Update,
    State0
)->

  State1 = add_object_subscription(Subscription, Update, State0),

  State2 = add_object_access(Subscription, State1),

  add_client(Subscription, State2).

%-------------------------------------------------------------------
%  Remove subscription
%-------------------------------------------------------------------
remove_subscription(
    ClientPID,
    SubsID,
    State0 = #state{
      objects = Objects0,
      clients = Clients0 = #{
        ClientPID := Client0 = #client{
          monitor = MonitorRef,
          subs = Subs0 = #{
            SubsID := SubObjects
          }
        }
      }
    }
)->

  %-----------Remove subscription from objects collection---------------
  Objects =
    maps:fold(
      fun(OID, #sub{ deps = Deps}, ObjectsAcc)->

        Object0 = #object{
          fields = Fields0
        }= maps:get(OID, ObjectsAcc),

        Fields =
          lists:foldl(
            fun(F, FieldsAcc)->
              Field0 = #field{
                clients = FieldClients0
              } = maps:get(F, FieldsAcc),

              ClientFieldSubs0 = maps:get(ClientPID, FieldClients0),
              ClientFieldSubs = ordsets:del_element(SubsID, ClientFieldSubs0),

              FieldClients =
                if
                  length(ClientFieldSubs) >0 ->
                    FieldClients0#{ ClientPID => ClientFieldSubs };
                  true ->
                    maps:remove(ClientPID, FieldClients0)
                end,

              if
                map_size( FieldClients )>0->
                  Field = Field0#field{
                    clients = FieldClients
                  },
                  FieldsAcc#{
                    F => Field
                  };
                true ->
                  maps:remove(F, FieldsAcc)
              end
            end,
            Fields0,
            Deps),

        if
          map_size( Fields ) > 0 ->
            Object = Objects0#object{
              fields = Fields
            },
            ObjectsAcc#{
              OID => Object
            };
          true ->
            maps:remove(OID, ObjectsAcc)
        end
      end,
      Objects0,
      SubObjects),

  %-----------Remove subscription from clients collection---------------
  Subs = maps:remove(SubsID, Subs0),
  Clients =
    if
      map_size(Subs) > 0 ->
        Client = Client0#client{
          subs = Subs
        },
        Clients0#{ ClientPID => Client };
      true ->
        elang:demonitor(MonitorRef),
        maps:remove( ClientPID, Clients0)
    end,

  State0#state{

  }.


remove_subscription(Client, SubsID, State )->
  % The subscription is not registered, ignore
  ?LOGDEBUG("remove unregistered subscription, client PID ~p, subscription id ~p",[Client, SubsID]),
  State.





%-------------------------------------------------------------------
%  State transformations
%-------------------------------------------------------------------
add_object_subscription(
    #subscription{
      conditions = {<<".oid">>,'=',OID},
      client = ClientID,
      id = SubsID,
      deps = SubsFields
    },
    Update,
    State0 = #state{
      objects = Objects0 = #{
        OID := Object0
      }
    }
)->
  %---------Update already existing object---------------
  Object1=init_fields(SubsFields, Update, Object0),
  Object = add_fields_subscription(SubsFields, ClientID, SubsID, Object1),


  Objects = Objects0#{
    OID => Object
  },

  State0#state{
    objects = Objects
  };

add_object_subscription(
    Subscription = #subscription{
      conditions = {<<".oid">>,'=',OID},
      client = ClientID,
      id = SubsID,
      deps = SubsFields
    },
    Update,
    State0 = #state{
      objects = Objects0
    }
)->
  %---------Init new object---------------
  Instance = ecomet_object:construct( OID ),
  ReadGroups =
    case ecomet:read_field( Instance, <<".readgroups">>) of
      {ok, RG} when is_list(RG)-> RG;
      _->[]
    end,

  Object = #object{
    instance = Instance,
    read_groups = ReadGroups,
    fields = #{}
  },

  Objects = Objects0#{
    OID => Object
  },

  add_object_subscription(
    Subscription,
    Update,
    State0 = #state{
      objects = Objects
    }
  ).

init_fields(
    Fields,
    Update,
    Object0 = #object{
      instance = Instance,
      fields = ObjectFields0
    }
)->

  NewFields = Fields -- maps:keys( ObjectFields0 ),
  ToReadFields = NewFields -- maps:keys(Update),
  ReadValues =
    if
      map_size(ToReadFields) > 0 ->
        ecomet_object:read_fields(Instance, ToReadFields);
      true ->
        #{}
    end,
  Values = maps:merge( ReadValues, Update ),

  ObjectFields = lists:foldl(
    fun(F, Acc)->
      Acc#{
        F=>#field{
          value = maps:get(F, Values),
          clients = #{}
        }
      }
    end,
    ObjectFields0,
    NewFields),

  Object0#object{
    fields = ObjectFields
  }.

add_fields_subscription(
    Fields,
    ClientID,
    SubsID,
    Object0 = #object{
      fields = ObjectFields0
    }
)->

  ObjectFields = lists:foldl(
    fun(F, Acc)->
      Field0 = maps:get(F, Acc),
      Field = add_field_client(ClientID, SubsID, Field0),
      Acc#{F => Field}
    end,
    ObjectFields0,
    Fields),

  Object0#object{
    fields = ObjectFields
  }.

add_field_client(
    ClientID,
    SubsID,
    Field0 = #field{
      clients = Clients0 = #{
        ClientID := ClientSubscriptions0
      }
    }
)->
  % Append subscription
  ClientSubscriptions = ordsets:add_element(SubsID, ClientSubscriptions0),
  Clients = Clients0#{
    ClientID => ClientSubscriptions
  };

add_field_client(
    ClientID,
    SubsID,
    Field0 = #field{
      clients = Clients0
    }
)->
  % Add new client
  Clients0#{
    ClientID => ordsets:from_list([SubsID])
  }.

add_object_access(
    #subscription{
      usergroups = is_admin
    },
    State
)->
  % The client has admin rights
  State;

add_object_access(
    #subscription{
      client = ClientID,
      usergroups = UserGroups
    },
    State0 = #state{
      objects = #{
        OID:=#object{
          read_groups = ReadGroups
        }
      },
      access_denied = AccessDenied0
    }
)->
  case ordsets:intersection( UserGroups, ReadGroups ) of
    []->
      DeniedClients0 = maps:get(OID, AccessDenied0, #{}),
      DeniedClients = DeniedClients0#{ClientID => true},
      AccessDenied = AccessDenied0#{OID => DeniedClients},
      State0#state{
        access_denied = AccessDenied
      };
    _->
      % The client has access to the object
      State0
  end.


add_client(
    Subscription,
    State0
)->
  State1 = init_client(Subscription, State0),

  State = add_client_subscription(Subscription, State1),

  trigger_subscription(Subscription, State),

  State.

init_client(
    #subscription{
      client = ClientID
    },
    State = #state{
      clients = #{ClientID := _}
    }
)->
  % The client already initialized
  State;

init_client(
    #subscription{
      client = ClientPID,
      usergroups = UserGroups
    },
    State0 = #state{
      clients = Clients0
    }
)->
  % add new client
  Ref = erlang:monitor(process, ClientPID),
  Client = #client{
    monitor = Ref,
    usergroups = UserGroups,
    subs = #{}
  },

  Clients = Clients0#{
    ClientPID => Client
  },

  State0#state{
    clients = Clients
  }.

add_client_subscription(
    Subscription = #subscription{
      client = ClientID
    },
    State0 = #state{
      clients = Clients0 = #{
        ClientID => Client0
      }
    }
)->

  Client1 = init_client_subscription(Subscription, Client0),
  Client = add_subscription_object(Subscription, Client1),

  Clients = Clients0#{
    ClientID => Client
  },

  State0#state{
    clients = Clients
  }.

init_client_subscription(
    #subscription{
      id = SubsID
    },
    Client = #client{
      subs = #{SubsID := #sub{}}
    }
)->
  % The subscription is already active
  Client;

init_client_subscription(
    #subscription{
      id = SubsID,
      deps = Fields,
      read = Read,
      params = #{
        no_feedback := NoFeedback
      }
    },
    Client0 = #client{
      subs = Subscriptions0
    }
)->

  Subscription = #sub{
    fields = Fields,
    read = Read,
    no_feedback = NoFeedback,
    objects = #{}
  },

  Subscriptions = Subscriptions0#{
    SubsID => Subscription
  },

  Client0#client{
    subs = Subscriptions
  }.

add_subscription_object(
    #subscription{
      conditions = {<<".oid">>,'=',OID},
      id = SubsID
    },
    Client0 = #client{
      subs = Subs0 = #{
        SubsID := Sub0 = #sub{
          objects = Objects0
        }
      }
    }
)->

  Objects = Objects0#{ OID => true },
  Sub = Sub0#sub{
    objects = Objects
  },
  Subs = Subs0#{
    SubsID => Sub
  },

  Client0#client{
    subs = Subs
  }.

trigger_subscription(
    #subscription{
      params = #{
        stateless := true
      }
    },
    _State
)->
  % The subscription is stateless
  ignore;

trigger_subscription(
    #subscription{
      client = ClientID,
      conditions = {<<".oid">>,'=',OID}
    },
    #state{
      access_denied = #{
        OID := #{ ClientID:=_ }
      }
    }
)->
  % The client doesn't have object access
  ignore;

trigger_subscription(
    #subscription{
      conditions = {<<".oid">>,'=',OID},
      client = ClientPID,
      id = SubsID
    },
    #state{
      objects = #{
        OID := #object{
          instance = Instance,
          fields = ObjectFields
        }
      },
      clients = #{
        ClientPID := #client{
          subs = #{
            SubsID:= #sub{
              read = Read,
              fields = SubscriptionFields
            }
          }
        }
      }
    }
)->

  ActualValues0 = actual_values( SubscriptionFields, ObjectFields ),
  ActualValues = ecomet_query:query_object(Instance, ActualValues0),
  Update = Read( ActualValues ),

  catch ClientPID ! ?SUBSCRIPTION(SubsID, create, OID, Update),

  ok.

actual_values(Needed, Fields)->
  lists:foldl(
    fun(F, Acc)->
      #field{value = V} = maps:get(F, Fields),
      Acc#{ F=> V }
    end,
    #{},
    Needed).








