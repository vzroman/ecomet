

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
  unsubscribe/2,
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
  queries
}).

-record(object,{
  instance,
  clients,
  queries,
  fields,
  fields_ref
}).

-record(client,{
  monitor,
  usergroups,
  subs
}).

-record(clt,{
  access,
  subs
}).

-record(sub,{
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

global_set(Tag)->
  [gen_server:cast(?NAME(N), {global_set, Tag}) || N <-ecomet_subscription_pool:get_workers()],
  ok.

global_reset(Tag)->
  [gen_server:cast(?NAME(N), {global_reset, Tag}) || N <-ecomet_subscription_pool:get_workers()],
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
    queries = #{}
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
      {reply, {error, E}, State}
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
    #subscribe{
      id = SubsID,
      client = Client
    },
    #state{
      clients = #{ Client := #client{
        subs = #{
          SubsID := _
        }
      } }
    } =State)->

  % The subscription already exists, ignore
  State;

add_subscription(
    Subscription,
    State0
)->

  State1 = add_object(Subscription, State0),
  State = add_client(Subscription, State1),
  init_subscription(Subscription, State),

  State.


%-------------------------------------------------------------------
%  Remove subscription
%-------------------------------------------------------------------
unsubscribe(
    ClientID,
    SubsID,
    State0 = #state{
      clients = #{
        ClientID := #client{
          subs = #{
            SubsID := _
          }
        }
      }
    }
)->

  State1 = remove_object(ClientID, SubsID, State0),
  State = remove_client(ClientID, SubsID, State1),

  State;

unsubscribe(
    _ClientID,
    _SubsID,
    State
)->
  % The subscription doesn't exist
  State.

%-------------------------------------------------------------------
%  State transformations
%-------------------------------------------------------------------
add_object(
    #subscribe{
      conditions = {<<".oid">>,'=',OID},
      client = ClientID,
      id = SubsID,
      usergroups = UserGroups,
      deps = SubsFields
    },
    State0 = #state{
      objects = Objects0 = #{
        OID := Object0
      }
    }
)->
  %---------Update already existing object---------------
  Object1 = add_fields(SubsFields, Object0),
  Object = add_object_client( ClientID, SubsID, UserGroups, Object1 ),

  Objects = Objects0#{
    OID => Object
  },

  State0#state{
    objects = Objects
  };

add_object(
    Subscription = #subscribe{
      conditions = {<<".oid">>,'=',OID},
      deps = SubsFields
    },
    State0 = #state{
      objects = Objects0
    }
)->
  %---------Init new object---------------
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

  Fields = ecomet:read_field(Instance, InitFields),
  FieldsRef = maps:map(fun(_F,_V)->1 end, Fields),

  Object = #object{
    instance = Instance,
    fields = Fields,
    fields_ref = FieldsRef,
    clients = #{},
    queries = ordsets:new()
  },

  Objects = Objects0#{
    OID => Object
  },

  add_object(
    Subscription,
    State0 = #state{
      objects = Objects
    }
  ).

remove_object(
    ClientID,
    SubsID,
    State0 = #state{
      clients = #{
        ClientID := #client{
          subs = #{
            SubsID:=#sub{
              fields = SubsFields,
              oid = OID
            }
          }
        }
      },
      objects = Objects0 =#{
        OID := Object0
      }
    }
)->

  Object1 = remove_object_client(ClientID, SubsID, Object0),
  Object = remove_fields(SubsFields, Object1),

  Objects =
    if
      map_size(Object#object.clients) > 0->
        Objects0#{
          OID => Object
        };
      true->
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
    _UserGroups,
    Object0 = #object{
      clients = Clients0 = #{
        ClientID := Client0 = #clt{
          subs = Subs0
        }
      }
    }
)->
  %-----------Add subscription to the existing client------------
  Subs = ordsets:add_element(SubsID, Subs0),
  Client = Client0#clt{
    subs = Subs
  },
  Clients = Clients0#{
    ClientID => Client
  },
  Object0#object{
    clients = Clients
  };

add_object_client(
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
  Client = #clt{
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
      clients = Clients0 = #{
        ClientID := Client0 = #clt{
          subs = Subs0
        }
      }
    }
)->
  Subs = ordsets:del_element(SubsID, Subs0),
  Clients =
    case ordsets:size(Subs) of
      0 ->
        maps:remove(ClientID, Clients0);
      _->
        Client = Client0#clt{
          subs = Subs
        },
        Clients0#{
          ClientID = Client
        }
    end,

  Object0#object{
    clients = Clients
  }.



add_client(
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
      clients = Clients0 = #{
        ClientID := Client0= #client{
          subs = Subs0
        }
      }
    }
)->
  %----------add subscription to the existing client--------------
  Sub = #sub{
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
  };

add_client(
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
  Sub = #sub{
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
      clients = Clients0= #{
        ClientID := Client0=#client{
          monitor = MonitorRef,
          subs = Subs0
        }
      }
    }
)->
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
  }.

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
      client = ClientID
    },
    #state{
      objects = #{
        OID:=#object{
          clients = #{
            ClientID := #clt{
              access = false
            }
          }
        }
      }
    }
)->
  % The the client has no access to the object
  ignore;

init_subscription(
    #subscribe{
      conditions = {<<".oid">>,'=',OID},
      client = ClientID,
      id = SubsID
    },
    #state{
      objects = #{
        OID:=#object{
          instance = Instance,
          fields = Fields
        }
      },
      clients = #{
        ClientID := #client{
          subs = #{
            SubsID:=#sub{
              read = Read
            }
          }
        }
      }
    }
)->
  % Send create to the client
  ActualValues = ecomet_query:query_object(Instance, Fields),
  Update = Read( ActualValues ),

  catch ClientID ! ?SUBSCRIPTION(SubsID, create, OID, Update),
  ok.

check_access(is_admin, _RG)->
  true;
check_access(UG, RG)->
  case ordsets:intersection(UG, RG) of
    [] -> false;
    _->true
  end.










