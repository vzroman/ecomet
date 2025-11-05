

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
  add_subscription_call/2,
  add_subscription_cast/2
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
  usergroups,
  subs
}).

-record(sub,{
  deps,
  read,
  no_feedback
}).

%%=================================================================
%%        API
%%=================================================================
add_subscription_call(OID, Subscription=#subscription{})->
  gen_server:call(?WORKER(OID), {add_subscription, OID, Subscription}, ?CALL_TIMEOUT).

add_subscription_cast(OID, Subscription=#subscription{})->
  gen_server:cast(?WORKER(OID), {add_subscription, OID, Subscription}).



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
handle_call({add_subscription, OID, Subscription}, _From, State0) ->

  try
    State = add_subscription(OID, Subscription, _Update = #{}, State0),
    {reply, ok, State}
  catch
    _:E:S->
      ?LOGERROR("add object subscription error: ~p, stack ~p",[E,S]),
      {reply, {error, E}, State}
  end.

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
    _OID,
    _Update,
    #subscription{
      id = SubsID,
      client = Client
    },
    #state{
      clients = #{ Client := #{ SubsID:=_ } }
    } =State)->

  % The subscription already exists, ignore
  State;

add_subscription(OID, Update, Subscription, State0)->

  State1 = add_object( OID, Subscription, Update, State0),

  State2 = check_access( OID, Subscription, State1 ),

  add_client( OID, Subscription, State2 ).


%-------------------------------------------------------------------
%  State transformations
%-------------------------------------------------------------------
%---------Extend already active object---------------
add_object(
    OID,
    #subscription{
      deps = Deps
    },
    Update,
    State0 = #state{
      objects = Objects0 =#{
        OID:= Object0 = #object{
          instance = Instance,
          fields = Fields0
        }
      }
    })->

  NewFields = Deps -- maps:keys( Fields0 ),
  ToReadFields = NewFields -- maps:keys(Update),
  ReadValues =
    if
      map_size(ToReadFields) > 0 ->
        ecomet_object:read_fields( Instance, ToReadFields );
      true ->
        #{}
    end,
  Values = maps:merge( ReadValues, Update ),

  Fields =
    lists:foldl(
      fun(F, Acc)->
        Acc#{F => #field{
          value = maps:get(F, Values),
          clients = #{}
        }}
      end,
      Fields0,
      NewFields),

  State0#state{
    objects = Objects0#{
      OID => Object0#object{
        fields = Fields
      }
    }
  };

%---------Append new object---------------
add_object(
    OID,
    Subscription,
    Update,
    State0 = #state{
      objects = Objects0
    })->

  Instance = ecomet_object:construct( OID ),
  #{ <<".readgroups">> := ReadGroups } = ecomet:read_fields( Instance, #{ <<".readgroups">> => [] }),
  State = State0#state{
    objects = Objects0#{
      OID => #object{
        instance = Instance,
        read_groups = ordsets:from_list(ReadGroups),
        fields = #{}
      }
    }
  },

  add_object( OID, Subscription, Update, State ).

check_access(
    _OID,
    #subscription{
      usergroups = is_admin
    },
    State
)->
  % The client has admin rights
  State;

check_access(
    OID,
    #subscription{
      client = Client,
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
      DeniedClients0 = maps:get( OID, AccessDenied0, #{}),
      DeniedClients = DeniedClients0#{ Client => true },
      AccessDenied = AccessDenied0#{ OID => DeniedClients },
      State0#state{
        access_denied = AccessDenied
      };
    _->
      % The client has access to the object
      State0
  end.



add_client(
    OID,
    #subscription{
      id = SubsID,
      client = ClientPID,
      usergroups = UserGroups,
      deps = Deps,
      read = Read,
      params = #{
        no_feedback := NoFeedback
      }
    },
    State0 = #state{
      objects = Objects,
      clients = Clients0
    }
)->

  Client0 =
    maps:get(ClientPID, Clients0, #client{ usergroups = UserGroups, subs = #{} }),

  Subs0 = Client0#client.subs,
  SubObjects0 = maps:get(SubsID, Subs0, #{}),

  SubObjects = SubObjects0#{
    OID => #sub{
      deps = Deps,
      read = Read,
      no_feedback = NoFeedback
    }
  },

  Subs = Subs0#{
    SubsID => SubObjects
  },

  Client = Client0#client{
    subs = Subs
  },

  if
    NoFeedback =:= false ->
      todo;
    true ->
      #object{
        instance = Instance,
        fields = Fields
      } = maps:get(OID, Objects),
      Values = actual_values( Deps, Fields ),
      Update = ecomet_query:query_object(Instance, Values),
      catch ClientPID ! ?SUBSCRIPTION(SubsID, create, OID, Update)
  end,

  State0#state{
    clients = Clients0#{
      ClientPID => Client
    }
  }.

actual_values(Needed, Fields)->
  lists:foldl(
    fun(F, Acc)->
      #field{value = V} = maps:get(F, Fields),
      Acc#{ F=> V }
    end,
    #{},
    Needed).








