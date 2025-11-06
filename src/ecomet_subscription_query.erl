

-module(ecomet_subscription_query).

-include("ecomet.hrl").
-include("ecomet_subscription.hrl").

-define(CALL_TIMEOUT, 60000).
-define(S_QUERY,ecomet_subscriptions_query).
-define(S_INDEX,ecomet_subscriptions_index).

%%=================================================================
%%        API
%%=================================================================
-export([
  subscribe/1,
  unsubscribe/2
]).

%%=================================================================
%%        OTP API
%%=================================================================
-export([
  start_link/0,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-record(state,{
  queries,
  clients
}).

-record(key,{
  dbs,
  fields,
  conditions
}).

-record(query,{
  ref,
  conditions,
  fields,
  clients
}).

-record(q,{
  ref,
  set
}).

-record(client,{
  monitor,
  subs
}).

-record(clt,{
  usergroups,
  subs
}).

-record(sub,{
  read,
  no_feedback,
  query
}).

-record(index,{
  tag,
  '&',
  '!',
  db
}).

%%=================================================================
%%        API
%%=================================================================
subscribe(Subscription=#subscribe{ })->
  gen_server:call(?MODULE, Subscription, ?CALL_TIMEOUT).

unsubscribe(Client, SubsID)->
  gen_server:cast(?MODULE, {unsubscribe, Client, SubsID}),
  ok.

%%=================================================================
%%        OTP
%%=================================================================
start_link()->
  gen_server:start_link(?MODULE, ?MODULE, [], []).


init([]) ->

  % Prepare the storage for query subscriptions
  ets:new(?S_QUERY,[
    named_table,
    protected,
    set,
    {read_concurrency, true},
    {write_concurrency,true}
  ]),

  % Prepare the storage for index
  ets:new(?S_INDEX,[
    named_table,
    protected,
    set,
    {read_concurrency, true},
    {write_concurrency,true}
  ]),

  {ok, #state{
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
handle_call(Request, From, State) ->
  ?LOGWARNING("unexpected call request ~p from ~p", [Request, From]),
  {reply, ok, State}.

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
    Subscription,
    State0
)->

  State1 = add_query(Subscription, State0),
  State = add_client(Subscription, State1),
  init_subscription(Subscription, State),

  State.

add_query(
    Subscription = #subscribe{
      client = Client,
      id = SubsID,
      conditions = Conditions,
      deps = Fields,
      dbs = DBs,
      params = #{
        stateless := Stateless
      }
    },
    State = #state{
      queries = #{
        #key{dbs = DBs, fields = Fields, conditions = Conditions} := #q{
          ref = Ref,
          set = Set
        }
      }
    }
)->
  %--------Add client to the existing query-------------
  Query = add_query_client(Subscription, get_query(Ref)),
  save_query(Ref, Query),
  if
    Stateless ->
      ignore;
    true->
      ecomet_subscription_object:init_query_client(Client, SubsID, Query, Set)
  end,

  State;

add_query(
    Subscription = #subscribe{
      conditions = Conditions0,
      deps = Fields,
      dbs = DBs,
      params = #{
        stateless := Stateless
      }
    },
    State0 = #state{
      queries = Queries0
    }
)->
  %-------Create new query---------------------
  Ref = make_ref(),
  Conditions = compile_conditions( Conditions0 ),

  Query0 = #query{
    ref = Ref,
    conditions = Conditions,
    fields = Fields,
    clients =#{}
  },

  Query = add_query_client(Subscription, Query0),

  % Prepare query index
  Tags = ecomet_resultset:subscription_prepare( Conditions ),
  IndexDBs =
    if
      is_list( DBs )-> ordsets:from_list(DBs);
      true -> DBs
    end,
  Index = compile_index( Tags, IndexDBs ),

  % Prepare initial set
  Set = ecomet_query:get(DBs,rs,Conditions0),

  save_query(Ref, Query),
  build_index(Index, Ref),
  ecomet_subscription_object:init_query(Query, Set, Stateless),

  Q = #q{
    ref = Ref,
    set = Set
  },

  Key = #key{
    dbs = DBs,
    fields = Fields,
    conditions = Conditions0
  },

  Queries = Queries0#{
    Key => Q
  },

  State0#state{
    queries = Queries
  }.

add_query_client(
    #subscribe{
      client = ClientID,
      id = SubsID,
      read = Read,
      params = #{
        no_feedback:=NoFeedback
      }
    },
    Query0 = #query{
      ref = Ref,
      clients = Clients0 = #{
        ClientID := Client0 = #clt{
          subs = Subs0
        }
      }
    }
)->
  %----------add subscription to the existing client------------
  Sub = #sub{
    read = Read,
    no_feedback = NoFeedback,
    query = Ref
  },

  Subs = Subs0#{
    SubsID => Sub
  },

  Client = Client0#clt{
    subs = Subs
  },
  Clients = Clients0#{
    ClientID => Client
  },

  Query0#query{
    clients = Clients
  }.

add_client(
    #subscribe{
      client = ClientID,
      id = SubsID,
      dbs = DBs,
      deps = Fields,
      conditions = Conditions
    },
    State0=#state{
      queries = #{
        #key{dbs=DBs, fields = Fields, conditions = Conditions} := #q{
          ref = Ref
        }
      },
      clients = Clients0 = #{
        ClientID := Client0 = #client{
          subs = Subs0
        }
      }
    }
)->
  %---------Add subscription to the existing client-----------
  Subs = Subs0#{
    SubsID => Ref
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
      client = ClientID,
      id = SubsID,
      dbs = DBs,
      deps = Fields,
      conditions = Conditions
    },
    State0=#state{
      queries = #{
        #key{dbs=DBs, fields = Fields, conditions = Conditions} := #q{
          ref = Ref
        }
      },
      clients = Clients0
    }
)->
  %----------Add new client------------------------
  Subs = #{
    SubsID => Ref
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
      dbs = DBs,
      deps = Fields,
      conditions = Conditions
    },
    #state{
      queries = #{
        #key{ dbs = DBs, fields = Fields, conditions = Conditions } = #q{
          ref = Ref,
          set = Set
        }
      }
    }
)->
  % Trigger subscription create
  Query = get_query(Ref),

  ok.

%%------------------------------------------------------------
%%  Search engine
%%------------------------------------------------------------
compile_conditions({<<".pattern">>,'=',PatternID})->
  Patterns = [PatternID|ecomet_pattern:get_children_recursive(PatternID)],
  {'OR',[{<<".pattern">>,':=',P}||P<-Patterns]};
compile_conditions({<<".path">>,'=',Path})->
  {<<".oid">>,'=',?OID(Path)};
compile_conditions({'AND',Conditions})->
  {'AND',[ compile_conditions(C) || C <- Conditions ]};
compile_conditions({'OR',Conditions})->
  {'OR',[ compile_conditions(C) || C <- Conditions ]};
compile_conditions({'ANDNOT',C1,C2})->
  {'ANDNOT',compile_conditions(C1),compile_conditions(C2)};
compile_conditions(Condition)->
  Condition.

compile_index([{[Tag|_]=And,Not}|Rest], DBs)->
  [#index{
    tag = Tag,
    '&' = ?NEW_SET(And),
    '!'= ?NEW_SET(Not),
    db = DBs } | compile_index( Rest, DBs ) ];
compile_index([], _DBs)->
  [].

build_index([#index{tag = Tag}=Index|Rest], ID)->
  case ets:lookup(?S_INDEX,{tag,Tag}) of
    [{_,Indexes}]->
      case Indexes of
        #{Index := Subscribers} ->
          ets:insert(?S_INDEX,{ {tag,Tag}, Indexes#{ Index => ordsets:add_element(ID,Subscribers) }});
        _->
          ets:insert(?S_INDEX,{ {tag,Tag}, Indexes#{ Index => [ID] }})
      end;
    []->
      ets:insert(?S_INDEX,{ {tag,Tag}, #{ Index => [ID] }}),
      global_set(Tag)
  end,
  build_index(Rest, ID);
build_index([], _ID)->
  ok.

destroy_index([#index{tag = Tag}=Index|Rest], ID )->
  case ets:lookup(?S_INDEX,{tag,Tag}) of
    [{_,Indexes}]->
      case Indexes of
        #{Index := Subscribers}->
          case ordsets:del_element( ID, Subscribers ) of
            [] ->
              Indexes1 = maps:remove(Index, Indexes),
              case maps:size( Indexes1 ) of
                0 ->
                  ets:delete(?S_INDEX,{tag,Tag}),
                  global_reset( Tag );
                _->
                  ets:insert(?S_INDEX, { {tag,Tag}, Indexes1 })
              end;
            Subscribers1->
              ets:insert(?S_INDEX, { {tag,Tag}, Indexes#{ Index => Subscribers1 } })
          end;
        _->
          case maps:size(Indexes) of
            0->
              ets:delete(?S_INDEX,{tag,Tag}),
              global_reset( Tag );
            _->
              ignore
          end
      end;
    []->
      global_reset(Tag)
  end,
  destroy_index(Rest, ID);
destroy_index([], _ID)->
  ok.

global_set(Tag)->
  ecomet_subscription_object:global_set( Tag ).

global_reset(Tag)->
  ecomet_subscription_object:global_reset( Tag ).

%---------------------------------------------------------
% Utilities
%---------------------------------------------------------
get_query( Ref )->
  [{_, Query}] = ets:lookup(?S_QUERY, Ref),
  Query.

save_query(Ref, Query)->
  ets:insert(?S_QUERY, {Ref, Query}).
