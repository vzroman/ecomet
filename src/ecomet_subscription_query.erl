

-module(ecomet_subscription_query).

-include("ecomet.hrl").
-include("ecomet_subscription.hrl").

-define(CALL_TIMEOUT, 60000).
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
  key2ref,
  clients
}).

-record(query,{
  key,
  count,
  index
}).

-record(client,{
  monitor_ref,
  subs
}).

-record(index,{
  tag,
  '&',
  '!',
  db
}).

-define(key(S),#query_id{
  dbs = S#subscribe.dbs,
  fields = S#subscribe.deps,
  conditions = S#subscribe.conditions
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

  % Prepare the storage for index
  ets:new(?S_INDEX,[
    named_table,
    protected,
    set,
    {read_concurrency, true},
    {write_concurrency,true}
  ]),

  {ok, #state{
    queries = #{},
    key2ref = #{},
    clients = #{}
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
handle_call(Request, From, State) ->
  ?LOGWARNING("unexpected call request ~p from ~p", [Request, From]),
  {reply, ok, State}.

handle_cast({unsubscribe, Client, SubsID}, State0) ->
  try
    State = remove_subscription(Client, SubsID, State0),
    {noreply, State}
  catch
    _:E:S->
      ?LOGERROR("remove object subscription error: ~p, stack ~p",[E,S]),
      {noreply, State0}
  end;

handle_cast(Request,State) ->
  ?LOGWARNING("unexpected cast request ~p", [Request]),
  {noreply, State}.

handle_info({'DOWN', _Ref, process, Client, _Reason}, State0) ->
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
%  Add new subscription
%-------------------------------------------------------------------
add_subscription(
    Subscription,
    State0
)->

  State1 = add_query_client(Subscription, State0),
  State = add_client(Subscription, State1),

  State.

remove_subscription(
    ClientID,
    SubsID,
    State0
)->

  {State1, Ref} = remove_client(ClientID, SubsID, State0),
  State = remove_query_client(ClientID, SubsID, Ref, State1),

  State.

add_query_client(
    Subscription,
    State0 = #state{
      key2ref = Key2Ref
    }
)->
  Key = ?key(Subscription),
  case Key2Ref of
    #{Key := Ref} ->
      add_query_client(Ref, Subscription, State0);
    _->
      init_query(Key, Subscription, State0)
  end.

add_query_client(
    Ref,
    Subscription,
    State0 = #state{
      queries = Queries0
    }
)->

  % Add new subscription to the existing query
  Query0 = #query{
    count = Count0
  } = maps:get(Ref, Queries0),

  Query = Query0#query{
    count = Count0 + 1
  },
  Queries = Queries0#{
    Ref => Query
  },

  ecomet_subscription_object:add_query_client(Ref, Subscription),

  State0#state{
    queries = Queries
  }.

init_query(
    Key,
    Subscription0 = #subscribe{
      conditions = Conditions0,
      dbs = DBs
    },
    State0 = #state{
      key2ref = Key2Ref0,
      queries = Queries0
    }
)->
  %-------Create new query---------------------
  Ref = make_ref(),

  Conditions = compile_conditions( Conditions0 ),

  % Prepare query index
  Tags = ecomet_resultset:subscription_prepare( Conditions ),
  IndexDBs =
    if
      is_list( DBs )-> ordsets:from_list(DBs);
      true -> DBs
    end,
  Index = compile_index( Tags, IndexDBs ),
  build_index(Index, Ref),

  % Prepare initial set
  InitSet = ecomet_query:get(DBs,rs,Conditions0),
  Subscription = Subscription0#subscribe{
    conditions = Conditions
  },

  ecomet_subscription_object:init_query(Ref, Subscription, InitSet),

  Query = #query{
    key = Key,
    count = 1,
    index = Index
  },

  Queries = Queries0#{
    Ref => Query
  },

  Key2Ref = Key2Ref0#{
    Key => Ref
  },

  State0#state{
    key2ref = Key2Ref,
    queries = Queries
  }.

remove_query_client(
    ClientID,
    SubsID,
    Ref,
    State0 = #state{
      key2ref = Key2Ref0,
      queries = Queries0= #{
        Ref := Query0= #query{
          key = Key,
          count = Count0,
          index = Index
        }
      }
    }
)->
  Count = Count0 - 1,
  Queries =
    if
      Count > 0->
        Query = Query0#query{
          count = Count
        },
        Queries0#{
          Ref => Query
        };
      true ->
        destroy_index(Index, Ref),
        maps:remove(Ref, Queries0)
    end,

  Key2Ref =
    case maps:is_key(Ref, Queries) of
      true -> Key2Ref0;
      _-> maps:remove(Key, Key2Ref0)
    end,

  ecomet_subscription_object:remove_query_client(Ref, ClientID, SubsID),

  State0#state{
    queries = Queries,
    key2ref = Key2Ref
  }.

add_client(
    Subscription = #subscribe{
      client = ClientID,
      id = SubsID
    },
    State0 = #state{
      key2ref = Key2Ref,
      clients = Clients0 = #{
        ClientID := Client0 = #client{
          subs = Subs0
        }
      }
    }
)->
  Ref = maps:get(?key(Subscription), Key2Ref),
  % Add new subscription to the existing client
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
    Subscription = #subscribe{
      client = ClientID,
      id = SubsID
    },
    State0 = #state{
      key2ref = Key2Ref,
      clients = Clients0
    }
)->
  % Add new client
  Ref = maps:get(?key(Subscription), Key2Ref),
  Subs = #{
    SubsID => Ref
  },

  MonitorRef = erlang:monitor(process, ClientID),

  Client = #client{
    monitor_ref = MonitorRef,
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
      clients = Clients0 = #{
        ClientID := Client0 =#client{
          monitor_ref = MonitorRef,
          subs = Subs0 #{
            SubsID := Ref
          }
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
      true ->
        erlang:demonitor(MonitorRef),
        maps:remove(ClientID, Clients0)
    end,

  State = State0#state{
    clients = Clients
  },

  {State, Ref};

remove_client(ClientID, SubsID, State)->
  ?LOGWARNING("Attempt to remove not registered subscription client pid ~p, subscription id ~p",[
    ClientID, SubsID
  ]),
  {State, undefined}.

destroy_client(
    ClientID,
    State0 = #state{
      clients = #{
        ClientID := #client{
          subs = Subs
        }
      }
    }
)->
  lists:foldl(
    fun(SubsID, Acc)->
      remove_subscription(ClientID, SubsID, Acc)
    end,
    State0,
    maps:keys(Subs)
  ).

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

