%%----------------------------------------------------------------
%% Copyright (c) 2022 Faceplate
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%----------------------------------------------------------------
-module(ecomet_subscription).

-include("ecomet.hrl").
-include("ecomet_subscription.hrl").

%%=================================================================
%%	Service API
%%=================================================================
-export([
  on_init/0,
  get_subscriptions/1,
  on_commit/2
]).

%%=================================================================
%%	API
%%=================================================================
-export([
  subscribe/1,
  unsubscribe/1
]).

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  object_monitor/1,
  query_monitor/2
]).

-record(sub,{id, pid, ts, owner }).
-record(index,{tag,'&','!',db}).


-define(SUBSCRIPTIONS,ecomet_subscriptions).
-define(S_OBJECT,ecomet_subscriptions_object).
-define(S_OBJECT_INDEX,ecomet_subscriptions_object_index).
-define(S_QUERY,ecomet_subscriptions_query).
-define(S_INDEX,ecomet_subscriptions_index).
-define(M_KEY(OID,ID,Owner),{OID, ID, Owner}).

%%=================================================================
%%	Service API
%%=================================================================
on_init()->

  % Initialize subscriptions optimization
  ok = ecomet_router:on_init( ?ENV( router_pool_size, ?ROUTER_POOL_SIZE ) ),

  % Prepare the storage for sessions
  ets:new(?SUBSCRIPTIONS,[
    named_table,
    public,
    set,
    {keypos, #sub.id},
    {read_concurrency, true},
    {write_concurrency,auto}
  ]),

  % Prepare the storage for object subscriptions
  ets:new(?S_OBJECT,[
    named_table,
    public,
    set,
    {read_concurrency, true},
    {write_concurrency,auto}
  ]),

  % Prepare the storage for object subscriptions
  ets:new(?S_OBJECT_INDEX,[
    named_table,
    public,
    bag,
    {read_concurrency, true},
    {write_concurrency,auto}
  ]),

  % Prepare the storage for query subscriptions
  ets:new(?S_QUERY,[
    named_table,
    public,
    set,
    {read_concurrency, true},
    {write_concurrency,auto}
  ]),

  % Prepare the storage for index
  ets:new(?S_INDEX,[
    named_table,
    public,
    set,
    {read_concurrency, true},
    {write_concurrency,auto}
  ]),

  ok.

%%=================================================================
%%	API
%%=================================================================
subscribe(#subscription{
  id = ID,
  owner = Owner
} = Subscription)->
  PID =
    ecomet_user:spawn_session(fun()->
      case ets:insert_new(?SUBSCRIPTIONS,#sub{
        id = {Owner,ID},
        ts = ecomet_lib:ts(),
        pid = self(),
        owner = Owner
      }) of
        true->
          erlang:monitor(process, Owner),

          Owner ! {ok, self()},

          try do_subscribe( Subscription )
          after
            ets:delete(?SUBSCRIPTIONS,{Owner,ID})
          end;
        _->
          Owner ! {error, self(), {not_unique,ID}}
      end
    end),

  receive
    {ok,PID} -> ok;
    {error,PID,Error}->throw(Error)
  end.

do_subscribe(#subscription{
  conditions = {<<".path">>,'=',Path}
} = S )->
  do_subscribe(S#subscription{
    conditions = {<<".oid">>,'=',?OID(Path)}
  });
do_subscribe(#subscription{
  conditions = {<<".oid">>,'=',OID}
} = S)->
  % Object subscription
  case ecomet_object:open(OID) of
    not_exists->
      throw({not_exists, OID});
    {error,Error}->
      throw(Error);
    Object->
      subscribe_object(Object, S)
  end;
do_subscribe( Subscription )->
  subscribe_query( Subscription ).

unsubscribe(ID)->
  case ets:lookup(?SUBSCRIPTIONS,{self(),ID}) of
    [#sub{pid = PID,owner = Owner}]->
      PID ! {unsubscribe,Owner};
    _->
      ignore
  end,
  ok.

%%=================================================================
%%	SINGLE OBJECT SUBSCRIPTION
%%=================================================================
-record(monitor,{
  id,
  oid,
  object,
  read,
  deps,
  owner,
  no_feedback,
  self
}).
subscribe_object(Object, S)->

  Monitor = create_monitor(Object, S ),

  object_monitor(Monitor).

object_monitor(#monitor{id = ID, oid = OID, owner = Owner } = Monitor )->
  receive
    {delete, OID, Ignore}->
      if
        Ignore-> ignore;
        true -> catch Owner ! ?SUBSCRIPTION(ID,delete,OID,#{})
      end,
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    {unsubscribe, Owner}->
      destroy_monitor( Monitor ),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    {'DOWN', _Ref, process, Owner, _Reason}->
      destroy_monitor( Monitor ),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    _->
      object_monitor( Monitor )
  after
    5->erlang:hibernate(?MODULE,?FUNCTION_NAME,[ Monitor ])
  end.

%%=================================================================
%%	OBJECT MONITOR ENGINE
%%=================================================================
create_monitor( Object, #subscription{
  id = ID,
  owner = Owner,
  read = Read,
  deps = Deps,
  params = #{
    no_feedback := NoFeedback,
    stateless := Stateless
  }
})->
  OID = ?OID(Object),
  Monitor = #monitor{
    id = ID,
    oid = OID,
    object = Object,
    owner = Owner,
    read = Read,
    deps = Deps,
    no_feedback = NoFeedback,
    self = self()
  },

  Key = ?M_KEY(OID,ID,Owner),
  ets:insert(?S_OBJECT, {Key, Monitor}),
  ets:insert(?S_OBJECT_INDEX, [{OID,Key} | [{{OID, F}, Key} || F <- Deps ]]),

  if
    Stateless -> ignore;
    true ->
      Fields = ecomet_object:read_all(Object),
      Update = Read( ecomet_query:object_map(Object,Fields) ),
      Owner ! ?SUBSCRIPTION(ID,create,OID, Update)
  end,

  Monitor.

destroy_monitor(#monitor{
  id = ID,
  oid = OID,
  owner = Owner,
  deps = Deps
})->
  Key = ?M_KEY(OID,ID,Owner),
  ets:delete_object (?S_OBJECT_INDEX, {OID, Key} ),
  [ ets:delete_object(?S_OBJECT_INDEX, {{OID, F}, Key} ) || F <- Deps],
  ets:delete(?S_OBJECT, Key).

check_object(#monitor{
  owner = Owner,
  no_feedback = true
}, #{
  action := Action,
  self := Self
}) when
  (Owner =:= Self) andalso ( (Action =:= light_update) or (Action =:= update) )->
  % Ignore own changes
  ok;
%%---------light update commit--------------------
check_object(#monitor{
  id = ID,
  oid = OID,
  owner = Owner,
  read = Read
}, #{
  action := light_update,
  object := Object
})->
  Updates = Read( Object ),
  catch Owner ! ?SUBSCRIPTION(ID,update,OID,Updates),
  ok;

%%---------full update commit--------------------
check_object(#monitor{
  id = ID,
  oid = OID,
  owner = Owner,
  read = Read
}, #{
  action := update,
  object := Object
})->
  Updates = Read( Object ),
  catch Owner ! ?SUBSCRIPTION(ID,update,OID,Updates),
  ok;

check_object(#monitor{
  owner = Owner,
  self = Self,
  oid = OID,
  no_feedback = NoFeedback
} = Monitor, #{
  action := delete,
  self := Actor
})->

  destroy_monitor( Monitor ),
  Ignore = NoFeedback =:= true andalso Actor=:=Owner,

  catch Self ! { delete, OID, Ignore },

  ok.

%%=================================================================
%%	Query subscription
%%=================================================================
-record(query,{rs, start_ts, self, subscription}).
subscribe_query( #subscription{
  id = ID,
  conditions = Conditions0,
  owner = Owner,
  dbs = DBs,
  params = #{
    stateless := Stateless,     % No initial query, only updates
    no_feedback := _NoFeedback    % Do not send updates back to the author process
  } = Params
} = S)->

  Conditions =
    case ecomet_user:is_admin() of
      {ok,true}->compile_conditions( Conditions0 );
      {error,Error}->throw(Error);
      _->
        {ok,UserGroups}=ecomet_user:get_usergroups(),
        {'AND',[
          compile_conditions( Conditions0 ),
          {'OR',[{<<".readgroups">>,'=',GID}||GID<-UserGroups]}
        ]}
    end,

  Tags = ecomet_resultset:subscription_prepare( Conditions ),

  IndexDBs = index_dbs( DBs ),
  Index = compile_index( Tags, IndexDBs ),
  build_index(Index, {Owner, ID}),

  RS = ecomet_query:get(IndexDBs,rs,Conditions0),

  StartTS =
    if
      Stateless ->
        ecomet_resultset:foldl(fun(OID, TS)->
          create_monitor( ecomet_object:construct( OID ), S ),
          TS
        end, -1, RS );
      true ->
        ecomet_resultset:foldl(fun(OID, TS)->
          Object = ecomet_object:construct( OID ),
          { ok, ObjectTS } = ecomet_object:read_field( Object, <<".ts">> ),
          create_monitor( Object, S ),
          if
            ObjectTS > TS-> ObjectTS;
            true -> TS
          end
        end, -1, RS )
    end,

  Query = #query{
    subscription = S#subscription{
      conditions = Conditions,
      params = Params#{ stateless => true }
    },
    start_ts = StartTS,
    rs = RS,
    self = self()
  },

  ets:insert( ?S_QUERY, {{Owner,ID}, Query} ),

  query_monitor(Query, Index).

% SUBSCRIBE CID=test GET .name, f1 from * where and(.folder=$oid('/root/f1'), .name='o12')
query_monitor(#query{
  subscription = #subscription{
    id = ID,
    owner = Owner
  } = S,
  rs = RS
} = Query, Index )->
  receive
    {add, OID, Object}->
      if
        Object =:= ignore -> ignore;
        true -> catch Owner ! ?SUBSCRIPTION(ID, create, OID, Object )
      end,
      create_monitor( ecomet_object:construct(OID), S ),
      query_monitor( Query#query{ rs = ecomet_resultset:add_oid( OID, RS ) }, Index );
    {delete, OID, Ignore}->
      if
        Ignore -> ignore;
        true -> catch Owner ! ?SUBSCRIPTION(ID, delete, OID, #{})
      end,
      query_monitor( Query#query{ rs = ecomet_resultset:remove_oid( OID, RS ) }, Index );
    {unsubscribe, Owner}->
      destroy_query( Query, Index );
    {'DOWN', _Ref, process, Owner, _Reason}->
      destroy_query( Query, Index );
    _->
      query_monitor( Query, Index )
  after
    5->erlang:hibernate(?MODULE,?FUNCTION_NAME,[ Query, Index ])
  end.

destroy_query(#query{
  subscription = #subscription{
    id = ID,
    owner = Owner
  },
  rs = RS
}, Index )->
  ecomet_resultset:foldl(fun(OID, _)->
    [ destroy_monitor( M ) || {_,M} <- ets:lookup(?S_OBJECT, ?M_KEY(OID, ID, Owner)) ],
    ok
  end, ignore, RS ),
  catch destroy_index(Index, {Owner, ID}),
  ets:delete(?S_QUERY, {Owner, ID} ),
  ets:delete(?SUBSCRIPTIONS,{Owner,ID}).

%%=================================================================
%%	QUERY MONITOR ENGINE
%%=================================================================
%------------------New object created--------------------------------
check_query(#query{
  start_ts = StartTS
}, #{
  action := create,
  ts := TS
}) when TS =< StartTS ->
  % Don't send objects that were sent on subscription initialization
  ignore;

check_query(#query{
  subscription = #subscription{
    owner = Owner,
    conditions = Conditions,
    read = Read,
    params = #{ no_feedback := NoFeedback }
  },
  self = Self
}, #{
  action := create,
  self := Actor,
  oid := OID,
  object := Object
})->

  case ecomet_resultset:direct(Conditions, Object) of
    true->
      Updates =
        if
          NoFeedback =:= true, Actor =:= Owner ->  ignore;
          true -> Read( Object )
        end,
      catch Self ! { add, OID, Updates };
    _->
      ignore
  end;

check_query(#query{
  subscription = #subscription{
    id = ID,
    owner = Owner,
    conditions = Conditions,
    read = Read,
    params = #{ no_feedback := NoFeedback }
  },
  self = Self
}, #{
  action := update,
  self := Actor,
  oid := OID,
  object := Object,
  object0 := Object0
})->
  FullObject = maps:merge(Object0, Object),
  case { ecomet_resultset:direct(Conditions, FullObject), ecomet_resultset:direct(Conditions, Object0) } of
    {true,true}->
      % Object monitor is working
      ignore;
    { true, false }->
      Updates =
        if
          NoFeedback =:= true, Actor =:= Owner ->  ignore;
          true -> Read( FullObject )
        end,
      % The subscription process should create monitor itself.
      % Because if I create it and the subscription is already dead
      % the the monitor will never be destroyed
      catch Self ! { add, OID, Updates };
    {false, true}->
      % Remove the monitor
      [ destroy_monitor( M ) || {_,M} <- ets:lookup(?S_OBJECT, ?M_KEY(OID, ID, Owner)) ],
      Ignore = NoFeedback =:= true andalso Actor =:= Owner,
      catch Self ! { delete, OID, Ignore };
    _->
      ignore
  end.

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

get_subscriptions( Session )->
  [ #{id => Id, ts => TS, pid => PID} || [Id,TS,PID] <-ets:match(?SUBSCRIPTIONS, #sub{id = {Session,'$1'}, ts ='$2', pid='$3', _ = '_'})].

index_dbs( '*' )->
  '*';
index_dbs([ Tag | Rest ]) when is_binary( Tag )->
  ecomet_db:find_by_tag( Tag ) ++ index_dbs( Rest );
index_dbs([ DB | Rest ]) when is_atom( DB )->
  [DB| index_dbs( Rest )];
index_dbs([])->
  [].

build_index([#index{tag = Tag}=Index|Rest], ID)->

  {ok,TagUnlock} = elock:lock(?LOCKS,{tag,Tag}, _IsShared = false, _Timeout = infinity),
  try
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
    end
  after
    TagUnlock()
  end,
  build_index(Rest, ID);
build_index([], _ID)->
  ok.

destroy_index([#index{tag = Tag}=Index|Rest], ID )->
  {ok,TagUnlock} = elock:lock(?LOCKS,{tag,Tag}, _IsShared = false, _Timeout = infinity),
  try
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
    end
  after
    TagUnlock()
  end,

  destroy_index(Rest, ID);
destroy_index([], _ID)->
  ok.

global_set(Tag)->
  ecomet_router:global_set( Tag ).

global_reset(Tag)->
  ecomet_router:global_reset( Tag ).

%%------------------------------------------------------------
%%  Trigger subscriptions
%%------------------------------------------------------------
notify_monitor( OID, #{ action:= delete } =  Log )->
  Monitors = find_all_monitors( OID ),
  [ check_object( M, Log ) || M <- Monitors ],
  ok;

notify_monitor( OID, #{ object := Object } =  Log )->
  Monitors = find_monitors( OID, maps:keys( Object ) ),
  [ check_object( M, Log ) || M <- Monitors ],
  ok.

%% All OID monitors
find_all_monitors( OID )->
  MKeys = [ K || {_, K} <- ets:lookup( ?S_OBJECT_INDEX, OID )],
  get_monitors( lists:usort( MKeys ) ).

%% OID fields monitors
find_monitors(OID, Fields)->
  MKeys = lists:append( [ [ K || {_, K} <- ets:lookup( ?S_OBJECT_INDEX, {OID, F} )] || F <- Fields ] ),
  get_monitors( lists:usort( MKeys ) ).

get_monitors( [MKey|Rest] )->
  case ets:lookup( ?S_OBJECT, MKey ) of
    [{_,M}]-> [M | get_monitors( Rest ) ];
    _-> get_monitors( Rest )
  end;
get_monitors([])->
  [].

%%=================================================================================
%%  OBJECT COMMIT
%%=================================================================================
%%---------------Light update-----------------------
on_commit(#{
  action := create,
  db := DB,
  tags := Tags
} = Log, Global)->

  case Global of
    ?EMPTY_SET->
      ignore;
    Global->
      light_search(Global, DB, Tags, Log)
  end;

%%---------------UPDATE-----------------------
% Light
on_commit(#{
  action := light_update,
  oid := OID
} = Log, _Global)->
  notify_monitor( OID, Log );
% Full
on_commit(#{
  action := update,
  oid := OID,
  db := DB,
  tags := {TAdd, TOld, TDel}
} = Log, Global)->


  % Run the search of query subscriptions
  case Global of
    ?EMPTY_SET->
      ignore;
    Global->
      TNewMask = ?SET_OR( TAdd, TOld ),
      TOldMask = ?SET_OR( TDel, TOld ),
      TMask = ?SET_OR( TNewMask, TDel ),
      search(Global, DB, TMask, TNewMask, TOldMask, Log)
  end,

  notify_monitor( OID, Log );

on_commit(#{
  action := delete,
  oid := OID
} = Log, _Global)->
  notify_monitor( OID, Log ).

light_search(Global, DB, Mask, Log )->
  case ?SET_AND(Mask,Global) of
    ?EMPTY_SET -> ignore;
    XTags->
      ?SET_FOLD(fun(Tag,Notified)->
        case ets:lookup(?S_INDEX,{tag,Tag}) of
          [{_,Indexes}]->
            maps:fold(fun(#index{'&' = And,'!' = Not, db = DBs}, Subscribers, TagNotified)->
              case bitmap_search(Mask, And, Not) of
                true ->
                  IsDB = DBs=:='*' orelse lists:member(DB, DBs),
                  if
                    IsDB ->
                      ToNotify = ordsets:subtract( Subscribers, TagNotified ),
                      [ case ets:lookup(?S_QUERY, S) of
                          [ {_, Query} ] -> check_query( Query, Log );
                          _-> ignore
                        end || S <- ToNotify ],
                      ordsets:union( TagNotified, ToNotify );
                    true->
                      TagNotified
                  end;
                false ->
                  TagNotified
              end
            end,Notified, Indexes );
          []->
            Notified
        end
      end,[], XTags)
  end.

search(Global, DB, TMask, TNewMask, TOldMask, Log )->
  case ?SET_AND(TMask,Global) of
    ?EMPTY_SET -> ignore;
    XTags->
      ?SET_FOLD(fun(Tag,Notified)->
        case ets:lookup(?S_INDEX,{tag,Tag}) of
          [{_,Indexes}]->
            maps:fold(fun(#index{'&' = And,'!' = Not, db = DBs}, Subscribers, TagNotified)->
              case bitmap_search(TMask, TOldMask, TNewMask, And, Not) of
                true ->
                  IsDB = DBs =:= '*' orelse lists:member(DB, DBs),
                  if
                    IsDB ->
                      ToNotify = ordsets:subtract( Subscribers, TagNotified ),
                      [ case ets:lookup(?S_QUERY, S) of
                          [ {_, Query} ] -> check_query( Query, Log );
                          _-> ignore
                        end || S <- ToNotify ],
                      ordsets:union( TagNotified, ToNotify );
                    true ->
                      TagNotified
                  end;
                false ->
                  TagNotified
              end
            end,Notified, Indexes );
          []->
            Notified
        end
      end,[], XTags)
  end.

bitmap_search(Mask, And, Not) ->
  case ?SET_IS_SUBSET( And, Mask ) of
    true->
      ?SET_IS_DISJOINT(Mask, Not);
    _->
      false
  end.

bitmap_search(Mask, TOldMask, TNewMask, And, Not) ->
  case ?SET_IS_SUBSET( And, Mask ) of
    true->
      case ?SET_IS_DISJOINT(Mask, Not) of
        true -> true;
        _ when TOldMask =:= ?EMPTY_SET ; TNewMask =:= ?EMPTY_SET ->
          % The object is either created or deleted
          false;
        _ ->
          case ?SET_IS_DISJOINT(TOldMask, Not) of
            true ->
              % The previous object satisfied to the NOT
              true;
            _ ->
              % The previous object didn't satisfy to the NOT
              case ?SET_IS_DISJOINT(TNewMask, Not) of
                true ->
                  % The actual object satisfies
                  true;
                _ ->
                  % The actual object don't satisfy also
                  false
              end
          end
      end;
    _-> false
  end.

























