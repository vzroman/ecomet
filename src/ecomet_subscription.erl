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
  subscribe/5,
  unsubscribe/1
]).

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  object_monitor/1,
  query_monitor/2
]).

-record(subscription,{id, pid, ts, owner }).
-record(index,{tag,'&','!',db}).


-define(SUBSCRIPTIONS,ecomet_subscriptions).
-define(S_OBJECT,ecomet_subscriptions_object).
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
    {keypos, #subscription.id},
    {read_concurrency, true},
    {write_concurrency,auto}
  ]),

  % Prepare the storage for object subscriptions
  ets:new(?S_OBJECT,[
    named_table,
    public,
    ordered_set,
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
subscribe( ID, DBs, Read, Conditions, Params)->
  Owner = self(),
  PID =
    ecomet_user:spawn_session(fun()->
      case ets:insert_new(?SUBSCRIPTIONS,#subscription{
        id = {Owner,ID},
        ts = ecomet_lib:ts(),
        pid = self(),
        owner = Owner
      }) of
        true->
          erlang:monitor(process, Owner),

          Owner ! {ok, self()},

          try do_subscribe(Conditions, Owner, ID, DBs, Read, Params )
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

do_subscribe({<<".path">>,'=',Path}, Owner, ID, DBs, Read, Params )->
  do_subscribe({<<".oid">>,'=',?OID(Path)}, Owner, ID, DBs, Read, Params );
do_subscribe({<<".oid">>,'=',OID}, Owner, ID, _DBs, Read, Params )->
  % Object subscription
  case ecomet_object:open(OID) of
    not_exists->
      throw({not_exists, OID});
    {error,Error}->
      throw(Error);
    Object->
      subscribe_object(ID,Object,Owner,Read,Params)
  end;
do_subscribe(Conditions, Owner, ID, DBs, Read, Params )->
  subscribe_query(ID, Conditions, Owner, DBs, Read, Params ).

unsubscribe(ID)->
  case ets:lookup(?SUBSCRIPTIONS,{self(),ID}) of
    [#subscription{pid = PID,owner = Owner}]->
      PID ! {unsubscribe,Owner};
    _->
      ignore
  end,
  ok.

%%=================================================================
%%	Single Object subscription
%%=================================================================
-record(monitor,{id,oid,object,read,owner,no_feedback,self}).
subscribe_object(ID,Object,Owner,Read,Params)->

  OID = ?OID(Object),
  Monitor = create_monitor( ID, OID, Object, Owner, Read, Params ),

  object_monitor(Monitor).

create_monitor( ID, OID, Object, Owner, Read, #{
  no_feedback := NoFeedback,
  stateless := Stateless
} )->

  Monitor = #monitor{
    id = ID,
    oid = OID,
    object = Object,
    owner = Owner,
    read = Read,
    no_feedback = NoFeedback,
    self = self()
  },

  ets:insert(?S_OBJECT, {?M_KEY(OID,ID,Owner), Monitor}),

  if
    Stateless -> ignore;
    true ->
      Fields = ecomet_object:read_all(Object),
      Update = Read( Fields, ecomet_query:object_map(Object,Fields) ),
      Owner ! ?SUBSCRIPTION(ID,create,OID, Update)
  end,

  Monitor.

destroy_monitor(#monitor{
  id = ID,
  oid = OID,
  owner = Owner
})->
  ets:delete(?S_OBJECT, ?M_KEY(OID,ID,Owner)).

object_monitor(#monitor{id = ID, oid = OID, owner = Owner } = Monitor )->
  receive
    {delete, OID}->
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

%%---------light update commit--------------------
check_object(#monitor{
  id = ID,
  oid = OID,
  owner = Owner,
  read = Read
}, #{
  action := light_update,
  object := Object,
  changes := Changes
})->
  Updates = Read( Changes, maps:merge(Object, Changes) ),
  case maps:size(Updates) of
    0-> ignore;
    _-> catch Owner ! ?SUBSCRIPTION(ID,update,OID,Updates)
  end,
  ok;

%%---------full update commit--------------------
check_object(#monitor{
  owner = Owner,
  no_feedback = true
}, #{
  action := update,
  self := Self
}) when Owner =:= Self->
  % Ignore own changes
  ok;
check_object(#monitor{
  id = ID,
  oid = OID,
  owner = Owner,
  read = Read
}, #{
  action := update,
  object := Object,
  changes := Changes
})->
  Updates = Read( Changes, maps:merge(Object, Changes) ),
  case maps:size(Updates) of
    0-> ignore;
    _-> catch Owner ! ?SUBSCRIPTION(ID,update,OID,Updates)
  end,
  ok;

check_object(#monitor{
  id = ID,
  owner = Owner,
  self = Self,
  oid = OID,
  no_feedback = NoFeedback
} = Monitor, #{
  action := delete,
  self := Actor
})->

  destroy_monitor( Monitor ),
  if
    NoFeedback =:= true, Actor=:=Owner-> ignore;
    true -> catch Owner ! ?SUBSCRIPTION(ID,delete,OID,#{})
  end,

  catch Self ! { delete, OID },

  ok.

%%=================================================================
%%	Query subscription
%%=================================================================
-record(query,{id, conditions, rs, read, params, owner, no_feedback, start_ts, self}).
subscribe_query(ID, InConditions, Owner, DBs, Read, #{
  stateless := Stateless,     % No initial query, only updates
  no_feedback := NoFeedback    % Do not send updates back to the author process
} = Params)->

  Conditions =
    case ecomet_user:is_admin() of
      {ok,true}->compile_conditions( InConditions );
      {error,Error}->throw(Error);
      _->
        {ok,UserGroups}=ecomet_user:get_usergroups(),
        {'AND',[
          compile_conditions( InConditions ),
          {'OR',[{<<".readgroups">>,'=',GID}||GID<-UserGroups]}
        ]}
    end,

  Tags = ecomet_resultset:subscription_prepare( Conditions ),

  IndexDBs = index_dbs( DBs ),
  Index = compile_index( Tags, IndexDBs ),
  build_index(Index, {Owner, ID}),

  {StartTS, ResultSet} =
    if
      Stateless -> {-1, ecomet_resultset:new()};
      true ->
        RS = ecomet_query:get(IndexDBs,rs,InConditions),

        TS = ecomet_resultset:foldl(fun(OID, TS)->
          Object = ecomet_object:construct( OID ),
          { ok, ObjectTS } = ecomet_object:read_field( Object, <<".ts">> ),
          create_monitor( ID, OID, Object, Owner, Read, Params ),
          if
            ObjectTS > TS-> ObjectTS;
            true -> TS
          end
        end, -1, RS ),

        { TS, RS }
    end,

  Query = #query{
    id = ID,
    conditions = Conditions,
    read = Read,
    params = Params#{ stateless => true },
    owner = Owner,
    no_feedback = NoFeedback,
    start_ts = StartTS,
    rs = ResultSet,
    self = self()
  },

  ets:insert( ?S_QUERY, {{Owner,ID}, Query} ),

  query_monitor(Query, Index).

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
  [ #{id => Id, ts => TS, pid => PID} || [Id,TS,PID] <-ets:match(?SUBSCRIPTIONS, #subscription{id = {Session,'$1'}, ts ='$2', pid='$3', _ = '_'})].

index_dbs( '*' )->
  '*';
index_dbs([ Tag | Rest ]) when is_binary( Tag )->
  ecomet_db:find_by_tag( Tag ) ++ index_dbs( Rest );
index_dbs([ DB | Rest ]) when is_atom( DB )->
  [DB| index_dbs( Rest )];
index_dbs([])->
  [].

%%------------------------------------------------------------
%%  Search engine
%%------------------------------------------------------------
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

check_query(#query{
  no_feedback = true,
  owner = Owner
}, #{
   self := Self
}) when Owner=:= Self ->
  % Ignore own changes
  ok;

%------------------New object created--------------------------------
check_query(#query{
  id = ID,
  owner = Owner,
  conditions = Conditions,
  read = Read,
  start_ts = StartTS,
  self = Self,
  no_feedback = NoFeedback
}, #{
  action := create,
  self := Actor,
  oid := OID,
  object := Object,
  changes := Changes,
  ts := TS
})->
  case ecomet_resultset:direct(Conditions, maps:merge(Object,Changes)) of
    true->
      if
        TS =< StartTS ->
          ignore;
        true ->
          if
            NoFeedback =:= true, Actor =:= Owner ->
              ignore;
            true ->
              catch Owner ! ?SUBSCRIPTION(ID,create,OID, Read( maps:merge(Object, Changes) ))
          end,
          catch Self ! { add, OID }
      end;
    _->
      ignore
  end;

check_query(#query{
  id = ID,
  owner = Owner,
  conditions = Conditions,
  read = Read,
  start_ts = StartTS,
  self = Self,
  no_feedback = NoFeedback
}, #{
  self := Actor,
  oid := OID,
  object := Object,
  changes := Changes,
  ts := TS
})->
  case { ecomet_resultset:direct(Conditions, maps:merge(Object,Changes)), ecomet_resultset:direct(Conditions, Object) } of
    {true,true}->
      % Object monitor is working
      ignore;
    { true, false }->
      if
        TS =< StartTS ->
          ignore;
        true ->
          if
            NoFeedback =:= true, Actor =:= Owner ->
              ignore;
            true ->
              catch Owner ! ?SUBSCRIPTION(ID,create,OID, Read( Object, Object ))
          end,
          catch Self ! { add, OID }
      end;
    {false, true}->
      if
        NoFeedback =:= true, Actor =:= Owner ->
          ignore;
        true ->
          catch Owner ! ?SUBSCRIPTION(ID,delete,OID,#{})
      end,
      catch Self ! { remove, OID };
    _->
      ignore
  end.

% SUBSCRIBE CID=test GET .name, f1 from * where and(.folder=$oid('/root/f1'), .name='o12')
query_monitor( #query{id = ID, owner = Owner, rs = RS, read = Read, params = Params } = Query, Index )->
  receive
    {add, OID}->
      create_monitor( ID, OID, ecomet_object:construct(OID), Owner, Read, Params ),
      query_monitor( Query#query{ rs = ecomet_resultset:add_oid( OID, RS ) }, Index );
    { remove, OID }->
      ets:delete(?S_OBJECT, ?M_KEY(OID, ID, Owner)),
      query_monitor( Query#query{ rs = ecomet_resultset:remove_oid( OID, RS ) }, Index );
    {delete, OID}->
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

destroy_query( #query{id = ID, owner = Owner, rs = RS }, Index )->
  ecomet_resultset:foldl(fun(OID, _)->
    ets:delete(?S_OBJECT, ?M_KEY(OID, ID, Owner))
  end, ignore, RS ),
  catch destroy_index(Index, {Owner, ID}),
  ets:delete(?S_QUERY, {Owner, ID} ),
  ets:delete(?SUBSCRIPTIONS,{Owner,ID}).

%%------------------------------------------------------------
%%  Trigger subscriptions
%%------------------------------------------------------------
notify_monitor( OID, Log )->
  notify_monitor( ets:next(?S_OBJECT, ?M_KEY( OID, -1, -1 )), OID, Log ).
notify_monitor(?M_KEY(OID,_,_)=K, OID, Log )->
  case ets:lookup( ?S_OBJECT, K ) of
    [{_,Monitor}]-> check_object( Monitor, Log );
    _-> ignore
  end,
  notify_monitor( ets:next(?S_OBJECT, K), OID, Log );
notify_monitor(_K, _OID, _Log )->
  ok.


%%---------------Light update-----------------------
on_commit(#{
  action := create,
  oid := OID,
  db := DB,
  tags := Tags
} = Log, Global)->

  case Global of
    ?EMPTY_SET->
      ignore;
    Global->
      light_search(Global, DB, Tags, Log)
  end,

  notify_monitor( OID, Log );

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

























