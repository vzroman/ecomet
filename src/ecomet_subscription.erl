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
  unsubscribe/1,
  build_tag_hash/1,
  new_bit_set/0,
  bit_and/2,
  bit_or/2,
  bit_subtract/2,
  set_bit/2,
  reset_bit/2
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

-define(HASH_BASE, 4294836225).

-define(SET_BIT(BM,Tag), set_bit(BM,Tag) ).
-define(RESET_BIT(BM,Tag), reset_bit(BM,Tag) ).
-define(X_BIT(X1,X2), bit_and(X1,X2) ).
-define(U_BIT(X1,X2), bit_or(X1,X2) ).
-define(EMPTY_SET,{0,nil}).

-record(log,{action, oid, object, changes, self, ts}).

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
  object_monitor(#monitor{id = ID,oid = OID,object = Object, owner = Owner, read = Read}, Params).

object_monitor(#monitor{id = ID,oid = OID, owner = Owner,object = Object, read = Read} =Monitor0, #{
  no_feedback := NoFeedback,
  stateless := Stateless
})->

  Monitor = Monitor0#monitor{
    no_feedback = NoFeedback,
    self = self()
  },
  ets:insert(?S_OBJECT, {OID, Monitor}),

  if
    Stateless -> ignore;
    true ->
      Fields = ecomet_object:read_all(Object),
      Update = Read( Fields, ecomet_query:object_map(Object,Fields) ),
      Owner ! ?SUBSCRIPTION(ID,create,OID, Update)
  end,

  object_monitor( Monitor ).

object_monitor(#monitor{id = ID,oid = OID, owner = Owner } = Monitor )->
  receive
    {delete, OID}->
      ets:delete(?SUBSCRIPTIONS,{Owner,ID}),
      ets:delete_object(?S_OBJECT, {OID, Monitor}),
      Owner ! ?SUBSCRIPTION(ID,delete,OID,#{});
    {unsubscribe, Owner}->
      ets:delete(?SUBSCRIPTIONS,{Owner,ID}),
      ets:delete_object(?S_OBJECT, {OID, Monitor});
    {'DOWN', _Ref, process, Owner, _Reason}->
      ets:delete(?SUBSCRIPTIONS,{Owner,ID}),
      ets:delete_object(?S_OBJECT, {OID, Monitor});
    _->
      object_monitor( Monitor )
  after
    5->erlang:hibernate(?MODULE,?FUNCTION_NAME,[ Monitor ])
  end.

check_object(#monitor{
  self = Self,
  oid = OID
}, #log{
  action = delete
})->
  catch Self ! { delete, OID },
  ok;

check_object(#monitor{
  owner = Owner,
  no_feedback = true
}, #log{
  self = Self
}) when Owner =:= Self->
  % Ignore own changes
  ok;
check_object(#monitor{
  id = ID,
  oid = OID,
  owner = Owner,
  read = Read
}, #log{
  object = Object,
  changes = Changes
})->
  Updates = Read( Changes, Object ),
  case maps:size(Updates) of
    0-> ignore;
    _-> catch Owner ! ?SUBSCRIPTION(ID,update,OID,Updates)
  end,
  ok.
%%=================================================================
%%	Query subscription
%%=================================================================
-record(query,{id, conditions, read,owner, no_feedback, start_ts}).
subscribe_query(ID, InConditions, Owner, DBs, Read, #{
  stateless := Stateless,     % No initial query, only updates
  no_feedback := NoFeedback    % Do not send updates back to the author process
})->

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

  StartTS =
    if
      Stateless -> -1;
      true ->
        RS = ecomet_query:get(IndexDBs,rs,InConditions),
        ecomet_resultset:foldl(fun(OID,TS)->
          Object = ecomet_object:construct( OID ),

          Fields = #{<<".ts">>:=ObjectTS} = ecomet_object:read_all( Object ),
          Update = Read(Fields,ecomet_query:object_map(Object,Fields)),
          Owner ! ?SUBSCRIPTION(ID,create,OID, Update),

          if
            ObjectTS > TS-> ObjectTS;
            true -> TS
          end
        end,-1, RS )
    end,

  Query = #query{
    id = ID,
    conditions = Conditions,
    read = Read,
    owner = Owner,
    no_feedback = NoFeedback,
    start_ts = StartTS
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
    tag = tag_hash(Tag),
    '&' = build_tag_hash(And),
    '!'= build_tag_hash(Not),
    db = DBs } | compile_index( Rest, DBs ) ];
compile_index([], _DBs)->
  [].

build_tag_hash( Tags )->
  gb_sets:from_list([ tag_hash(T) || T <- Tags ] ).
tag_hash( Tag )->
  erlang:phash2(Tag,?HASH_BASE).

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
}, #log{
   self = Self
}) when Owner=:= Self ->
  % Ignore own changes
  ok;
check_query(#query{
  id = ID,
  owner = Owner,
  conditions = Conditions,
  read = Read,
  start_ts = StartTS
}, #log{
  oid = OID,
  object = Object,
  changes = Changes,
  ts = TS
})->
  case { ecomet_resultset:direct(Conditions, Object), ecomet_resultset:direct(Conditions, maps:merge(Object,Changes)) } of
    {true,true}->
      Updates = Read( Changes, Object ),
      case maps:size(Updates) of
        0-> ignore;
        _-> Owner ! ?SUBSCRIPTION(ID,update,OID,Updates)
      end;
    { true, false }->
      if
        TS =< StartTS ->
          ignore;
        true ->
          Owner ! ?SUBSCRIPTION(ID,create,OID, Read( Object, Object ))
      end;
    {false, true}->
      Owner ! ?SUBSCRIPTION(ID,delete,OID,#{});
    _->
      ignore
  end.

% SUBSCRIBE CID=test GET .name, f1 from * where and(.folder=$oid('/root/f1'), .name='o12')
query_monitor( #query{id = ID, owner = Owner } = Query, Index )->
  receive
    {unsubscribe, Owner}->
      catch destroy_index(Index, {Owner, ID}),
      ets:delete(?S_QUERY, {Owner, ID} ),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    {'DOWN', _Ref, process, Owner, _Reason}->
      catch destroy_index(Index, {Owner, ID}),
      ets:delete(?S_QUERY, {Owner, ID} ),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    _->
      query_monitor( Query, Index )
  after
    5->erlang:hibernate(?MODULE,?FUNCTION_NAME,[ Query, Index ])
  end.

%%------------------------------------------------------------
%%  Trigger subscriptions
%%------------------------------------------------------------
on_commit(#{
  object := #{<<".oid">> := OID} = Object,
  db := DB,
  tags := {TAdd, TOld, TDel},
  changes := Changes,
  self := Self,
  ts:=TS
}, Global)->

  % Run object monitors
  Log = #log{
    action = if TAdd =:=?EMPTY_SET andalso TOld =:= ?EMPTY_SET -> delete; true -> update end,
    oid = OID,
    object = Object,
    changes = Changes,
    self = Self,
    ts = TS
  },
  [ check_object( Monitor, Log ) ||{_, Monitor} <- ets:lookup( ?S_OBJECT, OID ) ],


  % Run the search of query subscriptions
  case Global of
    ?EMPTY_SET->
      ignore;
    Global->
      if
        TAdd =:=?EMPTY_SET, TDel=:=?EMPTY_SET->
          % No index changes
          light_search(Global, DB, TOld, Log);
        TOld=:=?EMPTY_SET, TDel=:=?EMPTY_SET->
          % Create
          light_search(Global, DB, TAdd, Log);
        TAdd=:=?EMPTY_SET, TOld=:=?EMPTY_SET->
          % Delete
          light_search(Global, DB, TDel, Log);
        true ->
          TNewMask = ?U_BIT( TAdd, TOld ),
          TOldMask = ?U_BIT( TDel, TOld ),
          TMask = ?U_BIT( TNewMask, TDel ),
          search(Global, DB, TMask, TNewMask, TOldMask, Log)
      end
  end.

light_search(Global, DB, Mask, Log )->
  case ?X_BIT(Mask,Global) of
    ?EMPTY_SET -> ignore;
    XTags->
      bit_fold(fun(Tag,Notified)->
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
  case ?X_BIT(TMask,Global) of
    ?EMPTY_SET -> ignore;
    XTags->
      bit_fold(fun(Tag,Notified)->
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
  case is_subset( And, Mask ) of
    true->
      is_disjoint(Mask, Not);
    _->
      false
  end.

bitmap_search(Mask, TOldMask, TNewMask, And, Not) ->
  case is_subset( And, Mask ) of
    true->
      case is_disjoint(Mask, Not) of
        true -> true;
        _ when TOldMask =:= ?EMPTY_SET ; TNewMask =:= ?EMPTY_SET ->
          % The object is either created or deleted
          false;
        _ ->
          case is_disjoint(TOldMask, Not) of
            true ->
              % The previous object satisfied to the NOT
              true;
            _ ->
              % The previous object didn't satisfy to the NOT
              case is_disjoint(TNewMask, Not) of
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

new_bit_set()->
  gb_sets:new().

set_bit(Set, Tag)->
  gb_sets:add_element(Tag, Set).

reset_bit(Set,Tag)->
  gb_sets:del_element(Tag, Set).

bit_and( Set1, Set2 )->
  gb_sets:intersection( Set1, Set2 ).

bit_or( Set1, Set2 )->
  gb_sets:union( Set1, Set2 ).

bit_subtract( Set1, Set2 )->
  gb_sets:subtract( Set1, Set2 ).

is_subset( Set1, Set2 )->
  gb_sets:is_subset( Set1, Set2 ).

is_disjoint( Set1, Set2 )->
  gb_sets:is_disjoint( Set1, Set2 ).

bit_fold(Fun, Acc, Set )->
  gb_sets:fold( Fun, Acc, Set ).























