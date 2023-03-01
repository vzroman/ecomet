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
  on_commit/1
]).

%%=================================================================
%%	API
%%=================================================================
-export([
  subscribe/5,
  unsubscribe/1,
  tag_hash/1
]).

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  object_monitor/1,
  query_monitor/1
]).

-record(subscription,{id, pid, ts, owner }).
-record(index,{tag,'&','!',db}).


-define(SUBSCRIPTIONS,ecomet_subscriptions).
-define(S_INDEX,ecomet_subscriptions_index).
-define(HASH_BASE, 4294836225).

-define(SET_BIT(BM,Tag), set_bit(BM,Tag) ).
-define(RESET_BIT(BM,Tag), reset_bit(BM,Tag) ).
-define(X_BIT(X1,X2), bit_and(X1,X2) ).
-define(U_BIT(X1,X2), bit_or(X1,X2) ).


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
-record(monitor,{id,oid,object,read,owner,no_feedback}).
subscribe_object(ID,Object,Owner,Read,Params)->

  OID = ?OID(Object),
  object_monitor(#monitor{id = ID,oid = OID,object = Object, owner = Owner, read = Read}, Params).

object_monitor(#monitor{id = ID,oid = OID, owner = Owner,object = Object, read = Read} =Monitor, #{
  no_feedback := NoFeedback,
  stateless := Stateless
})->

  esubscribe:subscribe(?ESUBSCRIPTIONS,{log,OID}, self(),[node()]),

  if
    Stateless -> ignore;
    true ->
      Fields = ecomet_object:read_all(Object),
      Update = Read( Fields, ecomet_query:object_map(Object,Fields) ),
      Owner ! ?SUBSCRIPTION(ID,create,OID, Update)
  end,

  object_monitor( Monitor#monitor{no_feedback = NoFeedback} ).

object_monitor(#monitor{id = ID,oid = OID, owner = Owner, read = Read, no_feedback = NoFeedback } = Monitor )->
  receive
    {?ESUBSCRIPTIONS, {log,OID}, {_IsDelete, _Object, _Changes, Self}, _Node, _Actor} when NoFeedback, Self=:=Owner->
      object_monitor( Monitor );
    {?ESUBSCRIPTIONS, {log,OID}, {_IsDelete = true, _Object, _Changes, _Self}, _Node, _Actor}->
      Owner! ?SUBSCRIPTION(ID,delete,OID,#{}),
      esubscribe:unsubscribe(?ESUBSCRIPTIONS,{log,OID}, self(),[node()]),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    {?ESUBSCRIPTIONS, {log,OID}, {_IsDelete, Object, Changes, _Self}, _Node, _Actor}->
      Updates = Read( Changes, Object ),
      case maps:size(Updates) of
        0-> ignore;
        _-> Owner ! ?SUBSCRIPTION(ID,update,OID,Updates)
      end,
      object_monitor( Monitor );
    {unsubscribe, Owner}->
      esubscribe:unsubscribe(?ESUBSCRIPTIONS,{log,OID}, self(),[node()]),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    {'DOWN', _Ref, process, Owner, _Reason}->
      esubscribe:unsubscribe(?ESUBSCRIPTIONS,{log,OID}, self(), [node()]),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    _->
      object_monitor( Monitor )
  after
    5->erlang:hibernate(?MODULE,?FUNCTION_NAME,[ Monitor ])
  end.

%%=================================================================
%%	Query subscription
%%=================================================================
-record(query,{id, conditions, read,owner, no_feedback, index}).
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
  Index = compile_index( Tags, DBs ),
  build_index(Index, self()),

  if
    Stateless -> ignore;
    true ->
      RS = ecomet_query:get(DBs,rs,InConditions),
      StartTS =
        ecomet_resultset:foldl(fun(OID,TS)->
          Object = ecomet_object:construct( OID ),

          Fields = #{<<".ts">>:=ObjectTS} = ecomet_object:read_all( Object ),
          Update = Read(Fields,ecomet_query:object_map(Object,Fields)),
          Owner ! ?SUBSCRIPTION(ID,create,OID, Update),

          if
            ObjectTS > TS-> ObjectTS;
            true -> TS
          end
        end,-1, RS ),

      drop_updates( StartTS)
  end,

  query_monitor(#query{id = ID, conditions = Conditions, read = Read, owner = Owner, no_feedback = NoFeedback, index = Index}).

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
    '&' = tags_mask(And,[]),
    '!'= tags_mask(Not,[]),
    db = DBs } | compile_index( Rest, DBs ) ];
compile_index([], _DBs)->
  [].

tag_hash( Tag )->
  erlang:phash2(Tag,?HASH_BASE).

tags_mask([Tag|Rest],Mask)->
  tags_mask( Rest, [tag_hash(Tag)|Mask] );
tags_mask([], Mask)->
  ordsets:from_list( Mask ).

get_subscriptions( Session )->
  [ #{id => Id, ts => TS, pid => PID} || [Id,TS,PID] <-ets:match(?SUBSCRIPTIONS, #subscription{id = {Session,'$1'}, ts ='$2', pid='$3', _ = '_'})].

%%------------------------------------------------------------
%%  Search engine
%%------------------------------------------------------------
build_index([#index{tag = Tag}=Index|Rest], Self)->

  {ok,TagUnlock} = elock:lock(?LOCKS,{tag,Tag}, _IsShared = false, _Timeout = infinity),
  try
    case ets:lookup(?S_INDEX,{tag,Tag}) of
      [{_,Indexes}]->
        case Indexes of
          #{Index := Subscribers} ->
            ets:insert(?S_INDEX,{ {tag,Tag}, Indexes#{ Index => ordsets:add_element(Self,Subscribers) }});
          _->
            ets:insert(?S_INDEX,{ {tag,Tag}, Indexes#{ Index => [Self] }})
        end;
      []->
        ets:insert(?S_INDEX,{ {tag,Tag}, #{ Index => [Self] }}),
        global_set(Tag)
    end
  after
    TagUnlock()
  end,
  build_index(Rest, Self);
build_index([], _Self)->
  ok.

destroy_index([#index{tag = Tag}=Index|Rest], Self )->
  {ok,TagUnlock} = elock:lock(?LOCKS,{tag,Tag}, _IsShared = false, _Timeout = infinity),
  try
    case ets:lookup(?S_INDEX,{tag,Tag}) of
      [{_,Indexes}]->
        case Indexes of
          #{Index := Subscribers}->
            case ordsets:del_element( Self, Subscribers ) of
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

  destroy_index(Rest, Self);
destroy_index([], _Self)->
  ok.

global_set(Tag)->
  {ok,Unlock} = elock:lock(?LOCKS,global, _IsShared = false, _Timeout = infinity),
  try
    case ets:lookup(?S_INDEX,global) of
      [{_,Global}]->
        ets:insert(?S_INDEX,{global, ?SET_BIT(Global,[Tag]) });
      []->
        ets:insert(?S_INDEX,{global, ?SET_BIT([],[Tag]) })
    end
  after
    Unlock()
  end.
global_reset(Tag)->
  {ok,Unlock} = elock:lock(?LOCKS,global, _IsShared = false, _Timeout = infinity),
  try
    case ets:lookup(?S_INDEX, global) of
      [{_,Global}]->
        ets:insert(?S_INDEX,{ global, ?RESET_BIT( Global, Tag ) });
      []->
        ignore
    end
  after
    Unlock()
  end.

drop_updates( StartTS )->
  receive
    {log, _OID, #{<<".ts">>:=TS}, _Changes, _Self} when TS =< StartTS->
      drop_updates( StartTS )
  after
    0->ok
  end.

% SUBSCRIBE CID=test GET .name, f1 from * where and(.folder=$oid('/root/f1'), .name='o12')
query_monitor( #query{id = ID, no_feedback = NoFeedback,owner = Owner, conditions = Conditions, read = Read, index = Index } = Query )->
  receive
    {log, _OID, _Object, _Changes, Self} when NoFeedback, Self=:=Owner->
      query_monitor( Query );
    {log, OID, Object, Changes, _Self}->
      case { ecomet_resultset:direct(Conditions, Object), ecomet_resultset:direct(Conditions, maps:merge(Object,Changes)) } of
        {true,true}->
          Updates = Read( Changes, Object ),
          case maps:size(Updates) of
            0-> ignore;
            _-> Owner ! ?SUBSCRIPTION(ID,update,OID,Updates)
          end;
        { true, false }->
          Owner ! ?SUBSCRIPTION(ID,create,OID, Read( Object, Object ));
        {false, true}->
          Owner ! ?SUBSCRIPTION(ID,delete,OID,#{});
        _->
          ignore
      end,
      query_monitor( Query );
    {unsubscribe, Owner}->
      catch destroy_index(Index, self()),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    {'DOWN', _Ref, process, Owner, _Reason}->
      catch destroy_index(Index, self()),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    _->
      query_monitor( Query )
  after
    5->erlang:hibernate(?MODULE,?FUNCTION_NAME,[ Query ])
  end.

%%------------------------------------------------------------
%%  Trigger subscriptions
%%------------------------------------------------------------
on_commit(#{
  object := #{<<".oid">> := OID} = Object,
  db := DB,
  tags := {TAdd, TOld, TDel},
  changes := Changes,
  self := Self
})->
  % Run object monitors
  IsDelete = length(TAdd) =:=0 andalso length( TOld ) =:= 0,
  catch esubscribe:notify(?ESUBSCRIPTIONS, {log,OID}, {IsDelete, Object, Changes, Self } ),

  % Run the search on the other nodes
  TNewMask = ?U_BIT( TAdd, TOld ),
  TOldMask = ?U_BIT( TDel, TOld ),
  search(OID, DB, TNewMask, TOldMask, Object, Changes, Self).

search( OID, DB, TNewMask, TOldMask, Object, Changes, Self )->
  case ets:lookup(?S_INDEX, global) of
    [{_,[]}]->
      ignore;
    [{_,Global}]->
      Mask = ?U_BIT( TNewMask, TOldMask ),
      case ?X_BIT(Mask,Global) of
        [] -> ignore;
        XTags->
          ordsets:fold(fun(Tag,Notified)->
            case ets:lookup(?S_INDEX,{tag,Tag}) of
              [{_,Indexes}]->
                maps:fold(fun(#index{'&' = And,'!' = Not, db = DBs}, Subscribers, TagNotified)->
                  case bitmap_search(Mask, TOldMask, TNewMask, And, Not) of
                    true ->
                      case lists:member(DB, DBs) of
                        true ->
                          ToNotify = ordsets:subtract( Subscribers, TagNotified ),
                          [ S ! {log, OID, Object, Changes, Self} || S <- ToNotify ],
                          ordsets:union( TagNotified, ToNotify );
                        _->
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
      end;
    []->
      ignore
  end.

bitmap_search(Mask, TOldMask, TNewMask, And, Not) ->
  case ?X_BIT( Mask, And ) of
    And->
      case ?X_BIT(Mask, Not) of
        [] -> true;
        _ when TOldMask =:= [] ; TNewMask =:= [] -> false;
        _ ->
          case ?X_BIT(TOldMask, Not) of
            [] -> true;
            _ ->
              case ?X_BIT(TNewMask, Not) of
                [] -> true;
                _ -> false
              end
          end
      end;
    _-> false
  end.


set_bit(Set, Tags)->
  ordsets:union(Set, ordsets:from_list( Tags ) ).

reset_bit(Set,Tag)->
  ordsets:del_element(Tag, Set).

bit_and( Set1, Set2 )->
  ordsets:intersection( Set1, Set2 ).

bit_or( Set1, Set2 )->
  ordsets:union( Set1, Set2 ).























