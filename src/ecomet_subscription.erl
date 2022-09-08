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
  on_commit/1,
  notify/2
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
  query_monitor/1
]).

-record(subscription,{id, pid, ts, owner }).
-record(index,{tag,'&','!',db}).


-define(SUBSCRIPTIONS,ecomet_subscriptions).
-define(S_INDEX,ecomet_subscriptions_index).
-define(HASH_BASE, 4294836225).

-define(SET_BIT(BM,Tag), ecomet_bitmap:zip(ecomet_bitmap:set_bit(BM,Tag)) ).
-define(RESET_BIT(BM,Tag), ecomet_bitmap:zip(ecomet_bitmap:reset_bit(BM,Tag)) ).

%%=================================================================
%%	Service API
%%=================================================================
on_init()->

  % Initialize subscriptions optimization
  ok = ecomet_router:on_init( ?ENV( router_pool_size, ?ROUTER_POOL_SIZE ) ),

  % Prepare the storage for sessions
  ets:new(?SUBSCRIPTIONS,[named_table,public,set,{keypos, #subscription.id}]),

  % Prepare the storage for index
  ets:new(?S_INDEX,[named_table,public,set,{keypos, #index.tag}]),

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

  esubscribe:subscribe({log,OID}, [node()], self(), infinity),

  if
    Stateless -> ignore;
    true ->
      Fields = ecomet_object:read_all(Object),
      Update = Read( Fields, Fields ),
      Owner ! ?SUBSCRIPTION(ID,create,OID, Update)
  end,

  object_monitor( Monitor#monitor{no_feedback = NoFeedback} ).

object_monitor(#monitor{id = ID,oid = OID, owner = Owner, read = Read, no_feedback = NoFeedback } = Monitor )->
  receive
    {'$esubscription', {log,OID}, {_Tags, _Object, _Changes, Self}, _Node, _Actor} when NoFeedback, Self=:=Owner->
      object_monitor( Monitor );
    {'$esubscription', {log,OID}, {[], _Object, _Changes, _Self}, _Node, _Actor}->
      Owner! ?SUBSCRIPTION(ID,delete,OID,#{}),
      esubscribe:unsubscribe({log,ID},[node()], self()),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    {'$esubscription', {log,OID}, {_Tags, Object, Changes, _Self}, _Node, _Actor}->
      Updates = Read( Changes, Object ),
      case maps:size(Updates) of
        0-> ignore;
        _-> Owner ! ?SUBSCRIPTION(ID,update,OID,Updates)
      end,
      object_monitor( Monitor );
    {unsubscribe, Owner}->
      esubscribe:unsubscribe({log,ID},[node()], self()),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    {'DOWN', _Ref, process, Owner, _Reason}->
      esubscribe:unsubscribe({log,ID},[node()], self()),
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
      {ok,true}->InConditions;
      {error,Error}->throw(Error);
      _->
        {ok,UserGroups}=ecomet_user:get_usergroups(),
        {'AND',[
          Conditions,
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
          Update = Read(Fields,Fields),
          Owner ! ?SUBSCRIPTION(ID,create,OID, Update),

          if
            ObjectTS > TS-> ObjectTS;
            true -> TS
          end
        end,-1, RS ),

      drop_updates( StartTS)
  end,

  query_monitor(#query{id = ID, conditions = Conditions, read = Read, owner = Owner, no_feedback = NoFeedback, index = Index}).

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
  ?SET_BIT(<<>>, lists:usort(Mask)).

get_subscriptions( Session )->
  [ #{id => Id, ts => TS, pid => PID} || [Id,TS,PID] <-ets:match(?SUBSCRIPTIONS, #subscription{id = {Session,'$1'}, ts ='$2', pid='$3', _ = '_'})].

%%------------------------------------------------------------
%%  Search engine
%%------------------------------------------------------------
build_index([#index{tag = Tag}=Index|Rest], Self)->

  {ok,TagUnlock} = elock:lock('$subsLocks$',{tag,Tag}, _IsShared = false, _Timeout = infinity),
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
  {ok,TagUnlock} = elock:lock('$subsLocks$',{tag,Tag}, _IsShared = false, _Timeout = infinity),
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
  {ok,Unlock} = elock:lock('$subsLocks$',global, _IsShared = false, _Timeout = infinity),
  try
    case ets:lookup(?S_INDEX,global) of
      [{_,Global}]->
        ets:insert(?S_INDEX,{global, ?SET_BIT(Global,Tag) });
      []->
        ets:insert(?S_INDEX,{global, ?SET_BIT(<<>>,Tag) })
    end
  after
    Unlock()
  end.
global_reset(Tag)->
  {ok,GlobalUnlock} = elock:lock('$subsLocks$',global, _IsShared = false, _Timeout = infinity),
  try
    case ets:lookup(?S_INDEX, global) of
      [{_,Global}]->
        ets:insert(?S_INDEX,{ global, ?RESET_BIT( Global, Tag ) });
      []->
        ignore
    end
  after
    GlobalUnlock()
  end.

drop_updates( StartTS )->
  receive
    {log, _OID, #{<<".ts">>:=TS}, _Changes, _Self} when TS =< StartTS->
      drop_updates( StartTS )
  after
    0->ok
  end.

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
          ?SUBSCRIPTION(ID,create,OID, Read( Object, Object ));
        {false, true}->
          Owner ! Owner! ?SUBSCRIPTION(ID,delete,OID,#{});
        _->
          ignore
      end,
      query_monitor( Query );
    {unsubscribe, Owner}->
      destroy_index(Index, self()),
      ets:delete(?SUBSCRIPTIONS,{Owner,ID});
    {'DOWN', _Ref, process, Owner, _Reason}->
      destroy_index(Index, self()),
      ets:delete(?SUBSCRIPTIONS,{oid,ID});
    _->
      query_monitor( Query )
  after
    5->erlang:hibernate(?MODULE,?FUNCTION_NAME,[ Query ])
  end.


on_commit(#ecomet_log{ changes = undefined })->
  % No changes
  ok;
on_commit(#ecomet_log{
  object = #{<<".oid">> := OID} = Object,
  db = DB,
  tags = {TAdd, TOld, TDel},
  rights = { RAdd, ROld, RDel },
  changes = Changes,
  self = Self
} = Log)->

  % Run object monitors
  Updates = maps:with( maps:keys(Changes), Object ),
  NewTags = TAdd ++ TOld,
  [ rpc:cast( N, esubscribe, notify,[ {log,OID}, { NewTags, Updates, Self } ]) || N <-ecomet_node:get_ready_nodes() ],

  %-----------------------Sorting----------------------------------------------
  [ TAdd1, TOld1, TDel1, RAdd1, ROld1, RDel1, ChangedFields1]=
    [ordsets:from_list(I)||I<-[ TAdd, TOld, TDel, RAdd, ROld, RDel, maps:keys(Changes)] ],

  %-----------------------The SEARCH phase-------------------------------------
  Tags = TAdd1 ++ TOld1 ++ TDel1,
  Index = {'AND',[
    {'OR',[ {'TAG',{tag,{T,1}}}  || T<-Tags ]},
    {'OR',[ {'TAG',{tag,{T,2}}} || T<-[none|Tags] ]},
    {'OR',[ {'TAG',{tag,{T,3}}} || T<-[none|Tags] ]}
  ]},

  Rights =
    {'OR',[{'TAG',{rights,R}} || R <- [is_admin|RAdd1 ++ ROld1 ++ RDel1] ]},

  Dependencies =
    {'OR',[{'TAG',{deps,D}} || D <- [<<"@ANY@">>|ChangedFields1]]},

  Query = {'AND',[
    Dependencies,
    Index,
    Rights,
    {'TAG',{db,DB}}
  ]},

  % Log with sorted items
  Log1=Log#ecomet_log{
    tags = {TAdd1, TOld1, TDel1},
    rights = { RAdd1, ROld1, RDel1 },
    changes = Changes
  },

  % Run the search on the other nodes
  [ rpc:cast( N,?MODULE,notify,[ Query, Log1 ]) || N <-ecomet_node:get_ready_nodes() -- [node()] ],

  % Local search
  notify( Query, Log1 ),

  ok.

notify( Query, #ecomet_log{self = Self} = Log )->

  case search( Query ) of
    none -> ignore;
    Set ->
      gb_sets:fold(fun({PID, Owner, NoFeedback}, Acc)->
        if
          NoFeedback, Owner=:=Self-> ignore;
          true ->
            % Run the second (FINAL MATCH) phase
            PID ! {match,Log}
        end,
        Acc
      end, none, Set)
  end,

  ok.

search({'TAG',Tag})->
  case ets:lookup(?S_INDEX, Tag) of
    [#index{value = Set}] -> Set;
    _->none
  end;
search({'AND',Tags})->
  search_and(Tags,none);
search({'OR',Tags})->
  search_or(Tags,none).

search_and([T|Tags],Acc)->
  case search(T) of
    none -> none;
    Set when Acc =:= none ->
      search_and(Tags, Set);
    Set ->
      case gb_sets:intersection(Set,Acc) of
        {0,nil}-> none;
        Set1-> search_and(Tags,Set1)
      end
  end;
search_and([], Acc)->
  Acc.

search_or([T|Tags], Acc)->
  case search(T) of
    none ->
      search_or(Tags,Acc);
    Set when Acc =:= none->
      search_or(Tags,Set);
    Set->
      search_or(Tags, gb_sets:union(Set,Acc))
  end;
search_or([],Acc)->
  Acc.


%%------------------------------------------------------------
%%  Subscription process
%%------------------------------------------------------------
% The subscription is not started yet, stockpile the logs and wait for message to start
wait_for_run(#state{session = Session}=State, Buffer)->
  receive
    {match,Log}->
      wait_for_run(State, [Log|Buffer]);
    {run,Match}->
      % Activate the subscription
      % Run the buffer
      [ Match(Log) || Log <- lists:reverse(Buffer) ],
      subscription_loop(State#state{match = Match});
    {stop, Session}->
      % The exit command
      unlink(Session),

      clean_up( State );
    Unexpected->
      ?LOGWARNING("unexpected message ~p",[Unexpected]),
      wait_for_run(State, Buffer)
  after
    5-> erlang:hibernate(?MODULE,?FUNCTION_NAME,[ State, Buffer])
  end.

% The subscription is active, wait for log events.
subscription_loop(#state{session = Session, match = Match} = State)->
  receive
    {match,Log}->
      try Match(Log) catch
        _:Error->?LOGERROR("subscription matching error, error ~p",[Error])
      end,
      subscription_loop(State);
    {stop, Session}->
      % The exit command
      unlink(Session),

      clean_up( State );
    Unexpected->
      ?LOGWARNING("unexpected message ~p",[Unexpected]),
      subscription_loop(State)
  after
    5-> erlang:hibernate(?MODULE,?FUNCTION_NAME,[ State ])
  end.

clean_up( #state{ id =Id, owner = Owner, params = Params} )->
  % Unregister the subscription process
  ets:delete(?SUBSCRIPTIONS,Id),

  % Destroy index
  destroy_index( self(), Owner, Params ),

  ok.























