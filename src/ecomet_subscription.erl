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

-export([
  subscribe_object/4
]).

%%=================================================================
%%	Service API
%%=================================================================
-export([
  on_init/0,
  start_link/3,
  run/2,
  stop/1,

  get_subscriptions/1,

  on_commit/1,
  notify/2
]).

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  wait_for_run/2,
  subscription_loop/1,
  object_monitor/1
]).

-record(subscription,{id, pid, ts, owner }).
-record(index,{key, value}).
-record(monitor,{id,oid,object,owner,fields,format,no_feedback}).

-record(state,{id, session, owner, params, match}).

-define(SUBSCRIPTIONS,ecomet_subscriptions).
-define(S_INDEX,ecomet_subscriptions_index).

%%=================================================================
%%	Internal API
%%=================================================================
subscribe_object(ID,Object,Fields,InParams)->
  Owner = self(),
  PID = spawn(fun()->

    erlang:monitor(process, Owner),
    OID = ?OID(Object),

    case ets:insert_new(?SUBSCRIPTIONS,#subscription{
      id = {oid,ID},
      ts = ecomet_lib:ts(),
      pid = self(),
      owner = Owner
    }) of
      true ->
        % I'm ready
        Owner ! {ok,self()},

        object_monitor(#monitor{id = ID,oid = OID,object = Object, owner = Owner, fields = Fields}, InParams);
      _->
        Owner ! {error,self(),{not_unique,ID}}
    end
  end),
  receive
    {ok,PID} -> {ok,PID};
    {error,PID,Error}->throw(Error)
  end.

object_monitor(#monitor{id = ID,oid = OID, owner = Owner,object = Object,fields = Fields0} =Monitor0, Params)->

  NoFeedback = maps:get(no_feedback, Params, false),

  Format = maps:get(format, Params, fun(_,V)->V end),

  Fields1 =
   case Fields0 of
     ['*'] ->
       maps:keys( ecomet_object:read_all(Object) )--[<<".oid">>];
     _->
       Fields0
   end,

  Fields =
    [ case F of
        <<".oid">> -> {<<".oid">>, term};
        <<".path">> -> {<<".path">>, string};
        _-> {F, element(2,ecomet_object:field_type(Object,F)) }
      end || F <- Fields1],

  Monitor = Monitor0#monitor{
    no_feedback = NoFeedback,
    format = Format,
    fields = Fields
  },

  esubscribe:subscribe({log,OID}, [node()], self(), infinity),

  case maps:get(stateless, Params, false) of
    false ->
      ReadParams =
        if
          Format =:= undefined-> #{};
          true-> #{format => Format}
        end,
      Create = ecomet_object:read_fields(Object,Fields1,ReadParams),
      Owner ! ?SUBSCRIPTION(ID,create,OID, Create);
    _->
      ignore
  end,

  object_monitor( Monitor ).

object_monitor(#monitor{id = ID,oid = OID, owner = Owner, fields = Fields, format = Format, no_feedback = NoFeedback } = Monitor )->
  receive
    {'$esubscription', {log,OID}, {_Tags,_Changes, Self}, _Node, _Actor} when NoFeedback, Self=:=Owner->
      object_monitor( Monitor );
    {'$esubscription', {log,OID}, {[],_Changes, _Self}, _Node, _Actor}->
      Owner! ?SUBSCRIPTION(ID,delete,OID,#{}),
      ets:delete(?SUBSCRIPTIONS,{oid,ID});
    {'$esubscription', {log,OID}, {_Tags, Changes, _Self}, _Node, _Actor}->
      Updates =
        lists:foldl(fun({Field, Type}, Acc)->
          case Changes of
            #{Field := Value}->
              Acc#{ Field => Format(Type,Value) };
            _ when Field =:= <<".oid">>->
              Acc#{Field => Format(Type, OID)};
            _ when Field =:= <<".path">>->
              Acc#{Field => Format(Type, ?PATH(OID))};
            _->
              Acc
          end
        end, #{}, Fields),
      case maps:size(Updates) of
        0-> ignore;
        _-> Owner ! ?SUBSCRIPTION(ID,update,OID,Updates)
      end,
      object_monitor( Monitor );
    {stop, Owner}->
      ets:delete(?SUBSCRIPTIONS,{oid,ID});
    {'DOWN', _Ref, process, Owner, _Reason}->
      ets:delete(?SUBSCRIPTIONS,{oid,ID});
    _->
      object_monitor( Monitor )
  after
    5->erlang:hibernate(?MODULE,?FUNCTION_NAME,[ Monitor ])
  end.

%%=================================================================
%%	Service API
%%=================================================================
on_init()->

  % Initialize subscriptions optimization
  ok = ecomet_router:on_init( ?ENV( router_pool_size, ?ROUTER_POOL_SIZE ) ),

  % Prepare the storage for sessions
  ets:new(?SUBSCRIPTIONS,[named_table,public,set,{keypos, #subscription.id}]),

  % Prepare the storage for index
  ets:new(?S_INDEX,[named_table,public,set,{keypos, #index.key}]),

  ok.

start_link( Id, Owner, Params )->
  Session = self(),
  PID = spawn_link(fun()->
    % Register the subscription process
    case ets:insert_new(?SUBSCRIPTIONS,#subscription{
      id = {Session,Id},
      ts = ecomet_lib:ts(),
      pid = self(),
      owner = Owner
    }) of
      true ->
        % Build subscription index
        build_index(self(), Owner, Params),

        % I'm ready
        Session ! {ok,self()},

        % Enter the wait loop
        wait_for_run(#state{id = {Session,Id}, session = Session, owner = Owner, params = Params}, []);
      _->
        unlink( Session ),
        Session ! {error,self(),{not_unique,Id}}
    end
  end),
  receive
    {ok,PID} -> {ok,PID};
    {error,PID,Error}->{error,Error}
  end.

run(PID, Match)->
  PID ! {run,Match}.

stop(PID) when is_pid( PID )->
  PID ! {stop, self()};
stop(Id)->
  case ets:lookup(?SUBSCRIPTIONS,{oid,Id}) of
    [#subscription{pid = PID,owner = Owner}]->
      PID ! {stop,Owner};
    _->ok
  end.

get_subscriptions( Session )->
  [ #{id => Id, ts => TS, pid => PID} || [Id,TS,PID] <-ets:match(?SUBSCRIPTIONS, #subscription{id = {Session,'$1'}, ts ='$2', pid='$3', _ = '_'})].

%%------------------------------------------------------------
%%  Search engine
%%------------------------------------------------------------
build_index(PID, Owner, #{
  rights:=Rights,
  databases:=DBs,
  tags:=Tags,
  dependencies:=Deps,
  no_feedback:=NoFeedback
})->

  Value = {PID, Owner, NoFeedback},

  % dependencies
  [ begin
    {ok,Unlock} = elock:lock('$subsLocks$', {deps, D}, _IsShared = false, _Timeout = infinity ),
    try
      case ets:lookup(?S_INDEX,{deps, D}) of
        []->
          ets:insert(?S_INDEX, #index{ key = {deps, D}, value = gb_sets:from_list([Value]) });
        [#index{value = Set}]->
          ets:insert(?S_INDEX, #index{ key = {deps, D}, value = gb_sets:add_element(Value,Set) })
      end
    after
      Unlock()
    end end || D <- Deps ],

  % Rights
  [ begin
    {ok,Unlock} = elock:lock('$subsLocks$', {rights, R}, _IsShared = false, _Timeout = infinity ),
    try
      case ets:lookup(?S_INDEX,{rights, R}) of
        []->
          ets:insert(?S_INDEX, #index{ key = {rights, R}, value = gb_sets:from_list([Value]) });
        [#index{value = Set}]->
          ets:insert(?S_INDEX, #index{ key = {rights, R}, value = gb_sets:add_element(Value,Set) })
      end
    after
      Unlock()
    end end || R <- Rights ],

  % Databases
  [ begin
    {ok,Unlock} = elock:lock('$subsLocks$', {db, DB}, _IsShared = false, _Timeout = infinity ),
    try
      case ets:lookup(?S_INDEX,{db, DB}) of
        []->
          ets:insert(?S_INDEX, #index{ key = {db, DB}, value = gb_sets:from_list([Value]) });
        [#index{value = Set}]->
          ets:insert(?S_INDEX, #index{ key = {db, DB}, value = gb_sets:add_element(Value,Set) })
      end
    after
      Unlock()
    end end|| DB <- DBs ],

  % Tags
  [ begin
    {ok,Unlock} = elock:lock('$subsLocks$', {tag, T}, _IsShared = false, _Timeout = infinity ),
    try
      case ets:lookup(?S_INDEX,{tag, T}) of
        []->
          ets:insert(?S_INDEX, #index{ key = {tag, T}, value = gb_sets:from_list([Value]) });
        [#index{value = Set}]->
          ets:insert(?S_INDEX, #index{ key = {tag, T}, value = gb_sets:add_element(Value,Set) })
      end
    after
      Unlock()
    end end|| T <- Tags ],

  ok.

destroy_index(PID, Owner, #{
  rights:=Rights,
  databases:=DBs,
  tags:=Tags,
  dependencies:=Deps,
  no_feedback:=NoFeedback
})->

  Value = {PID, Owner, NoFeedback},

  % dependencies
  [ begin
    {ok,Unlock} = elock:lock('$subsLocks$', {deps, D}, _IsShared = false, _Timeout = infinity ),
    try
      case ets:lookup(?S_INDEX,{deps, D}) of
        []->
          ignore;
        [#index{value = Set}]->
          case gb_sets:delete_any( Value, Set ) of
            {0,nil} ->
              ets:delete(?S_INDEX, {deps, D});
            Set1->
              ets:insert(?S_INDEX, #index{ key = {deps, D}, value = Set1 })
          end
      end
    after
      Unlock()
    end end|| D <- Deps ],

  % Rights
  [ begin
    {ok,Unlock} = elock:lock('$subsLocks$', {rights, R}, _IsShared = false, _Timeout = infinity ),
    try
      case ets:lookup(?S_INDEX,{rights, R}) of
        []->
          ignore;
        [#index{value = Set}]->
          case gb_sets:delete_any( Value, Set ) of
            {0,nil} ->
              ets:delete(?S_INDEX, {rights, R});
            Set1->
              ets:insert(?S_INDEX, #index{ key = {rights, R}, value = Set1 })
          end
      end
    after
      Unlock()
    end end|| R <- Rights ],

  % Databases
  [ begin
    {ok,Unlock} = elock:lock('$subsLocks$', {db, DB}, _IsShared = false, _Timeout = infinity ),
    try

      case ets:lookup(?S_INDEX,{db, DB}) of
        []->
          ignore;
        [#index{value = Set}]->
          case gb_sets:delete_any( Value, Set ) of
            {0,nil} ->
              ets:delete(?S_INDEX, {db, DB});
            Set1->
              ets:insert(?S_INDEX, #index{ key = {db, DB}, value = Set1 })
          end
      end
    after
      Unlock()
    end end|| DB <- DBs ],

  % Tags
  [ begin
    {ok,Unlock} = elock:lock('$subsLocks$', {tag, T}, _IsShared = false, _Timeout = infinity ),
    try
      case ets:lookup(?S_INDEX,{tag, T}) of
        []->
          ignore;
        [#index{value = Set}]->
          case gb_sets:delete_any( Value, Set ) of
            {0,nil} ->
              ets:delete(?S_INDEX, {tag, T});
            Set1->
              ets:insert(?S_INDEX, #index{ key = {tag, T}, value = Set1 })
          end
      end
    after
      Unlock()
    end end|| T <- Tags ],

  ok.

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























