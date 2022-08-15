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
  start_link/3,
  run/2,
  stop/1,

  get_subscriptions/1,

  on_commit/1,
  notify/2
]).

-record(subscription,{id, pid, ts, owner }).
-record(index,{key, value}).

-record(state,{id, session, owner, params, match}).

-define(SUBSCRIPTIONS,ecomet_subscriptions).
-define(S_INDEX,ecomet_subscriptions_index).

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
  case ets:insert_new(?SUBSCRIPTIONS,{Owner,Id}) of
    true->

      PID = spawn_link(fun()->

        % Register the subscription process
        ets:insert(?SUBSCRIPTIONS,#subscription{
          id = {Session,Id},
          ts = ecomet_lib:ts(),
          pid = self(),
          owner = Owner
        }),

        % Enter the wait loop
        wait_for_run(#state{id = {Session,Id}, session = Session, owner = Owner, params = Params}, [])
      end),

      % Build subscription index
      build_index(PID, Owner, Params),

      {ok,PID};
    _->
      {error,{not_unique,Id}}
  end.

run(PID, Match)->
  PID ! {run,Match}.

stop(PID)->
  PID ! {stop, self()}.

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
  [ case ets:lookup(?S_INDEX,{deps, D}) of
      []->
        ets:insert(?S_INDEX, #index{ key = {deps, D}, value = gb_sets:from_list([Value]) });
      [#index{value = Set}]->
        ets:insert(?S_INDEX, #index{ key = {deps, D}, value = gb_sets:add_element(Value,Set) })
    end || D <- Deps ],

  % Rights
  [ case ets:lookup(?S_INDEX,{rights, R}) of
      []->
        ets:insert(?S_INDEX, #index{ key = {rights, R}, value = gb_sets:from_list([Value]) });
      [#index{value = Set}]->
        ets:insert(?S_INDEX, #index{ key = {rights, R}, value = gb_sets:add_element(Value,Set) })
    end || R <- Rights ],

  % Databases
  [ case ets:lookup(?S_INDEX,{db, DB}) of
      []->
        ets:insert(?S_INDEX, #index{ key = {db, DB}, value = gb_sets:from_list([Value]) });
      [#index{value = Set}]->
        ets:insert(?S_INDEX, #index{ key = {db, DB}, value = gb_sets:add_element(Value,Set) })
    end || DB <- DBs ],

  % Tags
  [ case ets:lookup(?S_INDEX,{tag, T}) of
      []->
        ets:insert(?S_INDEX, #index{ key = {tag, T}, value = gb_sets:from_list([Value]) });
      [#index{value = Set}]->
        ets:insert(?S_INDEX, #index{ key = {tag, T}, value = gb_sets:add_element(Value,Set) })
    end || T <- Tags ],

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
  [ case ets:lookup(?S_INDEX,{deps, D}) of
      []->
        ignore;
      [#index{value = Set}]->
        case gb_sets:delete_any( Value, Set ) of
          {0,nil} ->
            ets:delete(?S_INDEX, {deps, D});
          Set1->
            ets:insert(?S_INDEX, #index{ key = {deps, D}, value = Set1 })
        end
    end || D <- Deps ],

  % Rights
  [ case ets:lookup(?S_INDEX,{rights, R}) of
      []->
        ignore;
      [#index{value = Set}]->
        case gb_sets:delete_any( Value, Set ) of
          {0,nil} ->
            ets:delete(?S_INDEX, {rights, R});
          Set1->
            ets:insert(?S_INDEX, #index{ key = {rights, R}, value = Set1 })
        end
    end || R <- Rights ],

  % Databases
  [ case ets:lookup(?S_INDEX,{db, DB}) of
      []->
        ignore;
      [#index{value = Set}]->
        case gb_sets:delete_any( Value, Set ) of
          {0,nil} ->
            ets:delete(?S_INDEX, {db, DB});
          Set1->
            ets:insert(?S_INDEX, #index{ key = {db, DB}, value = Set1 })
        end
    end || DB <- DBs ],

  % Tags
  [ case ets:lookup(?S_INDEX,{tag, T}) of
      []->
        ignore;
      [#index{value = Set}]->
        case gb_sets:delete_any( Value, Set ) of
          {0,nil} ->
            ets:delete(?S_INDEX, {tag, T});
          Set1->
            ets:insert(?S_INDEX, #index{ key = {tag, T}, value = Set1 })
        end
    end || T <- Tags ],

  ok.

on_commit(#ecomet_log{ changes = undefined })->
  % No changes
  ok;
on_commit(#ecomet_log{
  db = DB,
  tags = {TAdd, TOld, TDel},
  rights = { RAdd, ROld, RDel },
  changes = Changes
} = Log)->

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
  end.

clean_up( #state{ id =Id, owner = Owner, params = Params} )->
  % Unregister the subscription process
  ets:delete(?SUBSCRIPTIONS,Id),

  % Destroy index
  destroy_index( self(), Owner, Params ),

  ok.























