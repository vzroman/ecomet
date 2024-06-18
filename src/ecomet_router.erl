%%----------------------------------------------------------------
%% Copyright (c) 2020 Faceplate
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
-module(ecomet_router).

-include("ecomet.hrl").
-include("ecomet_subscription.hrl").
%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  on_init/1,
  on_commit/1,
  notify/1,
  global_set/1,
  global_reset/1
]).

-export([
  worker_loop/1
]).


on_init( PoolSize )->

  case pg:start_link( ?MODULE ) of
    {ok,_} -> ok;
    {error,{already_started,_}}->ok;
    {error,Error}-> throw({pg_error, Error})
  end,
  pg:join( ?MODULE , {?MODULE,'$members$'}, self() ),

  spawn_link(fun ready_nodes/0),

  Workers =
    lists:foldl(fun(I, Acc)->
      Acc#{ I => spawn_link(fun()->worker_loop( ?EMPTY_SET ) end) }
    end, #{}, lists:seq(0, PoolSize-1) ),

  persistent_term:put({?MODULE, pool}, { PoolSize, maps:from_list(Workers) }).

worker_loop( Global )->
  receive
    {log, Log} ->
      try ecomet_subscription:on_commit( Log, Global )
      catch
        _:Error:Stack->
          ?LOGERROR("log error ~p, ~p, ~p",[ Log, Error, Stack ])
      end,
      worker_loop( Global );
    {{global_set, Bit}, Ref, ReplyTo}->
      catch ReplyTo ! {Ref, self(), ok},
      worker_loop( ?SET_ADD( Bit, Global ) );
    {{global_reset, Bit}, Ref, ReplyTo}->
      catch ReplyTo ! {Ref, self(), ok},
      worker_loop( ?SET_DEL( Bit, Global ) );
    Unexpected->
      ?LOGWARNING("unexpected message ~p", [Unexpected]),
      worker_loop( Global )
  end.

on_commit( Log )->
  ecall:cast_all(persistent_term:get({?MODULE,ready_nodes},[]), ?MODULE , notify,[ Log ] ),
  notify( Log ).

notify( Log )->
  case persistent_term:get({?MODULE, pool}, none) of
    { PoolSize, Workers }->
      notify( Log, PoolSize, Workers );
    _->
      % Not initialized yet
      ignore
  end.

notify( [ #{oid := OID} = Log | Rest], PoolSize, Workers )->
  W = maps:get( erlang:phash2(OID, PoolSize), Workers),
  W ! { log, Log },
  notify( Rest, PoolSize, Workers );
notify( [], _PoolSize, _Workers )->
  ok.

global_set( Bit )->
  workers_update( {global_set, Bit} ).

global_reset( Bit )->
  workers_update( {global_reset, Bit} ).

workers_update( Message )->
  Ref = make_ref(),

  {_, WorkersMap} = persistent_term:get({?MODULE, pool}),
  Workers = maps:values( WorkersMap ),
  [ Worker ! {Message, Ref, self()} || Worker <- maps:values( Workers )],

  wait_confirm(Workers, Ref).

wait_confirm([], _Ref)->
  ok;
wait_confirm(Workers, Ref)->
  receive
    {Ref, Worker, ok} ->
      wait_confirm( lists:delete( Worker, Workers ), Ref )
  end.

ready_nodes()->
  Nodes = [ node(P) || P <- pg:get_members( ?MODULE, {?MODULE,'$members$'} )],
  catch persistent_term:put({?MODULE, ready_nodes }, Nodes -- [node()]),
  timer:sleep(100),
  ready_nodes().
