%%----------------------------------------------------------------
%% Copyright (c) 2024 Faceplate
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
-module(ecomet_queue).

-include("ecomet.hrl").

%% ====================================================================
%% Service API
%% ====================================================================
-export([
  start_link/0,
  enqueue/1,
  waiting/1
]).

-record( state,{ queue, monitor, actors, waiting }).
-record( queue, { term, ref, actor } ).

start_link()->
  {ok, spawn_link(fun init/0)}.


enqueue( Term )->
  Ref = make_ref(),
  Actor = self(),
  ?MODULE ! #queue{ term = Term, ref = Ref, actor = Actor },
  {Term, Ref}.

waiting({ Term, Ref })->

  ?MODULE ! { waiting, Term, Ref, self() },
  receive { your_turn, Ref } -> ok end.

init()->

  register( ?MODULE, self() ),

  Queue = ets:new(queue,[ private, set ]),

  loop(#state{
    queue = Queue,
    monitor = #{},
    actors = #{},
    waiting = #{}
  }).

loop( #state{
  actors = Actors
} = State0 )->
  receive
    #queue{ term = Term, ref = Ref, actor = Actor } ->
      
      case Actors of
        #{ Actor := #{ Term := Ref0 } } ->
          % Why is doing it again?
          State1 = dequeue( Term, Ref0, Actor, State0 ),
          State = enqueue( Term, Ref, Actor, State1 ),
          loop( State );
        _->
          State = enqueue( Term, Ref, Actor, State0 ),
          loop( State )
      end;
    { waiting, Term, Ref, Actor }->

      State = waiting( Term, Ref, Actor, State0 ),
      loop( State );

    {'DOWN', _MonRef, process, Actor, _Reason}->
      case Actors of
        #{ Actor := ActorTerms }->
          State = maps:fold(fun(Term, Ref, Acc)->
            dequeue( Term, Ref, Actor, Acc )
          end, State0, ActorTerms ),
          loop( State );
        _->
          loop( State0 )
      end;
    _->
      loop( State0 )
  end.

enqueue( Term, Ref, Actor, #state{
  monitor = Monitor0,
  actors = Actors0,
  queue = Queue
} = State0 )->

  Monitor =
    case maps:is_key( Actor, Monitor0 ) of
      false -> Monitor0#{ Actor => erlang:monitor(process, Actor ) };
      true -> Monitor0
    end,

  %% Actors: 
  %% #{
  %%    Actor => #{
  %%      Term => Ref
  %%    }
  %% }

  ActorTerms0 = maps:get( Actor, Actors0, #{} ),
  Actors = Actors0#{
    Actor => ActorTerms0#{ Term => Ref }
  },
  
  ets:insert( Queue, {{ Term, Ref }, Actor }),
  
  State0#state{ 
    monitor = Monitor,
    actors = Actors
  }.

waiting( Term, Ref, Actor, #state{
  waiting = Waiting0
} = State0 )->

  %% Waiting:
  %% #{
  %%    Term => #{
  %%      Actor => true
  %%    }
  %% }

  TermWaiting0 = maps:get( Term, Waiting0, #{} ),
  TermWaiting = TermWaiting0#{ Actor => true },

  Waiting = Waiting0#{ Term => TermWaiting  },

  trigger_queue(Term, Ref, State0#state{ waiting = Waiting } ).


dequeue(Term, Ref, Actor, #state{
  monitor = Monitor0,
  actors = Actors0,
  queue = Queue,
  waiting = Waiting0
} = State0 )->

  ets:delete( Queue, { Term, Ref } ),

  ActorTerms0 = maps:get( Actor, Actors0, #{} ),
  ActorTerms = maps:remove( Term, ActorTerms0 ),

  Actors =
    if
      map_size( ActorTerms ) =:= 0 -> maps:remove( Actor, Actors0 );
      true -> Actors0#{ Actor => ActorTerms }
    end,

  Monitor =
    case maps:is_key( Actor, Actors ) of
      false ->
        % Actor isn't queued for any terms anymore
        MonRef = maps:get( Actor, Monitor0 ),
        catch erlang:demonitor( MonRef ),
        maps:remove( Actor, Monitor0 );
      _->
        % Actor is still queued
        Monitor0
    end,

  TermWaiting0 = maps:get( Term, Waiting0, #{} ),
  TermWaiting = maps:remove( Actor, TermWaiting0 ),

  Waiting =
    if
      map_size( TermWaiting ) =:= 0 -> maps:remove( Term, Waiting0 );
      true -> Waiting0#{ Term => TermWaiting }
    end,

  trigger_queue(Term, Ref, State0#state{
    monitor = Monitor,
    actors = Actors,
    waiting = Waiting
  }).

trigger_queue(Term, Ref, #state{
  queue = Queue,
  waiting = Waiting
} = State )->

  %% Waiting:
  %% #{
  %%    Term => #{
  %%      Actor => true
  %%    }
  %% }
  case maps:is_key( Term, Waiting ) of
    false ->
      % Nobody is waiting for term
      State;
    true ->
      case ets:prev( Queue, { Term, Ref } ) of
        { Term, _PrevRef }->
          % The ref was not first in the queue
          State;
        _->
          % The ref was first
          % Strict match to Term!!! If somebody is waiting then it must be in the queue
          {Term, NextRef} = ets:next( Queue, { Term, Ref } ),
          [{_, NextActor}] = ets:lookup( Queue, {Term, NextRef} ),
          case Waiting of
            #{ Term := #{ NextActor := _ } }->
              % The next actor is already waiting
              catch NextActor ! { your_turn, NextRef },
              dequeue( Term, NextRef, NextActor, State );
            _->
              State
          end
      end
  end.

