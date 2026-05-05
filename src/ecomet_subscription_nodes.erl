

-module(ecomet_subscription_nodes).

-include("ecomet.hrl").

%%=================================================================
%%        API
%%=================================================================
-export([
  get_active/0
]).

%%=================================================================
%%        OTP API
%%=================================================================
-export([
  start_link/1,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-record(state, {
  monitor_ref
}).

%%=================================================================
%%        API
%%=================================================================
get_active()->
  persistent_term:get({?MODULE,ready_nodes},[]).


%%=================================================================
%%        OTP
%%=================================================================
start_link(IsActive)->
  gen_server:start_link(?MODULE, [IsActive], []).


init([IsActive]) ->

  case pg:start_link( ?MODULE ) of
    {ok,_} -> ok;
    {error,{already_started,_}}->ok;
    {error,Error}-> throw({pg_error, Error})
  end,

  if
    IsActive ->
      pg:join( ?MODULE , ?MODULE, self() );
    true->
      ignore
  end,

  {Ref, PIDs} = pg:monitor( ?MODULE, ?MODULE ),

  ActiveNodes = ordsets:from_list([ node(PID) || PID <- PIDs, PID =/= self() ]),
  persistent_term:put({?MODULE, ready_nodes }, ActiveNodes),

  {ok, #state{
    monitor_ref = Ref
  }}.

%%===================================================================
%% OTP generic server callbacks
%%===================================================================
handle_call(Request, From, State) ->
  ?LOGWARNING("unexpected call request ~p from ~p", [Request, From]),
  {reply, ok, State}.

handle_cast(Request,State) ->
  ?LOGWARNING("unexpected cast request ~p", [Request]),
  {noreply, State}.


handle_info({Ref, join, ?MODULE, PIDs}, #state{
  monitor_ref = Ref
} = State) ->

  NewNodes = ordsets:from_list([ node(PID) || PID <- PIDs, PID =/= self() ]),
  ExistingNodes = get_active(),
  ActiveNodes = ordsets:union( ExistingNodes, NewNodes ),

  persistent_term:put({?MODULE, ready_nodes }, ActiveNodes),

  {noreply, State};

handle_info({Ref, leave, ?MODULE, PIDs}, #state{
  monitor_ref = Ref
} = State) ->

  DownNodes = ordsets:from_list([ node(PID) || PID <- PIDs, PID =/= self() ]),
  ExistingNodes = get_active(),
  ActiveNodes = ordsets:subtract( ExistingNodes, DownNodes ),

  persistent_term:put({?MODULE, ready_nodes }, ActiveNodes),

  {noreply, State};

handle_info(Info, State) ->
  ?LOGWARNING("unexpected info event ~p", [Info]),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.




