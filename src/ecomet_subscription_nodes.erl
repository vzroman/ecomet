

-module(ecomet_subscription_nodes).

-include("ecomet.hrl").

%%=================================================================
%%        API
%%=================================================================
-export([
  get_active/0
]).

%%====================================================================
%%        Test API
%%====================================================================
-ifdef(TEST).

-export([
  request_pool_sizes/1
]).

-endif.

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

  ActiveNodes = request_pool_sizes(remote_nodes(PIDs)),
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

  NewNodes = request_pool_sizes(remote_nodes(PIDs)),
  ExistingNodes = get_active(),
  ActiveNodes = merge_nodes( ExistingNodes, NewNodes ),

  persistent_term:put({?MODULE, ready_nodes }, ActiveNodes),

  {noreply, State};

handle_info({Ref, leave, ?MODULE, PIDs}, #state{
  monitor_ref = Ref
} = State) ->

  DownNodes = remote_nodes(PIDs),
  ExistingNodes = get_active(),
  ActiveNodes = remove_nodes( ExistingNodes, DownNodes ),

  persistent_term:put({?MODULE, ready_nodes }, ActiveNodes),

  {noreply, State};

handle_info(Info, State) ->
  ?LOGWARNING("unexpected info event ~p", [Info]),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

remote_nodes(PIDs)->
  ordsets:from_list([
    node(PID) || PID <- PIDs, node(PID) =/= node()
  ]).

request_pool_sizes(Nodes)->
  lists:foldl(
    fun(Node, Acc)->
      case request_pool_size(Node) of
        {ok, NodeSize}->
          [NodeSize | Acc];
        error->
          Acc
      end
    end,
    [],
    Nodes
  ).

request_pool_size(Node)->
  case ecall:call(Node, ecomet_subscription_pool, get_size, []) of
    {ok, Size} when is_integer(Size), Size > 0 ->
      {ok, {Node, Size}};
    {ok, Size}->
      ?LOGWARNING("ignore subscription node ~p with pool size ~p", [Node, Size]),
      error;
    {error, Error}->
      ?LOGWARNING("could not read subscription pool size from ~p: ~p", [Node, Error]),
      error
  end.

merge_nodes(ExistingNodes, NewNodes)->
  lists:sort(
    maps:to_list(
      maps:merge(
        maps:from_list(ExistingNodes),
        maps:from_list(NewNodes)
      )
    )
  ).

remove_nodes(ExistingNodes, DownNodes)->
  [NodeSize || {Node, _Size} = NodeSize <- ExistingNodes,
    not ordsets:is_element(Node, DownNodes)].

