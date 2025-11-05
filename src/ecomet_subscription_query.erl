

-module(ecomet_subscription_query).

-include("ecomet.hrl").

-define(NAME(N),list_to_atom("ecomet_subscription_object_"++integer_to_list(N))).
-define(S_QUERY,ecomet_subscriptions_query).
-define(S_INDEX,ecomet_subscriptions_index).

%%=================================================================
%%        API
%%=================================================================
-export([

]).

%%=================================================================
%%        OTP API
%%=================================================================
-export([
  start_link/0,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-record(state, {

}).

%%=================================================================
%%        API
%%=================================================================



%%=================================================================
%%        OTP
%%=================================================================
start_link()->
  gen_server:start_link(?MODULE, ?MODULE, [], []).


init([]) ->

  % Prepare the storage for query subscriptions
  ets:new(?S_QUERY,[
    named_table,
    public,
    set,
    {read_concurrency, true},
    {write_concurrency,true}
  ]),

  % Prepare the storage for index
  ets:new(?S_INDEX,[
    named_table,
    public,
    set,
    {read_concurrency, true},
    {write_concurrency,true}
  ]),

  {ok, #state{

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


handle_info(Info, State) ->
  ?LOGWARNING("unexpected info event ~p", [Info]),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.




