

-module(ecomet_subscription_object).

-include("ecomet.hrl").
-include("ecomet_subscription.hrl").

-define(CALL_TIMEOUT, 60000).
-define(NAME(N),list_to_atom("ecomet_subscription_object_"++integer_to_list(N))).
-define(WORKER(OID), ?NAME(erlang:phash2(OID, ecomet_subscription_pool:get_size()))).

%%=================================================================
%%        API
%%=================================================================
-export([
  add_subscription_call/2,
  add_subscription_cast/2
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
  subs,
  clients,
  access_denied
}).

%%=================================================================
%%        API
%%=================================================================
add_subscription_call(OID, Subscription=#subscription{})->
  gen_server:call(?WORKER(OID), {add_subscription, OID, Subscription}, ?CALL_TIMEOUT).

add_subscription_cast(OID, Subscription=#subscription{})->
  gen_server:cast(?WORKER(OID), {add_subscription, OID, Subscription}).



%%=================================================================
%%        OTP
%%=================================================================
start_link(N)->
  gen_server:start_link(?NAME(N), ?MODULE, [], []).


init([]) ->

  {ok, #state{
    subs = #{},
    clients = #{},
    access_denied = #{}
  }}.

%%===================================================================
%% OTP generic server callbacks
%%===================================================================
handle_call({add_subscription, OID, Subscription}, _From, State0) ->

  try
    State = add_subscription(OID, Subscription, _Fields = #{}, State0),
    {reply, ok, State}
  catch
    _:E:S->
      ?LOGERROR("add object subscription error: ~p, stack ~p",[E,S]),
      {reply, {error, E}, State}
  end.

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


%-------------------------------------------------------------------
%  Add new subscription
%-------------------------------------------------------------------
add_subscription(
    _OID,
    _Fields,
    #subscription{
      id = SubsID,
      client = Client
    },
    #state{
      clients = #{ Client := #{ SubsID:=_ } }
    } =State)->

  % The subscription already exists, ignore
  State;

add_subscription(OID, Fields, Subscription, State0)->

  State1 = add_object( OID, Subscription, Fields, State0),

  State2 = check_access( OID, Subscription, State1 ),

  add_client( OID, Subscription, State2 ).


%-------------------------------------------------------------------
%  State transformations
%-------------------------------------------------------------------
add_object(
    OID,
    #subscription{

    },
    Fields,
    State0 = #state{

    })->



