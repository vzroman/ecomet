

-module(ecomet_subscription_sup).

-include("ecomet.hrl").

-behaviour(supervisor).

-export([
  on_init/0
]).

-export([
  start_link/0,
  init/1
]).

-define(MAX_RESTARTS,10).
-define(MAX_PERIOD,1000).
-define(STOP_TIMEOUT,600000).

on_init()->

  case ?ENV(disable_subscriptions, false) of
    false ->
      case supervisor:start_child( ecomet_sup, #{
        id=>?MODULE,
        start=>{ ?MODULE ,start_link ,[]},
        restart=>permanent,
        shutdown=> infinity,
        type=>supervisor,
        modules=>[?MODULE]
      }) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, already_present} -> ok;
        {error, {already_started, _}} -> ok;
        {error, Error}->
          ?LOGERROR("subscriptions environment start error: ~p",[ Error ]),
          throw(Error)
      end;
    _->
      case supervisor:start_child( ecomet_sup, #{
        id=>ecomet_subscription_nodes,
        start=>{ecomet_subscription_nodes,start_link,[_IsActive = false]},
        restart=>permanent,
        shutdown=> ?STOP_TIMEOUT,
        type=>worker,
        modules=>[ecomet_subscription_nodes]
      }) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, already_present} -> ok;
        {error, {already_started, _}} -> ok;
        {error, Error}->
          ?LOGERROR("subscriptions environment start error: ~p",[ Error ]),
          throw(Error)
      end
  end.


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->

  ecomet_subscription:on_init(),

  NodesServer = #{
    id=>ecomet_subscription_nodes,
    start=>{ecomet_subscription_nodes,start_link,[_IsActive = true]},
    restart=>permanent,
    shutdown=> ?STOP_TIMEOUT,
    type=>worker,
    modules=>[ecomet_subscription_nodes]
  },

  Pool = #{
    id=>ecomet_subscription_pool,
    start=>{ecomet_subscription_pool,start_link,[]},
    restart=>permanent,
    shutdown=> ?STOP_TIMEOUT,
    type=>supervisor,
    modules=>[ecomet_subscription_pool]
  },

  QueryServer = #{
    id=>ecomet_subscription_query,
    start=>{ecomet_subscription_query,start_link,[]},
    restart=>permanent,
    shutdown=> ?STOP_TIMEOUT,
    type=>worker,
    modules=>[ecomet_subscription_query]
  },

  Supervisor=#{
    strategy=>one_for_one,
    intensity=>?ENV(supervisor_max_restarts, ?MAX_RESTARTS),
    period=>?ENV(supervisor_max_period, ?MAX_PERIOD)
  },

  {ok, {Supervisor,
    [
      NodesServer,
      Pool,
      QueryServer
    ]
  }}.
