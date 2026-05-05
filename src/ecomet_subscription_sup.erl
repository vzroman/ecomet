

-module(ecomet_subscription_sup).

-include("ecomet.hrl").

-behaviour(supervisor).

-export([
  start_link/0,
  init/1
]).

-define(MAX_RESTARTS,10).
-define(MAX_PERIOD,1000).
-define(STOP_TIMEOUT,600000).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->

  NodesServer = #{
    id=>ecomet_subscription_nodes,
    start=>{ecomet_subscription_nodes,start_link,[_IsActive = true]},
    restart=>permanent,
    shutdown=> brutal_kill,
    type=>worker,
    modules=>[ecomet_subscription_nodes]
  },

  Pool = #{
    id=>ecomet_subscription_pool,
    start=>{ecomet_subscription_pool,start_link,[]},
    restart=>permanent,
    shutdown=> infinity,
    type=>supervisor,
    modules=>[ecomet_subscription_pool]
  },

  QueryServer = #{
    id=>ecomet_subscription_query,
    start=>{ecomet_subscription_query,start_link,[]},
    restart=>permanent,
    shutdown=> brutal_kill,
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
