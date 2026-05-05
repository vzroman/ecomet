

-module(ecomet_subscription_pool).

-include("ecomet.hrl").

-behaviour(supervisor).

%%=================================================================
%%        API
%%=================================================================
-export([
  get_size/0,
  get_workers/0
]).

%%=================================================================
%%        OTP API
%%=================================================================
-export([
  start_link/0,
  init/1
]).

-define(MAX_RESTARTS,10).
-define(MAX_PERIOD,1000).
-define(STOP_TIMEOUT,600000).

%%=================================================================
%%        API
%%=================================================================
get_size()->
  persistent_term:get({?MODULE,pool_size},[]).

get_workers()->
  PoolSize = get_size(),
  lists:seq(0, PoolSize-1).

%%=================================================================
%%        OTP
%%=================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->

  PoolSize = erlang:system_info(logical_processors),
  persistent_term:put({?MODULE,pool_size}, PoolSize),

  Workers =
    [ #{
      id=> N,
      start=>{ecomet_subscription_object, start_link,[ N ]},
      restart=>permanent,
      shutdown=> brutal_kill,
      type=>worker,
      modules=>[ecomet_subscription_object]
    } || N <- lists:seq(0, PoolSize-1)],


  Supervisor=#{
    strategy=>one_for_one,
    intensity=>?ENV(supervisor_max_restarts, ?MAX_RESTARTS),
    period=>?ENV(supervisor_max_period, ?MAX_PERIOD)
  },

  {ok, {Supervisor, Workers}}.
