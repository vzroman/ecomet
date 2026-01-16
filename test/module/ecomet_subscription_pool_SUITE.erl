
-module(ecomet_subscription_pool_SUITE).

-include_lib("ecomet_test.hrl").

%% API
-export([
  all/0,
  groups/0,
  init_per_testcase/2,
  end_per_testcase/2,
  init_per_group/2,
  end_per_group/2,
  init_per_suite/1,
  end_per_suite/1
]).


-export([
  api_test/1
]).


all()->
  [
    api_test
  ].

groups()->
  [].

%% Init system storages
init_per_suite(Config)->
  Config.
end_per_suite(_Config)->
  ok.

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

%--------------------------------------------------------------
% Set bit
%--------------------------------------------------------------
api_test(_Config) ->
  {ok, _} = ecomet_subscription_pool:start_link(),

  PoolSize = ecomet_subscription_pool:get_size(),
  ?assertEqual(true, is_integer(PoolSize)),

  Workers = ecomet_subscription_pool:get_workers(),
  ?assertEqual(PoolSize, length(Workers)),

  ?assertEqual(0, hd(Workers)),
  ?assertEqual(PoolSize-1, lists:last(Workers)),

  ok.
