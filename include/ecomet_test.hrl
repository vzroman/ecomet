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

-ifndef(ECOMET_TEST).
-define(ECOMET_TEST,1).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(GET(Key,Config),proplists:get_value(Key,Config)).
-define(GET(Key,Config,Default),proplists:get_value(Key,Config,Default)).

-define(DEPENDENCIES,[
  compiler,
  syntax_tools,
  goldrush,
  lager,
  dlss
]).

-define(START_DEPENDENCIES,[
  begin
    ct:pal("starting ~p",[D]),
    ok = application:start(D)
  end|| D <- ?DEPENDENCIES ]).

-define(STOP_DEPENDENCIES,[
  begin
    ct:pal("stopping ~p",[D]),
    application:stop(D)
  end|| D <- lists:reverse(?DEPENDENCIES) ]).

-define(BACKEND_INIT(),
  begin

    ?STOP_DEPENDENCIES,

    mnesia:delete_schema([node()]),
    application:set_env(mnesia, dir,?config(priv_dir,Config)++"/DB_"++atom_to_list(?MODULE)),

    ?START_DEPENDENCIES,

    {ok,_}=ecomet_schema:init([])
  end).

-define(BACKEND_STOP(Timeout),(fun()->

  ?STOP_DEPENDENCIES,
  Wait=fun(R,T)->
    case mnesia:system_info(is_running) of
      no->ok;
      Other->
        ct:pal("is running ~p",[Other]),
        if
          T=<0->error;
          true->
            timer:sleep(1000),
            R(R,T-1000)
        end
    end
  end,
  Wait(Wait,Timeout)
end)()).

-endif.
