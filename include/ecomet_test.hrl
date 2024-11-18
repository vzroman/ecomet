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
  ecomet
]).

-define(START_DEPENDENCIES,[
  begin
    ct:pal("starting ~p",[D]),
    {ok,_} = application:ensure_all_started(D),
    ok = application:ensure_started(D)
  end|| D <- ?DEPENDENCIES ]).

-define(STOP_DEPENDENCIES,[
  begin
    ct:pal("stopping ~p",[D]),
    application:stop(D),
    zaya:stop()
  end|| D <- lists:reverse(?DEPENDENCIES) ]).

-define(SUITE_PROCESS_START(),spawn(fun()->
  F=fun(R)->
    receive
      stop->ok;
      {execute,Fun}->
        Fun(),
        R(R)
    end
    end,
  F(F)
 end)).

-define(SUITE_PROCESS_STOP(PID),PID!stop).

-define(SUITE_PROCESS_EXECUTE(PID,Fun),PID!{execute,Fun}).

-define(PROCESSLOG(PID,Record),PID!{'_LOG_',Record}).
-define(PROCESSLOG(Record),?PROCESSLOG(self(),Record)).

-define(GETLOG,fun()->
  Receive
    =fun(R,Acc)->
    receive
      {'_LOG_',Record}->R(R,[Record|Acc])
    after
      100->lists:reverse(Acc)
    end
     end,
  Receive(Receive,[])
 end).

-define(BACKEND_INIT(),
  begin

    ?STOP_DEPENDENCIES,

    % mnesia:delete_schema([node()]),
    % application:set_env(mnesia, dir,?config(priv_dir,Config)++"/DB_"++atom_to_list(?MODULE)),

    ?START_DEPENDENCIES

    % {ok,_}=ecomet_schema:init([])
  end).

-define(BACKEND_STOP(),(fun()->

  ?STOP_DEPENDENCIES

end)()).

-endif.
