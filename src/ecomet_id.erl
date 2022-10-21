%%----------------------------------------------------------------
%% Copyright (c) 2022 Faceplate
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
-module(ecomet_id).

-include("ecomet.hrl").
-include("ecomet_schema.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  init/0,
  new/1
]).

%%====================================================================
%% PERSISTENT
%%====================================================================
-define(logModule,zaya_leveldb).
-define(dir,?SCHEMA_DIR++"/OID").
-define(params,
  #{
    dir => ?dir,
    eleveldb => #{
      open_options=>#{
        paranoid_checks => false,
        verify_compactions => false,
        compression => false
      },
      read => #{
        verify_checksums => false
      },
      write => #{
        sync => true
      }
    }
  }
).

%%====================================================================
%% Refs
%%====================================================================
-define(log,{?MODULE,log}).
-define(ets,{?MODULE,ets}).

-define(logRef,persistent_term:get(?log)).
-define(etsRef,persistent_term:get(?ets)).

%%====================================================================
%%		API
%%====================================================================
init()->
  LogRef =
    case filelib:is_file(?dir ++ "/CURRENT") of
      false->
        log_create();
      _->
        log_open()
    end,

  persistent_term:put(?log, LogRef),

  EtsRef = ets:new(?MODULE,[public,set]),
  persistent_term:put(?ets, EtsRef),

  ets:insert(EtsRef, ?logModule:find(LogRef,#{})),

  ok.

log_create()->
  try ?logModule:create(?params)
  catch
    _:E->
      ?LOGERROR("create log id error ~p, close the application, fix the problem and try start again",[E]),
      timer:sleep(infinity)
  end.

log_open()->
  try ?logModule:open(?params)
  catch
    _:E->
      ?LOGERROR("open log id error ~p, close the application, fix the problem and try start again",[E]),
      timer:sleep(infinity)
  end.


new(Scope)->
  ID = ets:update_counter(?etsRef, Scope, {2,1}, {Scope,0}),
  if
    ID rem ?BITSTRING_LENGTH =:= 1->
      ?logModule:write(?logRef,[{Scope,ID + ?BITSTRING_LENGTH - 1}]);
    true->
      ignore
  end,
  ID.
