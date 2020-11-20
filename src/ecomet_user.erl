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
-module(ecomet_user).

%%=================================================================
%%	Service API
%%=================================================================
-export([
  get_user/0,
  get_usergroups/0,
  on_init_state/0,
  is_admin/0
]).

-define(CONTEXT,eCoMeT_uSeRcOnTeXt).
-record(state,{uid,session,is_admin,groups}).

%%=================================================================
%%	Service API
%%=================================================================
%% Get OID of current user
get_user()->
  case get(?CONTEXT) of
    undefined->{error,user_undefined};
    #state{uid=UID}->{ok,UID}
  end.

get_usergroups()->
  case get(?CONTEXT) of
    undefined->{error,user_undefined};
    #state{groups=Groups}->
      case Groups of
        none->{ok,[]};
        _->{ok,Groups}
      end
  end.

% Set user context for master init procedure
on_init_state()->
  put(?CONTEXT,#state{
    uid=none,
    session=none,
    is_admin=true,
    groups=[]
  }).

%% Check if current user is admin
is_admin()->
  case get(?CONTEXT) of
    undefined->{error,user_undefined};
    #state{is_admin=IsAdmin}->{ok,IsAdmin}
  end.