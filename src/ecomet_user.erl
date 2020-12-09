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

-include("ecomet.hrl").

-behaviour(ecomet_object).

%%=================================================================
%%	Service API
%%=================================================================
-export([
  login/2,login/3,
  get_user/0,
  get_usergroups/0,
  on_init_state/0,
  is_admin/0,
  get_state/0,put_state/1,
  get_salt/0
]).

%%===========================================================================
%% Ecomet object behaviour
%%===========================================================================
-export([
  on_create/1,
  on_edit/1,
  on_delete/1
]).

-define(CONTEXT,eCoMeT_uSeRcOnTeXt).
-record(state,{uid,session,is_admin,groups}).

%%=================================================================
%%	Service API
%%=================================================================
login(Login,Pass)->
  login(Login,Pass,#{}).
login(Login,Pass,Info)->
  case ?PIPE([
    fun(_)->ecomet:path2oid(<<"/root/.users/",Login/binary>>) end,
    fun(UserID)->{ok,?OBJECT(UserID)} end,
    fun(User)->
      ?PIPE([
        fun(_)->ecomet:read_field(User,<<"password">>) end,
        fun(CodedPass)->
          case <<CodedPass/binary,(maps:get(<<"salt">>,Info,<<>>))/binary>> of
            Pass->ok;
            _->error
          end
        end,
        fun(_)->create_session(Info) end,
        fun(PID)->set_state(User,PID) end
      ],none)
    end
  ],none) of
    {ok,_}->ok;
    _->error
  end.

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

% Helpers, used to clone state to child processes
get_state()->
  get(?CONTEXT).
put_state(State)->
  put(?CONTEXT,State).

get_salt()->
  list_to_binary(ecomet_lib:guid()).

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  check_pass(Object),
  check_groups(Object),
  check_rights(Object),
  ok.

on_edit(Object)->
  check_pass(Object),
  check_groups(Object),
  check_rights(Object),
  ok.

on_delete(_Object)->
  ok.

check_pass(Object)->
  case ecomet:field_changes(Object,<<"password">>) of
    none->ok;
    {NewPass,_OldPass}->
      ecomet:edit_object(Object,#{<<"password">>=>code_pass(NewPass)})
  end.

check_groups(Object)->
  case ecomet:field_changes(Object,<<"usergroups">>) of
    none->ok;
    { New, _Old } when is_list(New)->
      ecomet:edit_object(Object,#{<<"usergroups">>=> ordsets:from_list(New) });
    { _New, _Old }->
      ecomet:edit_object(Object,#{<<"usergroups">>=>[]})
  end.

check_rights(Object)->
  {ok,Rights}=ecomet:read_field(Object,<<".writegroups">>,[]),
  case lists:member(?OID(Object),Rights) of
    true->ok;
    _->
      ok = ecomet:edit_object(Object,#{ <<".writegroups">>=>[?OID(Object)|Rights] })
  end.

%%=================================================================
%%	Internal helpers
%%=================================================================
code_pass(Pass)->
  %% Converts real password to storage format
  base64:encode(erlang:md5(Pass)).

create_session(_Info)->
  % TODO
  ok.

set_state(User,Session)->
  {ok,UserGroups} = ecomet:read_field(User,<<"usergroups">>),
  IsAdmin=is_admin(UserGroups),
  put(?CONTEXT,#state{
    uid=?OID(User),
    session=Session,
    is_admin=IsAdmin,
    groups=UserGroups
  }),
  ok.

is_admin([GID|Rest])->
  case ecomet_field:lookup_storage(ramdisc,GID,<<".name">>)  of
    <<".administrators">>->true;
    _->is_admin(Rest)
  end;
is_admin([])->
  false.