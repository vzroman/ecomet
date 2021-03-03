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
  login/2,login/3,dirty_login/1,dirty_login/2,
  logout/0,
  get_user/0,
  get_usergroups/0,
  get_session/0,
  on_init_state/0,
  is_admin/0,
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
    fun(_)->ecomet_folder:path2oid(<<"/root/.users/",Login/binary>>) end,
    fun(UserID)->{ok,?OBJECT(UserID)} end,
    fun(User)->
      ?PIPE([
        fun(_)->ecomet:read_field(User,<<"password">>) end,
        fun(CodedPass)->
          case Info of
            #{<<"salt">>:=Salt}->
              % If the salt is defined then we consider the pass is given as coded and salted already.
              % Add the salt to the original password to be able to compare values
              case code_pass(<<CodedPass/binary,Salt/binary>>) of
                Pass->ok;
                _->error
              end;
            _->
              % If the salt is not defined the we consider the password is given as is.
              % Code it to be able compare
              case code_pass(Pass) of
                CodedPass->ok;
                _->error
              end
          end
        end,
        fun(_)->set_context(User) end,
        fun(_)->create_session(Info) end
      ],none)
    end
  ],none) of
    {ok,_}->ok;
    _->error
  end.

dirty_login(Login) ->
  dirty_login(Login, #{}).

dirty_login(Login, Info) ->
  case ?PIPE([
    fun(_)->ecomet_folder:path2oid(<<"/root/.users/",Login/binary>>) end,
    fun(UserID)->{ok,?OBJECT(UserID)} end,
    fun(User)->
      ?PIPE([
        fun(_)->set_context(User) end,
        fun(_)->create_session(Info) end
      ],none)
    end
  ],none) of
    {ok,_}->ok;
    _->error
  end.

logout()->
  case erase(?CONTEXT) of
    #state{session = PID} when is_pid(PID)->
      ecomet_session:stop(PID,{shutdown,logout});
    _->ok
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
    #state{uid=UID,groups=Groups}->
      if
        is_list(Groups) ->
          {ok, ordsets:from_list([UID|Groups])};
        true -> {ok,[UID]}
      end
  end.

get_session()->
  case get(?CONTEXT) of
    undefined->{error,user_undefined};
    #state{session = PID} when is_pid(PID)->
      {ok,PID};
    _->
      {error,invalid_session}
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
      case ecomet:is_admin() of
        {ok,true}->ecomet:edit_object(Object,#{<<"usergroups">>=> ordsets:from_list(New) });
        _->?ERROR(access_denied)
      end;
    { _New, _Old }->
      ecomet:edit_object(Object,#{<<"usergroups">>=>[]})
  end.

check_rights(Object)->
  {ok,Rights}=ecomet:read_field(Object,<<".writegroups">>,#{default=>[]}),
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

create_session(Info)->
  Context = get(?CONTEXT),

  {ok,PID} = ecomet_session:start_link(fun()->
    put(?CONTEXT,Context)
  end,Info),

  put(?CONTEXT,Context#state{session = PID}),
  ok.

set_context(User)->

  % Close the session
  logout(),

  % save the context
  {ok,UserGroups} = ecomet:read_field(User,<<"usergroups">>),
  IsAdmin=is_admin(UserGroups),
  put(?CONTEXT,#state{
    uid=?OID(User),
    session=none,
    is_admin=IsAdmin,
    groups=UserGroups
  }),

  ok.

is_admin([GID|Rest])->
  case ecomet_field:lookup_storage(disc,GID,<<".name">>)  of
    <<".administrators">>->true;
    _->is_admin(Rest)
  end;
is_admin([])->
  false.