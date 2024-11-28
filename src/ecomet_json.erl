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
-module(ecomet_json).

-include("ecomet.hrl").

%%=================================================================
%% Protocol API
%%=================================================================
-export([
  on_request/2,
  handle/4,
  on_subscription/4
]).

%%=================================================================
%% Utiities
%%=================================================================
-export([
  to_json/1,
  from_json/1
]).

%%====================================================================
%% Protocol API
%%====================================================================
on_request(Msg, State)->
  case try from_json(Msg) catch
    _:_->{error,invalid_format}
  end of
    {error,Error}->reply_error(none,Error);
    #{
      <<"id">>:=ID,
      <<"action">>:=Action,
      <<"params">>:=Params
    }->
      case try handle(Action,ID,Params, State) catch
        _:HandlingError->{error,HandlingError}
      end of
        ok->
          {
            reply_ok(ID,ok),
            State
          };
        {ok,Result}->
          {
            reply_ok(ID,Result),
            State
          };
        {error,Error}->
          {
            reply_error(ID,Error),
            State
          }
      end;
    _->
      {
        reply_error(<<"none">>,<<"invalid request">>),
        State
      }
  end.

on_subscription(ID,Action,OID,Fields)->
  reply_ok(ID,#{
    <<"oid">>=> ecomet_types:term_to_string(OID),
    <<"fields">>=>ecomet_types:to_json(Fields),
    <<"action">>=>Action
  }).


reply_ok(ID,Result)->
  reply(ID,ok,Result).

reply_error(ID,{error,Error})->
  reply_error(ID,Error);
reply_error(ID,Error)->
  reply(ID,error,?T2B(Error)).

reply(ID,Type,Result)->
  to_json(#{
    <<"id">>=>ID,
    <<"type">>=>Type,
    <<"result">>=>Result
  }).

handle(<<"login">>,_ID,#{<<"login">>:=Login,<<"pass">>:=Pass}, State)->
  Info =
    case State of
      #{ connection := ConnectionInfo} -> #{ connection => ConnectionInfo };
      _-> #{}
    end,
  case ecomet_user:login(Login, Pass, Info) of
    ok->{ok,ok};
    error->{error,invalid_credentials}
  end;

handle(<<"query">>,_ID,#{<<"statement">>:=Statement}, _State)->
  case ecomet:query(Statement) of
    {error,Error}->{error,Error};
    Result->
      JSONResult = query_result(Result),
      {ok, JSONResult}
  end;
handle(<<"activate_fork">>, ID, #{<<"token">> := Token}, _State) ->
  case ets:lookup(?ECOMET_SESSION_TOKENS,Token) of
		[{Token,Login}] -> 
      case ecomet:dirty_login(Login) of
        ok ->
          {ok, ID};
        _ ->
          {error, access_denied}
        end;
  _ -> {error, access_denied}
end;
handle(<<"fork_connection">>, _ID, _Params, _State) ->
  Token = base64:encode(crypto:hash(sha256,integer_to_binary(erlang:unique_integer([monotonic])))),
  Timeout = ?ENV(auth_timeout, 2000),
  {ok, User} = ecomet:get_user(),
  {ok, Login} = ecomet:read_field(?OBJECT(User), <<".name">>),
  spawn(fun()->
    ets:insert(?ECOMET_SESSION_TOKENS,{Token,Login}),
    timer:sleep(Timeout),
    ets:delete(?ECOMET_SESSION_TOKENS,Token)
  end),
  {ok, Token};
%-----------------------------------------------------------
% Object level actions
%----------------------------------------------------------
handle(<<"create">>,_ID,#{<<"fields">>:=Fields}, _State)->
  Object = ecomet:create_object(Fields,#{ format=>fun ecomet:from_json/2 }),
  OID=?OID(Object),
  {ok, ?T2B(OID)};

handle(<<"update">>,_ID,#{<<"oid">>:=ID,<<"fields">>:=Fields}, _State)->
  ecomet:transaction(fun()->
    Object = ecomet:open(?OID(ID),write),
    ecomet:edit_object( Object , Fields, #{ format=>fun ecomet:from_json/2 }),
    OID =?OID(Object),
    ?T2B(OID)
  end);

handle(<<"delete">>,_ID,#{<<"oid">>:=ID}, _State)->
  ecomet:transaction(fun()->
    Object = ecomet:open(?OID(ID),write),
    ecomet:delete_object( Object ),
    OID=?OID(Object),
    ?T2B(OID)
  end);

handle(_Action,_ID,_Params, _State)->
  {error,invalid_request}.
%%=================================================================
%% Utilities
%%=================================================================
to_json(Item)->
  jsx:encode(Item).

from_json(Item)->
  jsx:decode(Item,[return_maps]).


query_result({Count,{Header,Rows}}) when is_integer(Count),is_list(Header),is_list(Rows)->
  #{<<"count">>=>Count,<<"result">>=>[Header|ecomet_types:to_json(Rows)] };
query_result({Header,Rows}) when is_list(Header),is_list(Rows)->
  [Header|ecomet_types:to_json(Rows)];
query_result(ResultList) when is_list(ResultList)->
  [query_result(R)||R<-ResultList];
query_result(Other)->
  ecomet_types:to_json(Other).




