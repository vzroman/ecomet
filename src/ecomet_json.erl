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
  on_request/1,
  on_subscription/2
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
on_request(Msg)->
  case try from_json(Msg) catch
    _:_->{error,invalid_format}
  end of
    {error,Error}->reply_error(<<"none">>,Error);
    #{
      <<"id">>:=ID,
      <<"action">>:=Action,
      <<"params">>:=Params
    }->
      case try handle(Action,ID,Params) catch
        _:HandlingError->{error,HandlingError}
      end of
        ok-> reply_ok(ID,ok);
        {ok,Result}->reply_ok(ID,Result);
        {error,Error}->reply_error(ID,Error)
      end;
    _->reply_error(<<"none">>,<<"invalid request">>)
  end.

on_subscription(ClientID,Object)->
  reply(ClientID,<<"ok">>,Object).


reply_ok(ID,Result)->
  reply(ID,ok,Result).

reply_error(ID,{error,Error})->
  reply_error(ID,Error);
reply_error(ID,Error)->
  reply(ID,<<"error">>,?T2B(Error)).

reply(ID,Type,Result)->
  JSON = to_json(#{
    <<"id">>=>ID,
    <<"type">>=>Type,
    <<"result">>=>Result
  }),
  {ok,JSON}.

handle(<<"login">>,_ID,#{<<"login">>:=Login,<<"pass">>:=Pass})->
  case ecomet_user:login(Login,Pass) of
    ok->{ok,ok};
    {error,Error}->{error,Error}
  end;

handle(<<"query">>,_ID,#{<<"statement">>:=Statement})->
  case ecomet:query(Statement) of
    {error,Error}->{error,Error};
    Result->
      JSONResult = query_result(Result),
      {ok, JSONResult}
  end;

handle(<<"application">>,ID,#{
  <<"module">>:=Module,
  <<"function">>:=Function,
  <<"function_params">>:=Params
})->
  case ecomet_lib:is_exported(?B2A(Module),?B2A(Function),2) of
    {ok,Fun}->
      try
        Fun(ID,Params)
      catch
        _Class:Reason->{error,Reason}
      end;
    {error,Error}->{error,Error}
  end;

handle(_Action,_ID,_Params)->
  {error,invalid_request}.
%%=================================================================
%% Utilities
%%=================================================================
to_json(Item)->
  jsx:encode(Item).

from_json(Item)->
  jsx:decode(Item,[return_maps]).


query_result({Count,{Header,Rows}})->
  #{<<"count">>=>Count,<<"result">>=>[Header|export_query_cell(Rows)] };
query_result({Header,Rows})->
  [Header|export_query_cell(Rows)];
query_result(ResultList) when is_list(ResultList)->
  [query_result(R)||R<-ResultList];
query_result(Other)->
  export_query_cell(Other).

export_query_cell(ItemsList) when is_list(ItemsList)->
  [export_query_cell(I)||I<-ItemsList];
export_query_cell(ItemsMap) when is_map(ItemsMap)->
  maps:fold(fun(K,V,Acc)->
    Acc#{ export_query_cell(K)=>export_query_cell(V) }
  end,#{},ItemsMap);
export_query_cell(String) when is_binary(String)->
  String;
export_query_cell(Number) when is_number(Number)->
  Number;
export_query_cell(false)->
  false;
export_query_cell(true)->
  true;
export_query_cell(Atom) when is_atom(Atom)->
  atom_to_binary(Atom,utf8);
export_query_cell(Term)->
  ecomet_types:term_to_string(Term).


