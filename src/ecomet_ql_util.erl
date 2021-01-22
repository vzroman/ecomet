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

-module(ecomet_ql_util).

-include("ecomet.hrl").

%%====================================================================
%%		Query Language Utilities
%%====================================================================
-export([
  oid/1,
  path/1,
  join/1,
  concat/2,concat/1,
  string/1,
  term/1,
  to_base64/1,
  from_base64/1
]).

%%====================================================================
%%		Utilities
%%====================================================================
oid(List) when is_list(List)->
  [oid(I)||I<-List];
oid(ID)->
  ?OID(ID).

path(List) when is_list(List)->
  [path(I)||I<-List];
path(ID)->
  ?PATH(ID).

join([none|_Tail])->
  none;
join([])->
  none;
join([OID,Field|Tail])->
  case try ecomet:read_field(?OBJECT(OID),Field) catch
         _:Error->{error,Error}
  end of
    {ok,NextOID}->join([NextOID|Tail]);
    {error,_}->none
  end;
join([Value])->
  Value.

concat(Value) when is_binary(Value)->
  Value;
concat([Value|Rest])->
  <<(concat(Value))/binary,(concat(Rest))/binary>>;
concat([])->
  <<>>;
concat(_)->
  ?ERROR(bad_arg).

concat(Value1,Value2) when is_binary(Value1),is_binary(Value2)->
  <<Value1/binary,Value2/binary>>;
concat(Head,Items) when is_binary(Head),is_list(Items)->
  [concat(Head,concat(I))||I<-Items];
concat(Items,Tail) when is_list(Items),is_binary(Tail)->
  [concat(concat(I),Tail)||I<-Items];
concat(Items1,Items2) when is_list(Items1),is_list(Items2)->
  [concat(I1,I2)||I1<-Items1,I2<-Items2];
concat(_,_)->
  ?ERROR(bad_arg).

string(Term)->
  ecomet_types:term_to_string(Term).

term(List) when is_list(List)->
  [term(I)||I<-List];
term(String) when is_binary(String)->
  {ok,Term}=ecomet_types:string_to_term(String),
  Term;
term(Term)->
  Term.

to_base64(Value) when is_binary(Value)->
  base64:encode(Value);
to_base64(Items) when is_list(Items)->
  [to_base64(I)||I<-Items];
to_base64(_)->
  ?ERROR(bad_arg).

from_base64(Value) when is_binary(Value)->
  base64:decode(Value);
from_base64(Items) when is_list(Items)->
  [from_base64(I)||I<-Items].

