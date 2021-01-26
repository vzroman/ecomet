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
-module(ecomet_types).

-include("ecomet.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
  parse_safe/2,
  check_value/2,
  string_to_term/1,
  term_to_string/1,
  get_supported_types/0
]).

%% ====================================================================
%% Formatters
%% ====================================================================
-export([
  %-------String-----------------
  to_string/2,
  from_string/2,

  %-------JSON-----------------
  to_json/2,
  from_json/2
]).

%%---------------------------------------------------------------------
%%	Convert value from string to ecomet internal type
%%---------------------------------------------------------------------
parse_safe(_Type,<<>>)->
  {ok,none};
parse_safe(_Type,none)->
  {ok,none};
parse_safe(Type,Value)->
  try
    {ok,from_string(Type,Value)}
  catch
      _:_ ->{error,invalid_value}
  end.

%%-------------------------------------------------------------------
%%		Check value
%%-------------------------------------------------------------------
check_value(_Type,none)->ok;
check_value(Type,Value)->
  case Type of
    string when is_binary(Value)->ok;
    integer when is_integer(Value)->ok;
    float when is_number(Value)->ok;
    bool when is_boolean(Value)->ok;
    binary when is_binary(Value)->ok;
    atom when is_atom(Value)->ok;
    link->case ecomet_object:is_oid(Value) of true->ok; _->error end;
    term->ok;
    {list,Subtype} when is_list(Value)->
      check_list_value(Value,Subtype);
    _->error
  end.
check_list_value([Value|Rest],Type)->
  case check_value(Type,Value) of
    ok->check_list_value(Rest,Type);
    error->error
  end;
check_list_value([],_Type)->ok.

%% Parsing string to erlang term
string_to_term(Binary)->
  try
    String=unicode:characters_to_list(<<Binary/binary,".">>),
    {ok,Tokens,_EndLine} = erl_scan:string(String),
    erl_parse:parse_term(Tokens)
  catch
    _:Reason->{error,Reason}
  end.

% Convert erlang term to string
term_to_string(Term)->
  unicode:characters_to_binary(io_lib:format("~p",[Term])).

get_supported_types()->
  Primitives=[
    string,
    integer,
    float,
    bool,
    binary,
    atom,
    link,
    term
  ],
  Primitives ++ [ {list, P} || P <- Primitives ].

%% ====================================================================
%% STRING FORMATTER
%% ====================================================================
from_string(_Any,<<>>)->
  none;
from_string(_Any,<<"none">>)->
  none;
from_string(_Any,none)->
  none;
%---------String-----------------------
from_string(string,Value)
  when is_binary(Value);is_list(Value)->
  unicode:characters_to_binary(Value);
from_string(string,Value) when is_integer(Value)->
  integer_to_binary(Value);
from_string(string,Value) when is_float(Value)->
  float_to_binary(Value);
from_string(string,Value) when is_atom(Value)->
  atom_to_binary(Value,utf8);
from_string(string,Term)->
  term_to_string(Term);
%---------Integer----------------------
from_string(integer,Value) when is_integer(Value)->
  Value;
from_string(integer,Value) when is_float(Value)->
  round(Value);
from_string(integer,Value) when is_binary(Value)->
  binary_to_integer(Value);
from_string(integer,Value) when is_list(Value)->
  list_to_integer(Value);
from_string(integer,Invalid)->
  ?ERROR(Invalid);
%---------Float----------------------
from_string(float,Value) when is_float(Value)->
  Value;
from_string(float,Value) when is_integer(Value)->
  1.0*Value;
from_string(float,Value) when is_binary(Value)->
  try binary_to_float(Value)
  catch
    _:_->1.0*binary_to_integer(Value)
  end;
from_string(float,Value) when is_list(Value)->
  try list_to_float(Value)
  catch
    _:_->1.0*list_to_integer(Value)
  end;
from_string(float,Invalid)->
  ?ERROR(Invalid);
%---------Bool----------------------
from_string(bool,true)->
  true;
from_string(bool,false)->
  true;
from_string(bool,<<"true">>)->
  true;
from_string(bool,<<"false">>)->
  false;
from_string(bool,"true")->
  true;
from_string(bool,"false")->
  false;
from_string(bool,1)->
  true;
from_string(bool,0)->
  false;
from_string(bool,<<"1">>)->
  true;
from_string(bool,<<"0">>)->
  false;
from_string(bool,"1")->
  true;
from_string(bool,"0")->
  false;
from_string(bool,Invalid)->
  ?ERROR(Invalid);
%---------Atom----------------------
from_string(atom,Value) when is_atom(Value)->
  Value;
from_string(atom,Value) when is_binary(Value)->
  binary_to_atom(Value,utf8);
from_string(atom,Value) when is_list(Value)->
  list_to_atom(Value);
from_string(atom,Invalid)->
  ?ERROR(Invalid);
%---------Binary----------------------
from_string(binary,Value) when is_binary(Value)->
  try
    base64:decode(Value)
  catch
    _:_->Value
  end;
from_string(binary,Value) when is_list(Value)->
  from_string(binary,list_to_binary(Value));
from_string(binary,Invalid)->
  ?ERROR(Invalid);
%---------Term----------------------
from_string(term,Value) when is_binary(Value)->
  case string_to_term(Value) of
    {ok,Term}->Term;
    _->Value
  end;
from_string(term,Value)->
  Value;
%---------Link----------------------
from_string(link,Value) when is_binary(Value)->
  {ok,OID}=ecomet_folder:path2oid(Value),
  OID;
from_string(link,OID)->
  case ecomet_object:is_oid(OID) of
    true->OID;
    _->?ERROR(OID)
  end;
%---------List----------------------
from_string({list,Type},Value) when is_list(Value)->
  [case parse_safe(Type,Item) of
     {ok,ItemValue}->ItemValue;
     _->?ERROR(Item)
   end||Item <- Value];
from_string({list,Type},Value) when is_binary(Value)->
  case string_to_term(Value) of
    {ok,List} when is_list(List)->
      from_string({list,Type},List);
    {ok,Term}->[from_string(Type,Term)]
  end;
from_string({list,_Type},Invalid)->
  ?ERROR(Invalid).

%%---------------------------------------------------------------------
%%	Convert value from ecomet internal type to string
%%---------------------------------------------------------------------
to_string(_Type,none)->
  <<"none">>;
to_string(_Type,undefined_field)->
  <<"undefined_field">>;
to_string(string,Value)->
  Value;
to_string(integer,Value)->
  integer_to_binary(Value);
to_string(float,Value)->
  float_to_binary(1.0*Value);
to_string(bool,true)->
  <<"true">>;
to_string(bool,false)->
  <<"false">>;
to_string(atom,Value)->
  atom_to_binary(Value,utf8);
to_string(binary,Value)->
  base64:encode(Value);
to_string(link,Value)->
  ecomet_folder:oid2path(Value);
to_string(term,Value)->
  term_to_string(Value);
to_string({list,Type},Value)->
  List= [ to_string(Type,Item) || Item <- Value ],
  ecomet_json:to_json(List).

%% ====================================================================
%% JSON FORMATTER
%% ====================================================================
from_json(_Type,null)->
  none;
from_json(Type,Value)->
  % The default from_string parser is flexible enough
  from_string(Type,Value).

to_json(_Type,none)->
  null;
% JSON friendly types
to_json(bool,Value)->
  Value;
to_json(integer,Value)->
  Value;
to_json(float,Value)->
  1.0*Value;
to_json({list,Type},Value)->
  [ to_json(Type,Item) || Item <- Value ];
% Other types are converted to a string
to_json(Type,Value)->
  to_string(Type,Value).
