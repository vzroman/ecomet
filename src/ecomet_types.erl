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
  parse_value/2,
  value_to_string/2,
  check_value/2,
  string_to_term/1,
  term_to_string/1
]).

%%---------------------------------------------------------------------
%%	Convert value from string to ecomet internal type
%%---------------------------------------------------------------------
parse_value(_Type,<<>>)->
  {ok,none};
parse_value(_Type,none)->
  {ok,none};
parse_value(Type,Value)->
  try
    {ok,parse_safe(Type,Value)}
  catch
      _:_ ->{error,invalid_value}
  end.
%---------String-----------------------
parse_safe(string,Value)
  when is_binary(Value);is_list(Value)->
  unicode:characters_to_binary(Value);
parse_safe(string,Value) when is_integer(Value)->
  integer_to_binary(Value);
parse_safe(string,Value) when is_float(Value)->
  float_to_binary(Value);
parse_safe(string,Value) when is_atom(Value)->
  atom_to_binary(Value,utf8);
parse_safe(string,Term)->
  term_to_string(Term);
%---------Integer----------------------
parse_safe(integer,Value) when is_integer(Value)->
  Value;
parse_safe(integer,Value) when is_float(Value)->
  round(Value);
parse_safe(integer,Value) when is_binary(Value)->
  binary_to_integer(Value);
parse_safe(integer,Value) when is_list(Value)->
  list_to_integer(Value);
parse_safe(integer,Invalid)->
  ?ERROR(Invalid);
%---------Float----------------------
parse_safe(float,Value) when is_float(Value)->
  Value;
parse_safe(float,Value) when is_integer(Value)->
  1.0*Value;
parse_safe(float,Value) when is_binary(Value)->
  binary_to_float(Value);
parse_safe(float,Value) when is_list(Value)->
  list_to_float(Value);
parse_safe(float,Invalid)->
  ?ERROR(Invalid);
%---------Bool----------------------
parse_safe(bool,true)->
  true;
parse_safe(bool,false)->
  true;
parse_safe(bool,<<"true">>)->
  true;
parse_safe(bool,<<"false">>)->
  false;
parse_safe(bool,"true")->
  true;
parse_safe(bool,"false")->
  false;
parse_safe(bool,1)->
  true;
parse_safe(bool,0)->
  false;
parse_safe(bool,Invalid)->
  ?ERROR(Invalid);
%---------Atom----------------------
parse_safe(atom,Value) when is_atom(Value)->
  Value;
parse_safe(atom,Value) when is_binary(Value)->
  binary_to_atom(Value,utf8);
parse_safe(atom,Value) when is_list(Value)->
  list_to_atom(Value);
parse_safe(atom,Invalid)->
  ?ERROR(Invalid);
%---------Binary----------------------
parse_safe(binary,Value) when is_binary(Value)->
  try
    base64:decode(Value)
  catch
      _:_->Value
  end;
parse_safe(binary,Value) when is_list(Value)->
  parse_safe(binary,list_to_binary(Value));
parse_safe(binary,Invalid)->
  ?ERROR(Invalid);
%---------Term----------------------
parse_safe(term,Value) when is_binary(Value)->
  case string_to_term(Value) of
    {ok,Term}->Term;
    _->Value
  end;
parse_safe(term,Value)->
  Value;
%---------Link----------------------
parse_safe(link,Value) when is_binary(Value)->
  {ok,OID}=ecomet_folder:path2oid(Value),
  OID;
parse_safe(link,OID)->
  case ecomet_object:is_oid(OID) of
    true->OID;
    _->?ERROR(OID)
  end;
%---------List----------------------
parse_safe({list,Type},Value) when is_list(Value)->
  [case parse_value(Type,Item) of
     {ok,ItemValue}->ItemValue;
     _->?ERROR(Item)
   end||Item <- Value];
parse_safe({list,Type},Value) when is_binary(Value)->
  case string_to_term(Value) of
    {ok,List} when is_list(List)->
      parse_safe({list,Type},List);
    _->?ERROR(invalid_list)
  end;
parse_safe({list,_Type},Invalid)->
  ?ERROR(Invalid).

%%---------------------------------------------------------------------
%%	Convert value from ecomet internal type to string
%%---------------------------------------------------------------------
value_to_string(_Type,none)->
  <<>>;
value_to_string(string,Value)->
  Value;
value_to_string(integer,Value)->
  integer_to_binary(Value);
value_to_string(float,Value)->
  float_to_binary(Value);
value_to_string(bool,true)->
  <<"true">>;
value_to_string(bool,false)->
  <<"false">>;
value_to_string(atom,Value)->
  atom_to_binary(Value,utf8);
value_to_string(binary,Value)->
  base64:encode(Value);
value_to_string(link,Value)->
  {ok,Path}=ecomet_folder:oid2path(Value),
  Path;
value_to_string(term,Value)->
  term_to_string(Value);
value_to_string({list,Type},Value)->
  List= [ value_to_string(Type,Item) || Item <- Value ],
  ecomet_json:to_json(List).

%%-------------------------------------------------------------------
%%		Check value
%%-------------------------------------------------------------------
check_value(_Type,none)->ok;
check_value(Type,Value)->
  case Type of
    string when is_binary(Value)->ok;
    integer when is_integer(Value)->ok;
    float when is_float(Value)->ok;
    bool when is_boolean(Value)->ok;
    binary when is_binary(Value)->ok;
    atom when is_atom(Value)->ok;
    link->case Value of {_D,_P,_I}->ok; _->error end;
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

