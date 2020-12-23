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
Definitions.

AND = (A|a)(N|n)(D|d)
ANDNOT = (A|a)(N|n)(D|d)(N|n)(O|o)(T|t)
AS = (A|a)(S|s)
ASC = (A|a)(S|s)(C|c)
BY = (B|b)(Y|y)
DELETE = (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
DESC = (D|d)(E|e)(S|s)(C|c)
GROUP = (G|g)(R|r)(O|o)(U|u)(P|p)
GET = (G|g)(E|e)(T|t)
SUBSCRIBE = (S|s)(U|u)(B|b)(S|s)(C|c)(R|r)(I|i)(B|b)(E|e)
INSERT = (I|i)(N|n)(S|s)(E|e)(R|r)(T|t)
LOCK = (L|l)(O|o)(C|c)(K|k)
OR = (O|o)(R|r)
ORDER = (O|o)(R|r)(D|d)(E|e)(R|r)
PAGE = (P|p)(A|a)(G|g)(E|e)
READ = (R|r)(E|e)(A|a)(D|d)
SET = (S|s)(E|e)(T|t)
TRANSACTION_START = (T|t)(R|r)(A|a)(N|n)(S|s)(A|a)(C|c)(T|t)(I|i)(O|o)(N|n)_(S|s)(T|t)(A|a)(R|r)(T|t)
TRANSACTION_COMMIT = (T|t)(R|r)(A|a)(N|n)(S|s)(A|a)(C|c)(T|t)(I|i)(O|o)(N|n)_(C|c)(O|o)(M|m)(M|m)(I|i)(T|t)
TRANSACTION_ROLLBACK = (T|t)(R|r)(A|a)(N|n)(S|s)(A|a)(C|c)(T|t)(I|i)(O|o)(N|n)_(R|r)(O|o)(L|l)(L|l)(B|b)(A|a)(C|c)(K|k)
WHERE = (W|w)(H|h)(E|e)(R|r)(E|e)
WRITE = (W|w)(R|r)(I|i)(T|t)(E|e)
STATELESS = (S|s)(T|t)(A|a)(T|t)(E|e)(L|l)(E|e)(S|s)(S|s)
NO_FEEDBACK = (N|n)(O|o)_(F|f)(E|e)(E|e)(D|d)(B|b)(A|a)(C|c)(K|k)

TEXT = '(\\'|[^'])*'
HEX = 0x([0-9a-zA-Z]*)
ATOM = \$([a-zA-Z][a-zA-Z0-9_\-]*)
MACROS = \$\$([a-zA-Z][a-zA-Z0-9_\-]*)
FIELD = ([a-zA-Z0-9_\-.]*)
COMMENT_MULTILINE = (/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(--.*)
WHITESPACE = ([\000-\s]*)
INTNUM   = (\-*[0-9]+)
FLOATDEC = (\-*([0-9]+)?\.[0-9]+)
FLOATSCI = (\-*([0-9]+)?(\.)?[0-9]+(E|e)(\+|\-)?[0-9]+)
EQ          = (=)
EQS         = (:=)
GTS         = (:>)
LTS         = (:<)
GTES        = (:>=)
LTES        = (:=<)
NES         = (:<>)
LIKE        = (L|l)(I|i)(K|k)(E|e)
LIKES       = (:(L|l)(I|i)(K|k)(E|e))
OPEN        = \(
CLOSE       =\)
COMMA       = (,)
SEMICOLON   = (;)
COLON       = (:)

Rules.
{AND} : {token, {'AND', TokenLine}}.
{ANDNOT} : {token, {'ANDNOT', TokenLine}}.
{AS} : {token, {'AS', TokenLine}}.
{ASC} : {token, {'ASC', TokenLine}}.
{MACROS} : {token, {macros,TokenLine,to_macros(TokenChars)}}.
{ATOM} : {token, {atom,TokenLine,to_atom(TokenChars)}}.
{BY} : {token, {by, TokenLine}}.
{DELETE} : {token, {delete, TokenLine}}.
{DESC} : {token, {'DESC', TokenLine}}.
{INSERT} : {token, {insert, TokenLine}}.
{GET} : {token, {get, TokenLine}}.
{SUBSCRIBE} : {token, {subscribe, TokenLine}}.
{GROUP} : {token, {group, TokenLine}}.
{OR} : {token, {'OR', TokenLine}}.
{ORDER} : {token, {order, TokenLine}}.
{PAGE} : {token, {page, TokenLine}}.
{READ} : {token, {read, TokenLine}}.
{SET} : {token, {set, TokenLine}}.
{TRANSACTION_START} : {token, {transaction_start, TokenLine}}.
{TRANSACTION_COMMIT} : {token, {transaction_commit, TokenLine}}.
{TRANSACTION_ROLLBACK} : {token, {transaction_rollback, TokenLine}}.
{WHERE} : {token, {where, TokenLine}}.
{WRITE} : {token, {write, TokenLine}}.
{STATELESS} : {token, {stateless, TokenLine}}.
{NO_FEEDBACK} : {token, {no_feedback, TokenLine}}.
{HEX} : {token, {integer, TokenLine, to_hex(TokenChars)}}.
{INTNUM}      : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
{FLOATDEC}    : {token, {float, TokenLine, list_to_float(TokenChars)}}.
{FLOATSCI}    : {token, {float, TokenLine, list_to_float(TokenChars)}}.
{EQ}          : {token, {'=', TokenLine}}.
{EQS}         : {token, {':=',TokenLine}}.
{GTS}         : {token, {':>',TokenLine}}.
{LTS}         : {token, {':<',TokenLine}}.
{GTES}        : {token, {':>=',TokenLine}}.
{LTES}        : {token, {':=<',TokenLine}}.
{NES}         : {token, {':<>',TokenLine}}.
{LIKE}        : {token, {'LIKE',TokenLine}}.
{LOCK}        : {token, {lock,TokenLine}}.
{LIKES}       : {token, {':LIKE',TokenLine}}.
{OPEN}        : {token, {'(',TokenLine}}.
{CLOSE}       : {token, {')',TokenLine}}.
{TEXT}        : {token, {text,TokenLine,to_text(TokenChars)}}.
{COMMA} : {token, {',', TokenLine}}.
{SEMICOLON} : {token, {';', TokenLine}}.
{COLON} : {token, {':', TokenLine}}.
{COMMENT_MULTILINE} : skip_token.
{WHITESPACE} : skip_token.
{FIELD} : {token, {field,TokenLine,list_to_binary(TokenChars)}}.
\z : {end_token, {'$end'}}.
.  : error(iolist_to_binary(io_lib:format("Unexpected token '~s'.", [TokenChars]))).

Erlang code.

-export([
    to_hex/1,
    to_atom/1,
    to_macros/1
]).

to_hex([$0,$x|Hex]) ->
    list_to_integer(Hex,16).
to_text([$'|Literal]) ->
    Text=filter_escapes(unicode:characters_to_list(lists:droplast(Literal)),[]),
    unicode:characters_to_binary(Text).
to_atom([$$|Literal]) ->
    list_to_atom(Literal).

to_macros([$$,$$|Literal]) ->
    list_to_atom(string:lowercase(Literal)).

filter_escapes([$\\,$'|Rest], Acc) ->
    filter_escapes(Rest,[$\'|Acc]);
filter_escapes([C|Rest], Acc) ->
    filter_escapes(Rest,[C|Acc]);
filter_escapes([], Acc) ->lists:reverse(Acc).

