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

AND         = (A|a)(N|n)(D|d)
ANDNOT      = (A|a)(N|n)(D|d)(N|n)(O|o)(T|t)
AS          = (A|a)(S|s)
ASC         = (A|a)(S|s)(C|c)
BY          = (B|b)(Y|y)
DELETE      = (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
DESC        = (D|d)(E|e)(S|s)(C|c)
GROUP       = (G|g)(R|r)(O|o)(U|u)(P|p)
GET         = (G|g)(E|e)(T|t)
FROM        = (F|f)(R|r)(O|o)(M|m)
SUBSCRIBE   = (S|s)(U|u)(B|b)(S|s)(C|c)(R|r)(I|i)(B|b)(E|e)
UNSUBSCRIBE = (U|u)(N|n)(S|s)(U|u)(B|b)(S|s)(C|c)(R|r)(I|i)(B|b)(E|e)
INSERT      = (I|i)(N|n)(S|s)(E|e)(R|r)(T|t)
UPDATE      = (U|u)(P|p)(D|d)(A|a)(T|t)(E|e)
LOCK        = (L|l)(O|o)(C|c)(K|k)
OR          = (O|o)(R|r)
ORDER       = (O|o)(R|r)(D|d)(E|e)(R|r)
PAGE        = (P|p)(A|a)(G|g)(E|e)
READ        = (R|r)(E|e)(A|a)(D|d)
SET         = (S|s)(E|e)(T|t)
IN          = (I|i)(N|n)
TRANSACTION_START = (T|t)(R|r)(A|a)(N|n)(S|s)(A|a)(C|c)(T|t)(I|i)(O|o)(N|n)_(S|s)(T|t)(A|a)(R|r)(T|t)
TRANSACTION_COMMIT = (T|t)(R|r)(A|a)(N|n)(S|s)(A|a)(C|c)(T|t)(I|i)(O|o)(N|n)_(C|c)(O|o)(M|m)(M|m)(I|i)(T|t)
TRANSACTION_ROLLBACK = (T|t)(R|r)(A|a)(N|n)(S|s)(A|a)(C|c)(T|t)(I|i)(O|o)(N|n)_(R|r)(O|o)(L|l)(L|l)(B|b)(A|a)(C|c)(K|k)
WHERE       = (W|w)(H|h)(E|e)(R|r)(E|e)
WRITE       = (W|w)(R|r)(I|i)(T|t)(E|e)
STATELESS   = (S|s)(T|t)(A|a)(T|t)(E|e)(L|l)(E|e)(S|s)(S|s)
NO_FEEDBACK = (N|n)(O|o)_(F|f)(E|e)(E|e)(D|d)(B|b)(A|a)(C|c)(K|k)
FORMAT      = (F|f)(O|o)(R|r)(M|m)(A|a)(T|t)

TEXT        = '(\\'|[^'])*'
HEX         = 0x([0-9a-zA-Z]*)
COMMENT_MULTILINE = (/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(--.*)
WHITESPACE  = ([\000-\s]*)
INTNUM      = (\-?[0-9]+)
FLOATDEC    = (\-?([0-9]+)?\.[0-9]+)
FLOATSCI    = (\-?([0-9]+)?(\.)?[0-9]+(E|e)(\+|\-)?[0-9]+)
FIELD       = (\.?([a-zA-Z0-9_-]+)*)
ATOM        = ([a-zA-Z][a-zA-Z0-9_\-]*)
S           = (\$)
ALL         = (\*)
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
CLOSE       = \)
LIST_OPEN   = \[
LIST_CLOSE  = \]
COMMA       = (,)
SEMICOLON   = (;)
COLON       = (:)

Rules.
{AND}         : {token, {'AND', TokenLine}}.
{ANDNOT}      : {token, {'ANDNOT', TokenLine}}.
{AS}          : {token, {'AS', TokenLine}}.
{ASC}         : {token, {'ASC', TokenLine}}.
{BY}          : {token, {by, TokenLine}}.
{DELETE}      : {token, {delete, TokenLine}}.
{DESC}        : {token, {'DESC', TokenLine}}.
{INSERT}      : {token, {insert, TokenLine}}.
{UPDATE}      : {token, {update, TokenLine}}.
{GET}         : {token, {get, TokenLine}}.
{FROM}        : {token, {from, TokenLine}}.
{SUBSCRIBE}   : {token, {subscribe, TokenLine}}.
{UNSUBSCRIBE} : {token, {unsubscribe, TokenLine}}.
{GROUP}       : {token, {group, TokenLine}}.
{OR}          : {token, {'OR', TokenLine}}.
{ORDER}       : {token, {order, TokenLine}}.
{PAGE}        : {token, {page, TokenLine}}.
{READ}        : {token, {read, TokenLine}}.
{SET}         : {token, {set, TokenLine}}.
{IN}          : {token, {in, TokenLine}}.
{TRANSACTION_START} : {token, {transaction_start, TokenLine}}.
{TRANSACTION_COMMIT} : {token, {transaction_commit, TokenLine}}.
{TRANSACTION_ROLLBACK} : {token, {transaction_rollback, TokenLine}}.
{WHERE}       : {token, {where, TokenLine}}.
{WRITE}       : {token, {write, TokenLine}}.
{STATELESS}   : {token, {stateless, TokenLine}}.
{NO_FEEDBACK} : {token, {no_feedback, TokenLine}}.
{FORMAT}      : {token, {format, TokenLine}}.
{S}           : {token, {'$', TokenLine}}.
{ALL}         : {token, {'*', TokenLine}}.
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
{LIST_OPEN}   : {token, {'[',TokenLine}}.
{LIST_CLOSE}  : {token, {']',TokenLine}}.
{COMMA}       : {token, {',', TokenLine}}.
{SEMICOLON}   : {token, {';', TokenLine}}.
{COLON}       : {token, {':', TokenLine}}.
{HEX}         : {token, {integer, TokenLine, to_hex(TokenChars)}}.
{INTNUM}      : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
{FLOATDEC}    : {token, {float, TokenLine, list_to_float(TokenChars)}}.
{FLOATSCI}    : {token, {float, TokenLine, list_to_float(TokenChars)}}.
{FIELD}       : {token, {field,TokenLine,list_to_binary(TokenChars)}}.
{ATOM}        : {token, {atom,TokenLine,list_to_atom(TokenChars)}}.
{TEXT}        : {token, {text,TokenLine,to_text(TokenChars)}}.
{COMMENT_MULTILINE} : skip_token.
{WHITESPACE}  : skip_token.
\z            : {end_token, {'$end'}}.
.             : error(iolist_to_binary(io_lib:format("Unexpected token '~s'.", [TokenChars]))).

Erlang code.

-export([
    to_hex/1
]).

to_hex([$0,$x|Hex]) ->
    list_to_integer(Hex,16).
to_text([$'|Literal]) ->
    Text=filter_escapes(unicode:characters_to_list(lists:droplast(Literal)),[]),
    unicode:characters_to_binary(Text).

filter_escapes([$\\,$'|Rest], Acc) ->
    filter_escapes(Rest,[$\'|Acc]);
filter_escapes([C|Rest], Acc) ->
    filter_escapes(Rest,[C|Acc]);
filter_escapes([], Acc) ->lists:reverse(Acc).

