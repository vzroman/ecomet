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

Nonterminals

StatementList
Statement
Get
Subscribe
Set
Insert
Delete
GetFieldList
GetField
SetFieldList
SetField
SetValue
FieldName
Function
ConditionList
Condition
Operator
Constant
Variable
ParamList
Param
SubParamList
SubParam
OrderByList
OrderBy
OrderDirection
GroupByList
GroupBy
Lock
.

Terminals
'AND'
'ANDNOT'
'AS'
'ASC'
atom
by
delete
'DESC'
insert
get
subscribe
group
'OR'
order
page
read
set
transaction_start
transaction_commit
transaction_rollback
where
write
stateless
no_feedback
integer
float
'='
':='
':>'
':<'
':>='
':=<'
':<>'
'LIKE'
':LIKE'
lock
'('
')'
text
','
field
';'
':'
.

Rootsymbol StatementList.

StatementList-> Statement: ['$1'].
StatementList-> Statement ';' StatementList : ['$1'|'$3'].


Statement -> Get : '$1'.
Statement -> Subscribe : '$1'.
Statement -> Set : '$1'.
Statement -> Insert : '$1'.
Statement -> Delete : '$1'.
Statement -> transaction_start : transaction_start.
Statement -> transaction_commit : transaction_commit.
Statement -> transaction_rollback : transaction_rollback.

Get -> get GetFieldList where Condition ParamList: {get,'$2','$4','$5'}.
Get -> get GetFieldList where Condition: {get,'$2','$4',[]}.

Subscribe -> subscribe text get GetFieldList where Condition SubParamList: {subscribe,get_token('$2'),'$4','$6','$7'}.
Subscribe -> subscribe text get GetFieldList where Condition: {subscribe,get_token('$2'),'$4','$6',[]}.

Set-> set SetFieldList where Condition Lock: {set,'$2','$4',['$5']}.
Set-> set SetFieldList where Condition: {set,'$2','$4',[]}.

Insert -> insert SetFieldList : { insert, '$2' }.

Delete -> delete where Condition Lock : { delete, '$3', ['$4']}.
Delete -> delete where Condition : { delete, '$3', []}.

GetFieldList -> GetField: ['$1'].
GetFieldList -> GetField ',' GetFieldList: ['$1'|'$3'].

GetField -> text 'AS' text : {get_token('$3'),get_token('$1')}.
GetField -> Variable 'AS' text : {get_token('$3'), '$1' }.
GetField -> text : get_token('$1').
GetField -> Variable : '$1'.

SetFieldList -> SetField: ['$1'].
SetFieldList -> SetField ',' SetFieldList: ['$1'|'$3'].

SetField-> field '=' Variable : { get_token('$1'), '$3' }.
SetField-> text '=' Variable : { get_token('$1'), '$3' }.

Variable -> Constant : '$1'.
Variable -> field : get_token('$1').
Variable -> Function '(' VariableList ')' : compile('$1','$3').

Constant -> integer : get_token('$1').
Constant -> float : get_token('$1').
Constant -> text : get_token('$1').
Constant -> atom : get_token('$1').
Constant -> Function '(' ConstantList ')' : execute('$1','$3').

VariableList -> Variable : ['$1'].
VariableList -> Variable ',' VariableList : ['$1'|'$3'].

Function -> atom : { ecomet_query ,get_token('$1') }.
Function -> atom ':' atom : { get_token('$1') ,get_token('$3') }.

ConditionList -> Condition: ['$1'].
ConditionList -> Condition ',' ConditionList: ['$1'|'$3'].

Condition -> field Operator Constant : { get_token('$1'), '$2', '$3' }.
Condition -> text Operator Constant : { get_token('$1'), '$2', '$3' }.
Condition -> 'AND' '(' ConditionList ')' : { 'AND', '$3' }.
Condition -> 'OR' '(' ConditionList ')' : { 'OR', '$3' }.
Condition -> 'ANDNOT' '(' Condition ',' Condition ')' : { 'ANDNOT', '$3', '$5' }.

Operator -> '=' : '='.
Operator -> ':=' : ':='.
Operator -> ':>' : ':>'.
Operator -> ':<' : ':<'.
Operator -> ':>=' : ':>='.
Operator -> ':=<' : ':=<'.
Operator -> ':<>' : ':<>'.
Operator -> 'LIKE' : 'LIKE'.
Operator -> ':LIKE' : ':LIKE'.

ParamList-> Param ParamList: ['$1'|'$2'].
ParamList-> Param: ['$1'].

Param-> order by OrderByList : {order,'$3'}.
Param-> group by GroupByList: {group,'$3'}.
Param-> page integer ':' integer: {page,{ get_token('$2'), get_token('$4') }}.
Param-> Lock : '$1'.

SubParamList-> SubParam SubParamList: ['$1'|'$2'].
SubParamList-> SubParam: ['$1'].

SubParam-> stateless : {stateless,true}.
SubParam-> no_feedback : {no_feedback,true}.

Lock -> lock read : { lock , read }.
Lock -> lock write : { lock , write }.

OrderByList -> OrderBy: ['$1'].
OrderByList -> OrderBy ',' OrderByList: ['$1'|'$3'].

OrderBy -> field OrderDirection: {get_token('$1'),'$2'}.
OrderBy -> text OrderDirection: {get_token('$1'),'$2'}.
OrderBy -> integer OrderDirection: { get_token('$1'), '$2'}.
OrderBy -> field : {get_token('$1'),'ASC'}.
OrderBy -> text : {get_token('$1'),'ASC'}.
OrderBy -> integer: { get_token('$1'), 'ASC'}.

OrderDirection -> 'ASC' : 'ASC'.
OrderDirection -> 'DESC' : 'DESC'.

GroupByList -> GroupBy: ['$1'].
GroupByList -> GroupBy ',' GroupByList: ['$1'|'$3'].

GroupBy -> field : get_token('$1').
GroupBy -> text : get_token('$1').
GroupBy -> integer : get_token('$1').

Erlang code.

get_token({Token, _Line})->Token;
get_token({_Token, _Line, Value}) -> Value.

get_function({_Token,_Line,Function})->
  case erlang:function_exported(ecomet_query,Function,1) of
    true->fun ecomet_query:Function/1;
    _->erlang:error({invalid_function,Function})
  end.
run_macros(Macros,ArgumentList)->
  ecomet_query:macros(Macros,ArgumentList).


% leex:file("ecomet_ql_lexer.xrl",[{scannerfile,"../src/ecomet_ql_parser.erl"}])
% c("ecomet_ql_lexer.erl")

% yecc:file("ecomet_ql_parser.yrl",[{parserfile,"../src/ecomet_ql_parser.erl"}])
% c("ecomet_ql_parser.erl",[{i,"/media/data/PROJECTS/ecomet/apps/ecomet/include"}])
