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
Unsubscribe
Set
Insert
Delete
Databases
Database
GetFieldList
GetField
SetFieldList
SetField
Function
ConditionList
Condition
Operator
Atom
Field
FieldValue
ConstTerm
Constant
ConstantList
Variable
VariableList
Date
GetParamList
GetParam
SubParamList
SubParam
SetParamList
SetParam
InsertParamList
InsertParam
OrderByList
OrderBy
OrderDirection
GroupByList
GroupBy
Lock
Format
.

Terminals
'AND'
'ANDNOT'
'AS'
'ASC'
by
delete
'DESC'
insert
update
get
subscribe
unsubscribe
group
'OR'
order
page
read
set
from
in
transaction_start
transaction_commit
transaction_rollback
where
write
stateless
no_feedback
format
field
atom
integer
float
'$'
'*'
'='
'>'
%'<'
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
'['
']'
text
','
';'
':'
.

Rootsymbol StatementList.

StatementList-> Statement: ['$1'].
StatementList-> Statement ';' StatementList : ['$1'|'$3'].


Statement -> Get : '$1'.
Statement -> Subscribe : '$1'.
Statement -> Unsubscribe : '$1'.
Statement -> Set : '$1'.
Statement -> Insert : '$1'.
Statement -> Delete : '$1'.
Statement -> transaction_start : transaction_start.
Statement -> transaction_commit : transaction_commit.
Statement -> transaction_rollback : transaction_rollback.

Get -> get GetFieldList from Databases where Condition GetParamList: {get,'$2','$4','$6',maps:from_list('$7')}.
Get -> get GetFieldList from Databases where Condition: {get,'$2','$4','$6',#{}}.

Subscribe -> subscribe text get GetFieldList from Databases where Condition SubParamList: {subscribe,get_token('$2'),'$4','$6','$8',maps:from_list('$9')}.
Subscribe -> subscribe text get GetFieldList from Databases where Condition: {subscribe,get_token('$2'),'$4','$6','$8',#{}}.

Unsubscribe -> unsubscribe text : { unsubscribe, get_token('$2') }.

Set-> set SetFieldList in Databases where Condition SetParamList : {set,maps:from_list('$2'),'$4','$6',maps:from_list('$7')}.
Set-> set SetFieldList in Databases where Condition: {set,maps:from_list('$2'),'$4','$6',#{}}.

Insert -> insert SetFieldList InsertParamList : { insert, maps:from_list('$2') ,maps:from_list('$3') }.
Insert -> insert SetFieldList : { insert, maps:from_list('$2') ,#{} }.

Delete -> delete from Databases where Condition Lock : { delete, '$3', '$5', maps:from_list(['$6'])}.
Delete -> delete from Databases where Condition : { delete, '$3','$5', #{} }.

Databases -> '*' : '*'.
Databases -> Database : ['$1'].
Databases -> Database ',' Databases: ['$1'|'$3'].

Database -> Atom : '$1'.
Database -> '$' Atom : atom_to_binary('$2', utf8 ).
Database -> text : binary_to_atom(get_token('$1'),utf8).

GetFieldList -> '*': ['*'].
GetFieldList -> GetField: ['$1'].
GetFieldList -> GetField ',' GetFieldList: ['$1'|'$3'].

GetField -> FieldValue 'AS' text : { get_token('$3'), field_value('$1') }.
GetField -> FieldValue : case {field_name('$1') ,field_value('$1')} of
                            {F,F} -> F;
                            {Name,Value} -> {Name,Value}
                         end.
GetField -> Function '(' ')' 'AS' text : { get_token('$5'), compile('$1',[]) }.
GetField -> Function '(' ')' : compile('$1',[]).
GetField -> Function '(' VariableList ')' 'AS' text : { get_token('$6'), compile('$1','$3') }.
GetField -> Function '(' VariableList ')' : compile('$1','$3').

FieldValue -> Field '>' FieldValue : ['$1'|'$3'].
FieldValue -> Field : ['$1'].

SetFieldList -> SetField: ['$1'].
SetFieldList -> SetField ',' SetFieldList: ['$1'|'$3'].

SetField-> Field '=' Variable : { '$1', '$3' }.

Atom -> field : binary_to_atom(get_token('$1'),utf8).
Atom -> atom : get_token('$1').

Field -> field : get_token('$1').
Field -> text : get_token('$1').

ConstTerm-> integer : get_token('$1').
ConstTerm -> float : get_token('$1').
ConstTerm -> text : get_token('$1').
ConstTerm -> Atom : '$1'.

ConstantList -> Constant : ['$1'].
ConstantList -> Constant ',' ConstantList : ['$1'|'$3'].

Constant -> ConstTerm : '$1'.
Constant -> '[' ConstantList ']' : ['$2'].
Constant -> Function '(' ConstantList ')' : compile('$1','$3').

VariableList -> Variable : ['$1'].
VariableList -> Variable ',' VariableList : ['$1'|'$3'].

Variable -> ConstTerm : '$1'.
Variable -> '$' FieldValue : get_field(field_value('$2')).
Variable -> '[' VariableList ']': compile('$2').
Variable -> Function '(' ')' : compile('$1',[]).
Variable -> Function '(' VariableList ')' : compile('$1','$3').

Function -> '$' Atom : { ecomet_ql_util ,'$2' }.
Function -> '$' Atom ':' Atom : { '$2' ,'$4' }.

ConditionList -> Condition: ['$1'].
ConditionList -> Condition ',' ConditionList: ['$1'|'$3'].

Condition -> Field Operator Constant : { '$1', '$2', '$3' }.
Condition -> Field '[' Date ':' Date ']' : { '$1', 'DATETIME', ['$3','$5'] }.
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

Date -> integer : get_token('$1').
Date -> text : ecomet_lib:parse_dt(get_token('$1')).

GetParamList-> GetParam GetParamList: ['$1'|'$2'].
GetParamList-> GetParam: ['$1'].

GetParam-> order by OrderByList : {order,'$3'}.
GetParam-> group by GroupByList: {group,'$3'}.
GetParam-> page integer ':' integer: {page,{ get_token('$2'), get_token('$4') }}.
GetParam-> Lock : '$1'.
GetParam-> Format : '$1'.

SubParamList-> SubParam SubParamList: ['$1'|'$2'].
SubParamList-> SubParam: ['$1'].

SubParam-> stateless : {stateless,true}.
SubParam-> no_feedback : {no_feedback,true}.
SubParam-> Format : '$1'.

SetParamList-> SetParam SetParamList: ['$1'|'$2'].
SetParamList-> SetParam: ['$1'].

SetParam-> Lock : '$1'.
SetParam-> Format : '$1'.

InsertParamList-> InsertParam InsertParamList: ['$1'|'$2'].
InsertParamList-> InsertParam: ['$1'].

InsertParam-> update : { update, true }.
InsertParam-> Lock : '$1'.
InsertParam-> Format : '$1'.

Lock -> lock read : { lock , read }.
Lock -> lock write : { lock , write }.

OrderByList -> OrderBy: ['$1'].
OrderByList -> OrderBy ',' OrderByList: ['$1'|'$3'].

OrderBy -> Field OrderDirection: {'$1','$2'}.
OrderBy -> integer OrderDirection: { get_token('$1'), '$2'}.
OrderBy -> Field : {'$1','ASC'}.
OrderBy -> integer: { get_token('$1'), 'ASC'}.

OrderDirection -> 'ASC' : 'ASC'.
OrderDirection -> 'DESC' : 'DESC'.

GroupByList -> GroupBy: ['$1'].
GroupByList -> GroupBy ',' GroupByList: ['$1'|'$3'].

GroupBy -> Field : '$1'.
GroupBy -> integer : get_token('$1').

Format -> format '$' Atom ':' Atom : { format, fun '$3':'$5'/2 }.
Format -> format '$' Atom : { format, fun ecomet_types:'$3'/2 }.

Erlang code.

get_token({Token, _Line})->Token;
get_token({_Token, _Line, Value}) -> Value.

get_field({Fun,Args}) when is_function(Fun)->
    {Fun,Args};
get_field(Field)->
    {fun erlang:hd/1,[Field]}.

field_name([Field])->
    Field;
field_name([Field|Rest])->
    <<Field/binary,">",(field_name(Rest))/binary>>.


field_value([Field])->
    Field;
field_value([Link|FieldChain])->
    {fun([OID])-> ecomet_ql_util:join([OID|FieldChain]) end,[Link]}.

compile(VarList)->
    ecomet_query:compile_function(VarList).
compile({Module,Function},Args)->
    ecomet_query:compile_function(Module,Function,Args).


% leex:file("ecomet_ql_lexer.xrl",[{scannerfile,"../src/ecomet_ql_parser.erl"}])
% c("ecomet_ql_lexer.erl")

% yecc:file("ecomet_ql_parser.yrl",[{parserfile,"../src/ecomet_ql_parser.erl"}])
% c("ecomet_ql_parser.erl",[{i,"/media/data/PROJECTS/ecomet/apps/ecomet/include"}])
