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

-module(ecomet_resultset).

-include("ecomet.hrl").
-include("ecomet_schema.hrl").

%%====================================================================
%% API functions
%%====================================================================
-export([
	prepare/1,
	normalize/1,
	new/0,
	new_branch/0,
	execute/5,
	no_transaction/5,
	count/1,
	foldr/3,
	foldr/4,
	foldl/3,
	foldl/4,
	fold/5
]).

%%====================================================================
%% Module remote call API
%%====================================================================
-export([
	do_single_node_transaction/6,
	db_transaction_request/6,
	db_no_transaction/5
]).
%%====================================================================
%% Subscriptions API
%%====================================================================
-export([
	subscription_prepare/1,
	direct/2
]).

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([
	define_patterns/1,
	build_conditions/1,
	has_direct/1,
	optimize/1,
	search_patterns/3,
	seacrh_idhs/3
]).

-endif.

-define(TIMEOUT,30000).

%%=====================================================================
%%	query compilation
%%=====================================================================
%% For search process we need info about storages for indexes. It's contained in patterns. Next variants are possible:
%% 1. Patterns are defined by query conditions. Example:
%% 		{'AND',[
%% 			{<<".pattern">>, '=', PatternID},
%% 			{<<"my_field">>, '=', Value }
%% 		]}
%%		Search is limited by only pattern PatternID. So, we can define storage for this field and perform search only there
%% 2. Patterns are defined while search. Example query:
%%		{<<"my_field">>, '=', Value}
%%		It contains no info about patterns. On search we will perform first extra step - search for patterns
%% 		through all storages (index include top layer on pattern).
prepare(Conditions)->
	Patterned=define_patterns(Conditions),
	Built=build_conditions(Patterned),
	case has_direct(Built) of
		true->
			Normal=normalize(Built),
			optimize(Normal);
		_->
			Built
	end.

%%=====================================================================
%%	Subscriptions API
%%=====================================================================
subscription_prepare(Conditions)->

	% Standard preparation procedure
	Patterned=define_patterns(Conditions),
	Built=build_conditions(Patterned),

	% Normalization
	{'OR',Normal,_}=normalize(Built),

	% Extract tags
	[{
			[ Tag || {'TAG',Tag,_} <- AND ], 				% 1. And tags
			[ Tag || {'TAG',Tag,_} <- ANDNOT ]			% 2. Andnot tags
		}
	|| {'NORM',{ {{'AND',AND,_},{'OR',ANDNOT,_}}, _Direct }, _} <- Normal ].

direct({'AND',Conditions}, Fields)->
	direct_and( Conditions, Fields );
direct({'OR',Conditions}, Fields)->
	direct_or( Conditions, Fields );
direct({'ANDNOT',And,Not}, Fields)->
	case direct(And, Fields) of
		false-> false;
		true->
			case direct(Not, Fields) of
				true-> false;
				false->true
			end
	end;
direct({Field,Oper,Value}, Fields)->
	FieldValue = maps:get(Field,Fields,none),
	if
		not is_list(FieldValue)->
			direct_compare(Oper,Value,FieldValue);
		true->
			Object = maps:get(object, Fields),
			case ecomet_object:field_type(Object, Field) of
				{ok,{list,_}}->
					direct_compare_list(Oper, Value, FieldValue);
				_->
					direct_compare(Oper, Value, FieldValue)
			end
	end.

direct_and([Cond|Rest],Fields)->
	case direct(Cond, Fields) of
		false -> false;
		true -> direct_and(Rest,Fields)
	end;
direct_and([],_Fields)->
	true.

direct_or([Cond|Rest],Fields)->
	case direct(Cond, Fields) of
		true -> true;
		false-> direct_or(Rest, Fields)
	end;
direct_or([],_Fields)->
	false.

%%=====================================================================
%%	Compilation utilities
%%=====================================================================
%%
%% Define conditions on patterns
%%
define_patterns({<<".pattern">>,':=',PatternID})->
	{'LEAF',{'=',<<".pattern">>,PatternID}};
define_patterns({<<".pattern">>,'=',PatternID})->
	Patterns = [PatternID|ecomet_pattern:get_children_recursive(PatternID)],
	define_patterns({'OR',[{<<".pattern">>,':=',P}||P<-Patterns]});
% Leaf condition
define_patterns({Field,Oper,Value}) when is_binary(Field)->
	{'LEAF',{Oper,Field,Value}};
% Intersect or union patterns
define_patterns({Oper,List}) when (Oper=='AND') or (Oper=='OR')->
	{ Oper, [ define_patterns(C) || C <-List ] };
define_patterns({'ANDNOT',Condition1,Condition2})->
	C1=define_patterns(Condition1),
	C2=define_patterns(Condition2),
	{'ANDNOT',{C1,C2}}.

%%
%% Building conditions
%%
build_conditions({'LEAF',{Oper,Field,Value}})
	when (Oper=='=');(Oper=='LIKE');(Oper=='DATETIME')->
	build_leaf({Oper,Field,Value});
build_conditions({'LEAF',Condition})->
	case element(1,Condition) of
		':='->ok;
		':>'->ok;
		':>='->ok;
		':<'->ok;
		':=<'->ok;
		':LIKE'->ok;
		':<>'->ok;
		Oper->?ERROR({invalid_operation,Oper})
	end,
	{'DIRECT',Condition,'UNDEFINED'};
build_conditions({Oper,Conditions}) when (Oper=='AND') or (Oper=='OR')->
	{Oper, [ build_conditions(C) || C<-Conditions], 'UNDEFINED'};
build_conditions({'ANDNOT',{Condition1,Condition2}})->
	C1=build_conditions(Condition1),
	C2=build_conditions(Condition2),
	{'ANDNOT',{C1,C2},'UNDEFINED'}.

%%
%% Check if conditions have DIRECT leafs
%%
has_direct({'DIRECT',_,_})->
	true;
has_direct({ Oper , Conditions ,_}) when (Oper=='AND') or (Oper=='OR')->
	has_direct(Conditions);
has_direct({ 'ANDNOT' , {C1, C2} ,_})->
	has_direct(C1) or has_direct(C2);
has_direct([C|Tail])->
	has_direct(C) or has_direct(Tail);
has_direct(_Other)->
	false.

%%
%%	Build helpers
%%
%------------Service fields-----------------------------------
build_leaf({'=',<<".path">>,<<"/root">>})->
	{'AND',[
		{'TAG',{<<".name">>,<<"root">>,'simple'},'UNDEFINED'},
		{'TAG',{<<".folder">>, {?FOLDER_PATTERN, 0 } ,'simple'},'UNDEFINED'}
	],'UNDEFINED'};
build_leaf({'=',<<".path">>,Value})->
	case re:run(Value,"(?<F>.*)/(?<N>[^/]+)",[{capture,['F','N'],binary}]) of
		{match,[Folder,Name]}->
			case ecomet_folder:path2oid(Folder) of
				{ok,FolderID}->
					{'AND',[
						{'TAG',{<<".name">>,Name,'simple'},'UNDEFINED'},
						{'TAG',{<<".folder">>,FolderID,'simple'},'UNDEFINED'}
					],'UNDEFINED'};
				_->
					?ERROR({invalid_path,Value})
			end;
		_->
			?ERROR({invalid_path,Value})
	end;
build_leaf({'LIKE',<<".path">>,Value})->
	Path = binary:split(Value, <<"/">>, [global]),
	Path1 = [ F || F <- Path , length( unicode:characters_to_list(F) ) > 0 ],
	path_recursive_search( Path1 );
build_leaf({'=',<<".oid">>,Value})->
	Object = ecomet_object:construct(Value),
	#{
		<<".name">>:=Name,
		<<".folder">>:=Folder
	} = ecomet:read_fields(Object,[<<".name">>,<<".folder">>]),

	{'AND',[
		{'TAG',{<<".name">>,Name,'simple'},'UNDEFINED'},
		{'TAG',{<<".folder">>,Folder,'simple'},'UNDEFINED'}
	],'UNDEFINED'};

%-----------------Simple index search-------------------------
build_leaf({'=',Field,Value})->
	{'TAG',{Field,Value,simple},'UNDEFINED'};
%----------------3gram index search---------------------------
build_leaf({'LIKE',Field,Value})->
	% We drop first and last 3grams, it's ^start, end$
	case ecomet_index:build_3gram([Value],[]) of
		% String is too short, use direct analog
		[]->{'DIRECT',{':LIKE',Field,Value},'UNDEFINED'};
		[_PhraseEnd|NGrams]->
			{'AND',[{'TAG',{Field,Gram,'3gram'},'UNDEFINED'}||Gram<-lists:droplast(NGrams)],'UNDEFINED'}
	end;
%-------------datetime index search---------------------------
build_leaf({'DATETIME',Field,[From,To]})->

	if
		From > To-> throw( {invalid_time_range, From, To} );
		true -> ok
	end,

	% Build an index on the time points
	FromIndex = ecomet_index:build_dt([From],[]),
	ToIndex = ecomet_index:build_dt([To],[]),

	% Building the query
	dt_query(Field,FromIndex,ToIndex).


bitmap_oper('AND',X1,X2)->
	case {X1,X2} of
		{'UNDEFINED',_}->X2;
		{_,'UNDEFINED'}->X1;
		_->bitmap_result(ecomet_bitmap:oper('AND',X1,X2))
	end;
bitmap_oper('OR',X1,X2)->
	case {X1,X2} of
		{'UNDEFINED',_}->'UNDEFINED';
		{_,'UNDEFINED'}->'UNDEFINED';
		_->bitmap_result(ecomet_bitmap:oper('OR',X1,X2))
	end;
bitmap_oper('ANDNOT',X1,X2)->
	case {X1,X2} of
		{'UNDEFINED',_}->'UNDEFINED';
		{_,'UNDEFINED'}->X1;
		_->bitmap_result(ecomet_bitmap:oper('ANDNOT',X1,X2))
	end.
bitmap_result(Bitmap)->
	Zip = ecomet_bitmap:zip(Bitmap),
	case ecomet_bitmap:is_empty(Zip) of
		false->Zip;
		true->none
	end.

%%
%%	Query normalization. Simplified example:
%%	Source query:
%%	{'AND',[
%%		{'OR',[
%%			{<<"field1">>,'=',Value1},
%%			{<<"field2">>,'=',Value1}
%% 		]},
%%		{'OR',[
%%			{<<"field1">>,'=',Value2},
%%			{<<"field2">>,'=',Value2}
%% 		]},
%%		{<<"field3">>,':>',Value3}
%% 	]}
%%	Result:
%%																					base conditions (indexed)											direct conditions
%%																					/														\													/						\
%%																				and													 andnot					  				and					 andnot
%%																				 |														 |											 |						 |
%%	{'OR',[																 |														 |											 |						 |
%%		{'NORM',{ [{<<"field1">>,'=',Value1}, {<<"field1">>,'=',Value2 }], [] }, { [{<<"field3">>,':>',Value3}], [] } },
%%		{'NORM',{ [{<<"field1">>,'=',Value1}, {<<"field2">>,'=',Value2 }], [] }, { [{<<"field3">>,':>',Value3}], [] } },
%%		{'NORM',{ [{<<"field2">>,'=',Value1}, {<<"field1">>,'=',Value2 }], [] }, { [{<<"field3">>,':>',Value3}], [] } },
%%		{'NORM',{ [{<<"field2">>,'=',Value1}, {<<"field2">>,'=',Value2 }], [] }, { [{<<"field3">>,':>',Value3}], [] } }
%%	]}
%% 	For each 'NORM' we define search limits by indexed conditions and after that we load each object and check it for direct conditions
normalize(Conditions)->
	norm_sort(norm(Conditions),element(3,Conditions)).
norm({'TAG',Condition,Config})->
	[[{1,{'TAG',Condition,Config}}]];
norm({'DIRECT',Condition,Config})->
	[[{1,{'DIRECT',Condition,Config}}]];
norm({'AND',ANDList,_})->
	case lists:foldl(fun(AND,AndAcc)->
		list_mult(AndAcc,norm(AND))
	end,start,ANDList) of
		start->[[]];
		Result->Result
	end;
norm({'OR',ORList,_})->
	lists:foldl(fun(OR,OrAcc)->
		list_add(OrAcc,norm(OR))
	end,[[]],ORList);
norm({'ANDNOT',{Condition1,Condition2},_})->
	list_andnot(norm(Condition1),norm(Condition2)).

list_mult(start,List2)->List2;
list_mult([[]],_List2)->[[]];
list_mult(_List1,[[]])->[[]];
list_mult(List1,List2)->
	lists:foldr(fun(L1,Result)->
		lists:foldr(fun(L2,ResultTags)->
			[L1++L2|ResultTags]
		end,Result,List2)
	end,[],List1).

list_add([[]],List2)->List2;
list_add(List1,[[]])->List1;
list_add(List1,List2)->
	List1++List2.

list_andnot(List1,List2)->
	list_add(
		list_mult(List1,list_not(list_filter(List2,1))),
		list_mult(List1,
			list_mult(
				list_filter(List2,1),
				list_not(list_filter(List2,-1))
			)
		)
	).

list_not(List)->
	case lists:foldr(fun(E1,Acc1)->
		list_mult(Acc1,lists:foldr(fun({Sign,Tag},Acc2)->
			[[{Sign*-1,Tag}]|Acc2]
		end,[],E1))
	end,start,List) of
		start->[[]];
		Result->Result
	end.

list_filter(List,Sign)->
	lists:foldr(fun(E1,Acc1)->
		case lists:foldr(fun({ESign,Tag},Acc2)->
			case ESign of
				Sign->[{ESign,Tag}|Acc2];
				_->Acc2
			end
		end,[],E1) of
			[]->Acc1;
			FilteredList->list_add(Acc1,[FilteredList])
		end
	end,[[]],List).

norm_sort(List,Config)->
	ORList=
	lists:foldr(fun(E1,Acc1)->
    {{TAdd,TDel},{DAdd,DDel}}=
		lists:foldr(fun({Sign,Tag},{{TagAdd,TagDel},{DirectAdd,DirectDel}})->
			case {element(1,Tag),Sign} of
				{'TAG',1}->{{[Tag|TagAdd],TagDel},{DirectAdd,DirectDel}};
				{'TAG',-1}->{{TagAdd,[Tag|TagDel]},{DirectAdd,DirectDel}};
				{'DIRECT',1}->{{TagAdd,TagDel},{[Tag|DirectAdd],DirectDel}};
				{'DIRECT',-1}->{{TagAdd,TagDel},{DirectAdd,[Tag|DirectDel]}}
			end
		end,{{[],[]},{[],[]}},E1),
		if TAdd==[]->?ERROR(no_base_conditions); true->ok end,
		[{'NORM',{{{'AND',TAdd,Config},{'OR',TDel,Config}},{DAdd,DDel}},Config}|Acc1]
	end,[],List),
	{'OR',ORList,Config}.

% Optimize normalized query. Try to factor out indexed ands andnots
optimize({'OR',[SingleNorm],Config})->{'OR',[SingleNorm],Config};
optimize({'OR',Normalized,Config})->
  {ListAND,ListANDNOT}=
	lists:foldl(fun({'NORM',{{{'AND',AND,_},{'OR',ANDNOT,_}},_Direct},_},{AccAND,AccANDNOT})->
    {[ordsets:from_list(AND)|AccAND],[ordsets:from_list(ANDNOT)|AccANDNOT]
    }
	end,{[],[]},Normalized),
  case ordsets:intersection(ListAND) of
    % No common tags, nothing to pick out
    []->{'OR',Normalized,Config};
    XAND->
      XANDNOT=ordsets:intersection(ListANDNOT),
      Cleared=
      lists:foldr(fun({'NORM',{{{'AND',AND,_},{'OR',ANDNOT,_}},Direct},_},Acc)->
				% Base AND condition list can not be empty, so we take at least one condition. It's exessive, TODO
				BaseAND=
				case ordsets:subtract(ordsets:from_list(AND),XAND) of
					[]->[lists:nth(1,AND)];
					ClearAND->ClearAND
				end,
        [{'NORM',{{{'AND',BaseAND,Config},{'OR',ordsets:subtract(ordsets:from_list(ANDNOT),XANDNOT),Config}},Direct},Config}|Acc]
      end,[],Normalized),
      case XANDNOT of
        []->{'AND',XAND++[{'OR',Cleared,Config}],Config};
        _->
          {'AND',[{'ANDNOT',{{'AND',XAND,Config},{'OR',XANDNOT,Config}},Config},{'OR',Cleared,Config}],Config}
      end
  end.

%%=====================================================================
%%	API helpers
%%=====================================================================
new()->[].
new_branch()->{none,maps:new()}.

%%=====================================================================
%%	QUERY EXECUTION
%%=====================================================================
execute(DBs,Conditions,Map,Reduce,Union)->
	case ecomet:is_transaction() of
		true->
			transaction(DBs, Conditions, Map, Reduce, Union);
		_->
			no_transaction(DBs, Conditions, Map, Reduce, Union)
	end.

%%---------------------------------------------------------------------
%%	NO TRANSACTION QUERY
%%---------------------------------------------------------------------
no_transaction([DB], Conditions, Map, Reduce, Union)->
	Context = ecomet_user:query_context(),
	case db_no_transaction(DB, Conditions, Map, Union, Context) of
		{error,_}->Reduce([]);
		Result-> Reduce([Result])
	end;
no_transaction(DBs, Conditions, Map, Reduce, Union)->
	Self = self(),
	Context = ecomet_user:query_context(),
	Master =
		spawn(fun()->
			MasterSelf = self(),
			Workers =
				[ begin
						 {W,_} = spawn_monitor(fun()->
							 MasterSelf ! {result, self(), db_no_transaction(DB, Conditions, Map, Union, Context)}
						 end),
					 {W,DB}
				 end || DB <- DBs],
			Results = no_transaction_wait( maps:from_list( Workers )),
			Self ! { results, self(), Results}
		end),
	receive
		{ results, Master, Results}->
			Reduce( order_DBs(DBs,Results) )
	end.

no_transaction_wait( Workers ) when map_size(Workers) > 0->
	receive
		{result, W, {error, _}}->
			no_transaction_wait(maps:remove(W, Workers));
		{result, W, Result}->
			case maps:take( W, Workers ) of
				{DB, RestWorkers}->
					[{DB, Result}| no_transaction_wait(RestWorkers)];
				_->
					no_transaction_wait( Workers )
			end;
		{'DOWN', _, process, W, _}->
			no_transaction_wait(maps:remove(W, Workers))
	end;
no_transaction_wait( _Workers )->
	[].

db_no_transaction(DB, Conditions, Map, Union, Context)->
	case ecomet_db:available_nodes( DB ) of
		[]->
			{error,not_available};
		Nodes->
			case lists:member(node(), Nodes) of
				true->
					ecomet_user:query_context( Context ),
					try execute_local(DB,Conditions,Map,Union)
					catch
						_:E->{error, E}
					end;
				_->
					case ecall:call_one(Nodes,?MODULE,?FUNCTION_NAME,[DB, Conditions, Map, Union, Context]) of
						{ok,{_Node,Result}}->
							Result;
						Error->
							Error
					end
			end
	end.

order_DBs([DB|Rest],Results)->
	case lists:keytake(DB, 1, Results) of
		{value,{_,DBResult},RestResults}->
			[DBResult | order_DBs(Rest, RestResults)];
		_->
			order_DBs(Rest, Results)
	end;
order_DBs([],_Results)->
	[].

%%---------------------------------------------------------------------
%%	TRANSACTIONAL QUERY
%%---------------------------------------------------------------------
transaction([DB|Rest] = DBs, Conditions, Map, Reduce, Union)->
	case x_nodes(Rest, ecomet_db:available_nodes(DB)) of
		[]->
			cross_nodes_transaction( DBs, Conditions, Map, Reduce, Union );
		Nodes->
			case lists:member(node(), Nodes) of
				true->
					local_transaction( DBs, Conditions, Map, Reduce, Union );
				_->
					single_node_transaction(Nodes, DBs, Conditions, Map, Reduce, Union)
			end
	end.

x_nodes(_DBs, [])->
	[];
x_nodes([DB|Rest],Nodes)->
	x_nodes(Rest, Nodes -- (Nodes -- ecomet_db:available_nodes(DB)));
x_nodes([], Nodes)->
	Nodes.

local_transaction( DBs, Conditions, Map, Reduce, Union )->
	Results = [ execute_local(DB,Conditions,Map,Union) || DB <- DBs ],
	Reduce( Results ).

single_node_transaction(Nodes, DBs, Conditions, Map, Reduce, Union)->
	Context = ecomet_user:query_context(),
	case ecall:call_one(Nodes, ?MODULE, do_single_node_transaction,[DBs, Conditions, Map, Reduce, Union, Context]) of
		{error,Error}->
			throw(Error);
		{ok,{_Node,Result}}->
			Result
	end.
do_single_node_transaction(DBs, Conditions, Map, Reduce, Union, Context)->
	ecomet_user:query_context( Context ),
	case ecomet:transaction(fun()->local_transaction(DBs, Conditions, Map, Reduce, Union) end) of
		{ok,Result}->
			Result;
		Error->
			Error
	end.

cross_nodes_transaction( DBs, Conditions, Map, Reduce, Union )->
	Self = self(),
	Context = ecomet_user:query_context(),
	Master =
		spawn(fun()->
			MasterSelf = self(),
			Workers =
				[ begin
						{W,_} = spawn_monitor(fun()->
							db_transaction(DB, Conditions, Map, Union, MasterSelf, Context)
						end),
						{W,DB}
					end || DB <- DBs],
			case transaction_wait( maps:from_list( Workers )) of
				{abort, Reason}->
					catch Self ! { abort, self(), Reason},
					exit(Reason);
				Results->
					catch Self ! { results, self(), Results}
			end
		end),

	receive
		{results, Master, Results}->
			Reduce( order_DBs( DBs, Results) );
		{abort, Master, Reason}->
			throw( Reason )
	end.

transaction_wait( Workers ) when map_size(Workers) > 0->
	receive
		{result, W, Result}->
			case maps:take( W, Workers ) of
				{DB, RestWorkers}->
					[{DB, Result}| transaction_wait(RestWorkers)];
				_->
					transaction_wait( Workers )
			end;
		{'DOWN', _, process, _, Reason}->
			{abort, Reason}
	end;
transaction_wait( _Workers )->
	[].

db_transaction(DB, Conditions, Map, Union, Master, Context)->
	erlang:monitor(process, Master),
	Nodes = ecomet_db:available_nodes(DB),
	case lists:member(node(), Nodes) of
		true->
			db_transaction_request(DB, Conditions, Map, Union, Master, Context);
		_->
			db_transaction(Nodes, DB, Conditions, Map, Union, Master, Context)
	end.

db_transaction([], DB, _Conditions, _Map, _Union, _Master, _Context)->
	exit({DB, not_available});
db_transaction(Nodes, DB, Conditions, Map, Union, Master, Context)->
	I = erlang:phash2(make_ref(),length(Nodes)),
	Node = lists:nth(I+1, Nodes),
	Worker = spawn(Node, ?MODULE, db_transaction_request, [DB, Conditions, Map, Union, _Master = self(), Context]),
	erlang:monitor(process, Worker),
	receive
		{result, Worker, Result}->
			catch Master ! {result, self(), Result},
			receive
				{'DOWN', _, process, Master, normal}->
					ok;
				{'DOWN', _, process, _, Reason}->
					exit(Reason)
			end;
		{'DOWN', _, process, Worker, _Reason}->
			db_transaction( Nodes--[Node], DB, Conditions, Map, Union, Master, Context );
		{'DOWN', _, process, Master, Reason}->
			exit(Reason)
	end.

db_transaction_request(DB, Conditions, Map, Union, Master, Context)->
	ecomet_user:query_context( Context ),
	case ecomet:transaction(fun()->
		Result = execute_local(DB,Conditions,Map,Union),
		catch Master ! {result, self(), Result},
		receive
			{'DOWN', _, process, Master, normal}->
				ok;
			{'DOWN', _, process, Master, Reason}->
				throw(Reason)
		end
	end) of
		{error,Reason}->
			exit( Reason );
		_->
			ok
	end.

%%=====================================================================
%%	SEARCH ENGINE
%%=====================================================================
%% Local search:
%% 1. Search patterns. Match all conditions against Pattern level index. This is optional step, we need it
%% 		only if patterns not explicitly defined in conditions.
%% 2. Search IDHIGHs. Match all conditions against IDHIGH level index.
%% 3. Search IDLOW. Match all conditions against IDLOW level index. Search only within defined on step 2 IDHIGHs
execute_local(DB,InConditions,Map,{Oper,RS})->

	% ServiceID search
	Conditions = search_patterns(InConditions,DB,'UNDEFINED'),
	{ ServiveIDs, _ } = element(3,Conditions),

	DB_RS=get_db_branch(DB,RS),
	RunPatterns=if Oper=='ANDNOT'->ServiveIDs; true->bitmap_oper(Oper,ServiveIDs,get_branch([],DB_RS)) end,

	% ServiceID cycle
	ResultRS=
		element(2,ecomet_bitmap:foldl(fun(IDP,{IDPBits,IDPMap})->
			% IDHIGH search
			{HBits,Conditions1}=seacrh_idhs(Conditions,DB,IDP),
			RunHBits=if Oper=='ANDNOT'->HBits; true->bitmap_oper(Oper,HBits,get_branch([IDP],DB_RS)) end,
			% IDH cycle
			case element(2,ecomet_bitmap:foldl(fun(IDH,{IDHBits,IDHMap})->
				% IDLOW search
				LBits=search_idls(Conditions1,DB,IDP,IDH),
				case bitmap_oper(Oper,LBits,get_branch([IDP,IDH],DB_RS)) of
					none->{IDHBits,IDHMap};
					ResIDLs->{ecomet_bitmap:set_bit(IDHBits,IDH),maps:put(IDH,ResIDLs,IDHMap)}
				end
			end,new_branch(),RunHBits,{none,none})) of
				{none,_}->{IDPBits,IDPMap};
				{IDHBits,IDHMap}->{ecomet_bitmap:set_bit(IDPBits,IDP),maps:put(IDP,{IDHBits,IDHMap},IDPMap)}
			end
		end,new_branch(),RunPatterns,{none,none})),
	Map([{DB,ResultRS}]).

%%
%% Search patterns
%%
search_patterns({'TAG',Tag,'UNDEFINED'},DB,ExtBits)->
	Storages=
		if
			% If no patterns range is defined yet, then search through all stoarges
			ExtBits=='UNDEFINED'->?STORAGE_TYPES;
			true->
				% Define storages where field is defined
				Field=element(1,Tag),
				FoundStorages=
					element(2,ecomet_bitmap:foldl(fun(ID,AccStorages)->
						Map=ecomet_pattern:get_map(ecomet_object:get_pattern_oid(ID)),
						case ecomet_field:get_storage(Map,Field) of
							{ok,Storage}->ordsets:add_element(Storage,AccStorages);
							{error,{undefined_field,_}}->AccStorages
						end
					end,[],ExtBits,{none,none})),
				% Order is [ram,ramdisc,disc]
				lists:subtract(?STORAGE_TYPES,lists:subtract(?STORAGE_TYPES,FoundStorages))
		end,
	Config=
		lists:foldr(fun(Storage,{AccPatterns,AccStorages})->
			case ecomet_index:read_tag(DB,Storage,[],Tag) of
				none->{AccPatterns,AccStorages};
				StoragePatterns->
					{bitmap_oper('OR',AccPatterns,StoragePatterns),[{Storage,StoragePatterns,[]}|AccStorages]}
			end
		end,{none,[]},Storages),
	{'TAG',Tag,Config};
search_patterns({'AND',Conditions,'UNDEFINED'},DB,ExtBits)->
	{ResPatterns,ResConditions}=
		lists:foldl(fun(Condition,{AccPatterns,AccCond})->
			PatternedCond=search_patterns(Condition,DB,AccPatterns),
			% If one branch can be true only for PATTERNS1, then hole AND can be true only for PATTERNS1
			{CBits,_}=element(3,PatternedCond),
			{bitmap_oper('AND',AccPatterns,CBits),AccCond++[PatternedCond]}
		end,{ExtBits,[]},Conditions),

	% 'UNDEFINED' only if AND contains no real tags.
	% !!! EMPTY {'AND',[]} MAY KILL ALL RESULTS
	Config=if ResPatterns=='UNDEFINED'->{none,[]}; true->{ResPatterns,[]} end,
	{'AND',ResConditions,Config};
search_patterns({'OR',Conditions,'UNDEFINED'},DB,ExtBits)->
	{ResPaterns,ResConditions}=
	lists:foldl(fun(Condition,{AccPatterns,AccCond})->
		PatternedCond=search_patterns(Condition,DB,ExtBits),
		{CBits,_}=element(3,PatternedCond),
		{bitmap_oper('OR',AccPatterns,CBits),AccCond++[PatternedCond]}
	end,{none,[]},Conditions),
	% !!! EMPTY {'OR',[]} MAY KILL ALL RESULTS
	{'OR',ResConditions,{ResPaterns,[]}};
search_patterns({'ANDNOT',{Condition1,Condition2},'UNDEFINED'},DB,ExtBits)->
	C1=search_patterns(Condition1,DB,ExtBits),
	{C1Bits,_}=element(3,C1),
	C2=search_patterns(Condition2,DB,C1Bits),
	% ANDNOT can be true only for C1 patterns
	{'ANDNOT',{C1,C2},{C1Bits,[]}};
search_patterns({'NORM',{{AND,ANDNOT},Direct},'UNDEFINED'},DB,ExtBits)->
	ResAND=search_patterns(AND,DB,ExtBits),
	{ANDBits,_}=element(3,ResAND),
	ResANDNOT=search_patterns(ANDNOT,DB,ANDBits),
	{'NORM',{{ResAND,ResANDNOT},Direct},{ANDBits,[]}};
% Strict operations
search_patterns(Condition,_DB,_ExtBits)->Condition.


%%
%% 	Search IDHs
%%
get_idh_storage([{Storage,SIDPBits,SIDHList}|Rest],IDP)->
	case ecomet_bitmap:get_bit(SIDPBits,IDP) of
		true->{Storage,SIDPBits,SIDHList};
		false->get_idh_storage(Rest,IDP)
	end;
get_idh_storage([],_IDP)->none.

seacrh_idhs({'TAG',Tag,{IDPBits,Storages}},DB,IDP)->
	% The tag can be found only in 1 storage for the pattern, because
	% tag defines field, that linked to certain storage
	case get_idh_storage(Storages,IDP) of
		none->{none,{'TAG',Tag,{IDPBits,Storages}}};
		{Storage,SIDPBits,SIDHList}->
			case ecomet_index:read_tag(DB,Storage,[IDP],Tag) of
				none->{none,{'TAG',Tag,{IDPBits,Storages}}};
				IDHs->
					ResStorages=[{Storage,SIDPBits,[{IDP,IDHs}|SIDHList]}|lists:keydelete(Storage,1,Storages)],
					{IDHs,{'TAG',Tag,{IDPBits,ResStorages}}}
			end
	end;
seacrh_idhs({Type,Condition,{IDPBits,IDHList}},DB,IDP)->
	case ecomet_bitmap:get_bit(IDPBits,IDP) of
		false->{none,{Type,Condition,{IDPBits,IDHList}}};
		true->
			{IDHBits,ResCondition}=search_type(Type,Condition,DB,IDP),
			ResIDHList=if IDHBits==none->IDHList; true->[{IDP,IDHBits}|IDHList] end,
			{IDHBits,{Type,ResCondition,{IDPBits,ResIDHList}}}
	end.
search_type('AND',Conditions,DB,IDP)->
	lists:foldl(fun(Condition,{AccBits,AccConditions})->
		if
		% One of branches is empty, no sense to search others
			AccBits==none->{none,AccConditions};
			true->
				{CBits,Condition1}=seacrh_idhs(Condition,DB,IDP),
				{bitmap_oper('AND',AccBits,CBits),AccConditions++[Condition1]}
		end
	end,{start,[]},Conditions);
search_type('OR',Conditions,DB,IDP)->
	lists:foldl(fun(Condition,{AccBits,AccConditions})->
		{CBits,Condition1}=seacrh_idhs(Condition,DB,IDP),
		{bitmap_oper('OR',AccBits,CBits),AccConditions++[Condition1]}
	end,{none,[]},Conditions);
search_type('ANDNOT',{Condition1,Condition2},DB,IDP)->
	case seacrh_idhs(Condition1,DB,IDP) of
		{none,Condition1_1}->{none,{Condition1_1,Condition2}};
		{C1Bits,Condition1_1}->
			{_C2Bits,Condition2_1}=seacrh_idhs(Condition2,DB,IDP),
			{C1Bits,{Condition1_1,Condition2_1}}
	end;
search_type('NORM',{{AND,ANDNOT},Direct},DB,IDP)->
	{ANDBits,AND1}=seacrh_idhs(AND,DB,IDP),
	ANDNOT1=
	if
		ANDBits==none->ANDNOT;
		true->
			{_,ANDNOTRes}=seacrh_idhs(ANDNOT,DB,IDP),
			ANDNOTRes
	end,
	{ANDBits,{{AND1,ANDNOT1},Direct}}.

search_idls({'TAG',Tag,Config},DB,IDP,IDH)->
	case get_idh_storage(element(2,Config),IDP) of
		none->none;
		{Storage,_SIDPBits,SIDHList}->
			case proplists:get_value(IDP,SIDHList,undefined) of
				% Nothing found for the Pattern, no sense to search storage
				undefined->none;
				IDHBits->
					case ecomet_bitmap:get_bit(IDHBits,IDH) of
						% Nothing found for the IDH
						false->none;
						true->ecomet_index:read_tag(DB,Storage,[IDP,IDH],Tag)
					end
			end
	end;
search_idls({Type,Condition,{_,IDHList}},DB,IDP,IDH)->
	case proplists:get_value(IDP,IDHList,undefined) of
		% Branch is empty for the IDP, no sense to search
		undefined->none;
		IDHBits->
			case ecomet_bitmap:get_bit(IDHBits,IDH) of
				% Branch is empty for the IDH
				false->none;
				true->search_type(Type,Condition,DB,IDP,IDH)
			end
	end.
search_type('AND',Conditions,DB,IDP,IDH)->
	lists:foldl(fun(Condition,AccBits)->
		if
		% One of branches is empty, no sense to search others
			AccBits==none->none;
			true->
				CBits=search_idls(Condition,DB,IDP,IDH),
				bitmap_oper('AND',AccBits,CBits)
		end
	end,start,Conditions);
search_type('OR',Conditions,DB,IDP,IDH)->
	lists:foldl(fun(Condition,AccBits)->
		CBits=search_idls(Condition,DB,IDP,IDH),
		bitmap_oper('OR',AccBits,CBits)
	end,none,Conditions);
search_type('ANDNOT',{Condition1,Condition2},DB,IDP,IDH)->
	case search_idls(Condition1,DB,IDP,IDH) of
		none->none;
		C1Bits->
			C2Bits=search_idls(Condition2,DB,IDP,IDH),
			bitmap_oper('ANDNOT',C1Bits,C2Bits)
	end;
search_type('NORM',{{AND,ANDNOT},{DAND,DANDNOT}},DB,IDP,IDH)->
	case search_idls(AND,DB,IDP,IDH) of
		none->none;
		ANDBits->
			ANDNOTBits=search_idls(ANDNOT,DB,IDP,IDH),
			case bitmap_oper('ANDNOT',ANDBits,ANDNOTBits) of
				none->none;
				TBits->
					element(2,ecomet_bitmap:foldl(fun(IDL,AccBits)->
						Object=ecomet_object:construct({IDP,IDH*?BITSTRING_LENGTH+IDL}),
						case check_direct(DAND,'AND',Object) of
							false->ecomet_bitmap:reset_bit(AccBits,IDL);
							true->
								case check_direct(DANDNOT,'OR',Object) of
									true->ecomet_bitmap:reset_bit(AccBits,IDL);
									false->AccBits
								end
						end
					end,TBits,TBits,{none,none}))
			end
	end.

check_direct(Conditions,Oper,Object)->
	Start=if Oper=='AND'->true; true->false end,
	check_direct(Conditions,Oper,Object,Start).
check_direct([Condition|Rest],'AND',Object,Result)->
	case (check_condition(Condition,Object) and Result) of
		true->check_direct(Rest,'AND',Object,true);
		false->false
	end;
check_direct([Condition|Rest],'OR',Object,Result)->
	case (check_condition(Condition,Object) or Result) of
		true->true;
		false->check_direct(Rest,'OR',Object,false)
	end;
check_direct([],_Oper,_Object,Result)->Result.

check_condition({'DIRECT',{Oper,Field,Value},_},Object)->
	case ecomet_object:read_field(Object,Field) of
		{ok,FieldValue}->
			if
				not is_list( FieldValue )->
					direct_compare(Oper,Value,FieldValue);
				true ->
					case ecomet_object:field_type(Object,Field) of
						{ok,{list,_}}->
							direct_compare_list(Oper,Value,FieldValue);
						_->
							direct_compare(Oper,Value,FieldValue)
					end
			end;
		{error,_}->false
	end.

direct_compare(Oper,Value,FieldValue)->
	if
		Oper =:= ':='; Oper =:= '='->FieldValue==Value;
		Oper =:= ':>'; Oper=:= '>'->FieldValue>Value;
		Oper =:= ':>='; Oper=:='>='->FieldValue>=Value;
		Oper =:= ':<'; Oper=:='<'->FieldValue<Value;
		Oper=:=':=<'; Oper=:='=<'->FieldValue=<Value;
		Oper =:=':LIKE'; Oper=:='LIKE'->direct_like(FieldValue,Value);
		Oper =:= ':<>'; Oper=:='<>'->FieldValue/=Value
	end.

direct_like(String,Pattern) when (is_binary(Pattern) and is_binary(String))->
	case Pattern of
		% Symbol '^' is string start
		<<"^",StartPattern/binary>>->
			Length=size(StartPattern)*8,
			case String of
				<<StartPattern:Length/binary,_/binary>>->true;
				_->false
			end;
		% Full text search
		_->case binary:match(String,Pattern) of
				 nomatch->false;
				 _->true
			 end
	end;
direct_like(_String,_Pattern)->
	false.

direct_compare_list(Oper, Value, ListValue)->
	case direct_compare( Oper, Value, ListValue ) of
		true -> true;
		_->
			compare_list_items(ListValue, Oper, Value)
	end.

compare_list_items([V|Rest], Oper, Value )->
	case direct_compare(Oper, Value, V) of
		true -> true;
		_-> compare_list_items( Rest, Oper, Value )
	end;
compare_list_items([], _Oper, _Value )->
	false.

%%=====================================================================
%%	ResultSet iterator
%%=====================================================================
count(RS)->
	lists:foldr(fun({_DB,DB_RS},RSAcc)->
		element(2,ecomet_bitmap:foldl(fun(IDP,PatternAcc)->
			element(2,ecomet_bitmap:foldl(fun(IDH,Acc)->
				Acc+ecomet_bitmap:count(get_branch([IDP,IDH],DB_RS))
			end,PatternAcc,get_branch([IDP],DB_RS),{none,none}))
		end,RSAcc,get_branch([],DB_RS),{none,none}))
	end,0,RS).

foldr(F,Acc,RS)->
  foldr(F,Acc,RS,none).
foldr(F,Acc,RS,Page)->
  fold(F,Acc,RS,foldr,Page).

foldl(F,Acc,RS)->
  foldl(F,Acc,RS,none).
foldl(F,Acc,RS,Page)->
  fold(F,Acc,RS,foldl,Page).

fold(F,Acc,RS,Scan,none)->
  lists:Scan(fun({_DB,DB_RS},RSD)->
    element(2,ecomet_bitmap:Scan(fun(IDP,RSP)->
			element(2,ecomet_bitmap:Scan(fun(IDH,RSH)->
				element(2,ecomet_bitmap:Scan(fun(IDL,RSL)->
          F({IDP,IDH*?BITSTRING_LENGTH+IDL},RSL)
        end,RSH,get_branch([IDP,IDH],DB_RS),{none,none}))
      end,RSP,get_branch([IDP],DB_RS),{none,none}))
    end,RSD,get_branch([],DB_RS),{none,none}))
  end,Acc,RS);
fold(F,Acc,RS,Scan,{Page,Length})->
  From=(Page-1)*Length,
  To=From+Length,
  lists:Scan(fun({_DB,DB_RS},RSD)->
    element(2,ecomet_bitmap:Scan(fun(IDP,RSP)->
      element(2,ecomet_bitmap:Scan(fun(IDH,{Count,RSH})->
				{BitsCount,NewAcc}=ecomet_bitmap:Scan(fun(IDL,RSL)->
					F({IDP,IDH*?BITSTRING_LENGTH+IDL},RSL)
        end,RSH,get_branch([IDP,IDH],DB_RS),{From-Count,To-Count}),
				{Count+BitsCount,NewAcc}
      end,RSP,get_branch([IDP],DB_RS),{none,none}))
    end,RSD,get_branch([],DB_RS),{none,none}))
  end,{0,Acc},RS).

%%=====================================================================
%%	Internal helpers
%%=====================================================================
get_db_branch(DB,RS)->
	case lists:keyfind(DB,1,RS) of
		{DB,DB_Result}->DB_Result;
		false->new_branch()
	end.

% Get branch from result set
get_branch([Key|Rest],{_,Map})->
	case maps:find(Key,Map) of
		{ok,Value}->get_branch(Rest,Value);
		error->none
	end;
get_branch([],Value)->
	case Value of
		{Bits,Map} when is_map(Map)->Bits;
		Bits->Bits
	end.

%%=====================================================================
%%	Datetime range conditions
%%=====================================================================
dt_query(Field,I1,I2)->
	Cap=get_cap(I1,I2,[]),
	Trapezoid =
		if
			length(Cap)=:=length(I1)-> [];
			true ->
				[{L,V1}|TLeft]=I1--Cap,
				[{L,V2}|TRight]=I2--Cap,
				EdgeLeft=edge(TLeft,left),
				EdgeRight=edge(TRight,right),
				[{'OR',
						[triangle_left(EdgeLeft,{L,V1})]++
						[{L,I}||I<-lists:seq(V1+1,V2-1)]++
						[triangle_right(EdgeRight,{L,V2})]
				}]
		end,
	set_query_field({'AND',Cap++Trapezoid},Field).

get_cap([V|T1],[V|T2],Acc)->
	get_cap(T1,T2,[V|Acc]);
get_cap(_T1,_T2,Acc)->
	lists:reverse(Acc).

edge(Levels,Side)->
	edge(lists:reverse(Levels),Side,[]).

edge([{L,V}|T],Side,[])->
	Limit=
		if
			Side=:=left ->level_min(L);
			true -> level_max(L)
		end,
	if
		V=:=Limit ->edge(T,Side,[]) ;
		true -> edge(T,Side,[{L,V}])
	end;
edge([I|T],Side,Acc)->
	edge(T,Side,[I|Acc]);
edge([],_Side,Acc)->
	Acc.

triangle_left([{L,V}|Tail],Vertex)->
	Min=level_min(L),
	Max=level_max(L),
	if
		V=:=Min->
			% Add the whole level
			if
				length(Tail)=:=0 ->
					% No filtering for the sub-levels is needed, add the whole vertex
					Vertex;
				true->
					% The edge element must be calculated separately
					{'OR',[
						{'ANDNOT',Vertex,{L,V}},	% Total level except for the first element
						{'AND',[Vertex,triangle_left(Tail,{L,V})]}	% Add triangle of the first element
					]}
			end;
		V=:=Max->
			% Take only triangle of the last element
			{'AND',[Vertex,triangle_left(Tail,{L,V})]};
		(V-Min)<(Max-V)->
			% It is easier to subtract excessive items from the whole level
			if
				length(Tail)=:=0->
					% No filtering for the sub-levels is needed, add the whole vertex
					{'ANDNOT',Vertex,{'OR',[{L,I}||I<-lists:seq(Min,V-1)]}};
				true->
					% The edge element must be calculated separately
					{'OR',[
						{'ANDNOT',Vertex,{'OR',[{L,I}||I<-lists:seq(Min,V)]}},	% The level after subtraction not included bundles
						{'AND',[Vertex,triangle_left(Tail,{L,V})]}	% Add triangle of the edge element
					]}
			end;
		true ->
			% Add level bundles
			{'AND',[Vertex,{'OR',[
				triangle_left(Tail,{L,V})|
				[{L,I}||I<-lists:seq(V+1,Max)]
			]}]}
	end;
triangle_left([],Vertex)->
	Vertex.

triangle_right([{L,V}|Tail],Vertex)->
	Min=level_min(L),
	Max=level_max(L),
	if
		V=:=Max->
			% Add the whole level
			if
				length(Tail)=:=0 ->
					% No filtering for the sub-levels is needed, add the whole vertex
					Vertex;
				true->
					% The edge element must be calculated separately
					{'OR',[
						{'ANDNOT',Vertex,{L,V}},	% Total level except for the first element
						{'AND',[Vertex,triangle_right(Tail,{L,V})]}	% Add triangle of the first element
					]}
			end;
		V=:=Min->
			% Take only triangle of the first element
			{'AND',[Vertex,triangle_right(Tail,{L,V})]};
		(V-Min)>(Max-V)->
			% It is easier to subtract excessive items from the whole level
			if
				length(Tail)=:=0 ->
					% No filtering for the sub-levels is needed, add the whole vertex
					{'ANDNOT',Vertex,{'OR',[{L,I}||I<-lists:seq(V+1,Max)]}};
				true ->
					{'OR',[
						{'ANDNOT',Vertex,{'OR',[{L,I}||I<-lists:seq(V,Max)]}},	% The level after subtraction not included bundles
						{'AND',[Vertex,triangle_right(Tail,{L,V})]}	% Add triangle of the edge element
					]}
			end;
		true ->
			% Add level bundles
			{'AND',[Vertex,{'OR',
					[{L,I}||I<-lists:seq(Min,V-1)]++
					[triangle_right(Tail,{L,V})]
			}]}
	end;
triangle_right([],Vertex)->
	Vertex.

level_max(m)->12;
level_max(d)->31;
level_max(h)->23;
level_max(mi)->59;
level_max(s)->59.

level_min(m)->1;
level_min(d)->1;
level_min(h)->0;
level_min(mi)->0;
level_min(s)->0.

set_query_field({'AND',Items},Field)->
	{'AND',[set_query_field(I,Field)||I<-Items],'UNDEFINED'};
set_query_field({'OR',Items},Field)->
	{'OR',[set_query_field(I,Field)||I<-Items],'UNDEFINED'};
set_query_field({'ANDNOT',Item1,Item2},Field)->
	{'ANDNOT',{set_query_field(Item1,Field),set_query_field(Item2,Field)},'UNDEFINED'};
set_query_field(Item,Field)->
	{'TAG',{Field,Item,datetime},'UNDEFINED'}.


%%=====================================================================
%%	Recursive search by path
%%=====================================================================
path_recursive_search([Name])->
	% Search only by name
	NameLength = length( unicode:characters_to_list(Name) ),
	if
		NameLength >= 3 ->
			% We use fuzzy search if the string is 3 symbols or more
			Roots = ecomet:get('*',[<<".oid">>],{'AND',[
				{<<".pattern">>,'=',?OID(<<"/root/.patterns/.folder">>)},
				{<<".name">>,'LIKE',Name}
			]}),
			{'OR',[
				build_leaf({'LIKE',<<".name">>,Name})
				|[build_leaf({'=',<<".folder">>,OID}) || OID <- ecomet_folder:get_content_recursive( Roots )]
			],'UNDEFINED'};
		true ->
			% Otherwise only exact search
			Roots = ecomet:get('*',[<<".oid">>],{'AND',[
				{<<".pattern">>,'=',?OID(<<"/root/.patterns/.folder">>)},
				{<<".name">>,'=',Name}
			]}),
			{'OR',[
				build_leaf({'=',<<".name">>,Name})
				|[build_leaf({'=',<<".folder">>,OID}) || OID <- ecomet_folder:get_content_recursive( Roots )]
			],'UNDEFINED'}
	end;

path_recursive_search( [Root|TailPath] )->
	case find_roots( Root ) of
		[]->
			% There are no folders fitting a root
			empty_tag();
		RootFolders->

			% Find possible starting points
			{ExactTail, [ItemName]} = lists:split(length(TailPath) -1, TailPath),
			case [F || {ok,F}<-[ ecomet_folder:path2oid(R, ExactTail) || R <- RootFolders ]] of
				[]->
					% Nothing can be found as there are no folders with a path that satisfies the query
					empty_tag();
				StartPoints->
					% Find possible entry points
					TailNameLength = length( unicode:characters_to_list( ItemName ) ),
					if
						TailNameLength >= 2 ->
							case ecomet_query:system('*',[<<".oid">>],{'AND',[
								{'OR',[{<<".folder">>,'=',OID} || OID <- StartPoints]},
								{<<".name">>,'LIKE',<<"^",ItemName/binary>>}
							]}) of
								[]->
									% No satisfying entry points, nothing can be found
									{'TAG',{<<".name">>,none,'simple'},'UNDEFINED'};
								EntryPoints->
									% Search through EntryPoints recursively and unite with the result of search in StartPoints by name
									{'OR',[
										{'AND',[
											{'OR',[build_leaf({'=',<<".folder">>,OID})|| OID <- StartPoints],'UNDEFINED'},
											build_leaf({'LIKE',<<".name">>,<<"^",ItemName/binary>>})
										],'UNDEFINED'}
										| [build_leaf({'=',<<".folder">>,OID})|| OID <- ecomet_folder:get_content_recursive( EntryPoints )]
									],'UNDEFINED'}
							end;
						true ->
							case ecomet_query:system('*',[<<".oid">>],{'AND',[
								{'OR',[{<<".folder">>,'=',OID} || OID <- StartPoints]},
								{<<".name">>,'=',ItemName}
							]}) of
								[]->
									% No satisfying entry points, nothing can be found
									empty_tag();
								EntryPoints->
									% Search through EntryPoints recursively and unite with the result of search in StartPoints by name
									{'OR',[
										{'AND',[
											{'OR',[build_leaf({'=',<<".folder">>,OID})|| OID <- StartPoints],'UNDEFINED'},
											build_leaf({'=',<<".name">>,ItemName})
										],'UNDEFINED'}
										| [build_leaf({'=',<<".folder">>,OID})|| OID <- ecomet_folder:get_content_recursive( EntryPoints )]
									],'UNDEFINED'}
							end
					end
			end
	end.

find_roots( Name )->

	RootLength = length( unicode:characters_to_list(Name) ),
	if
		RootLength >= 2 ->
			% We can search min by 3 symbols as Root ends we can add a string end symbol '$'
			% therefore the minimum is 2
			ecomet_query:system('*',[<<".oid">>],{'AND',[
				{<<".pattern">>,'=',?OID(<<"/root/.patterns/.folder">>)},
				{<<".name">>,'LIKE',<<Name/binary,"$">>}
			]});
		true ->
			ecomet_query:system('*',[<<".oid">>],{'AND',[
				{<<".pattern">>,'=',?OID(<<"/root/.patterns/.folder">>)},
				{<<".name">>,'=',Name}
			]})
	end.

empty_tag()->
	{'TAG',{<<".name">>,none,'simple'},'UNDEFINED'}.

