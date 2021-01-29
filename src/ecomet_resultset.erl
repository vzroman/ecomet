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
	execute_local/4,
	remote_call/6,
	count/1,
	foldr/3,
	foldr/4,
	foldl/3,
	foldl/4,
	fold/5
]).

%%====================================================================
%% Subscriptions API
%%====================================================================
-export([
	on_init/0,
	subscription_prepare/1,
	subscription_fields/1,
	subscription_indexes/1,
	subscription_tags/1,
	subscription_match_function/1,
	subscription_compile/1
]).

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([
	define_patterns/1,
	build_conditions/1,
	optimize/1,
	search_patterns/3,
	seacrh_idhs/3,
	execute_remote/4,
	wait_remote/2
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
on_init()->
	% Store the subscription pattern as a persistent term for optimized access
	PatternID = ?OID(<<"/root/.patterns/.subscription">>),
	ID=ecomet_object:get_id(PatternID),
	Bit=ecomet_bitmap:set_bit(none,ID),
	persistent_term:put({?MODULE,subscription_pattern},Bit),
	ok.

subscription_prepare(Conditions)->

	% Standard preparation procedure
	Patterned=define_patterns(Conditions),
	Built=build_conditions(Patterned),

	% Normalization
	{'OR',Normal,_}=normalize(Built),

	% Extract tags
	[{
			[ Tag || {'TAG',Tag,_} <- AND ], 				% 1. And tags
			[ Tag || {'TAG',Tag,_} <- ANDNOT ],			% 2. Andnot tags
			[ Tag || {'DIRECT',Tag,_} <- DAND] ,		% 3. And direct conditions
			[ Tag || {'DIRECT',Tag,_} <- DANDNOT]		% 4. Andnot direct conditions
		}
	|| {'NORM',{ {{'AND',AND,_},{'OR',ANDNOT,_}}, {DAND, DANDNOT} }, _} <- Normal ].

subscription_fields(Subscription)->
	Fields=
		[
			[ F || {F, _, _ } <- AND ] ++
			[ F || {F, _, _ } <- ANDNOT ] ++
			[ F || {_, F, _ } <- DAND ] ++
			[ F || {_, F, _ } <- DANDNOT ]
		||{ AND, ANDNOT, DAND, DANDNOT} <- Subscription ],
	ordsets:from_list(lists:append(Fields)).

subscription_indexes(Subscription)->
	Index=
		[case AND of
			 [T1,T2,T3|_] ->[{T1,1},{T2,2},{T3,3}];
			 [T1,T2]			->[{T1,1},{T2,2},{none,3}];
			 [T1]					->[{T1,1},{none,2},{none,3}]
		end ||{ AND, _ANDNOT, _DAND, _DANDNOT} <- Subscription ],
	ordsets:from_list(lists:append(Index)).

subscription_tags(Tags)->
	Tags1=
		[[{T,1},{T,2},{T,3}]|| T <- Tags],
	[{none,2},{none,3}|lists:append(Tags1)].

subscription_match_function(Subscription)->
	Ordered=
		[{
			ordsets:from_list(AND),
			ordsets:from_list(ANDNOT),
			ordsets:from_list(DAND),
			ordsets:from_list(DANDNOT)
		} || { AND, ANDNOT, DAND, DANDNOT} <- Subscription ],

	fun(Tags,Fields)->
		reverse_check(Ordered,Tags,Fields)
	end.

subscription_compile(Query)->
	case persistent_term:get({?MODULE,subscription_pattern},none) of
		none->
			% The schema is not initialized yet
			prepare({<<"none">>,'=',none});
		Pattern->
			subscription_compile(Query,Pattern)
	end.
subscription_compile({'AND',List},Pattern)->
	{'AND',[subscription_compile(I,Pattern)||I<-List],{Pattern,[]}};
subscription_compile({'OR',List},Pattern)->
	{'OR',[subscription_compile(I,Pattern)||I<-List],{Pattern,[]}};
subscription_compile({Field,'=',Value},Pattern)->
	{'TAG', {Field,Value,simple}, {Pattern,[{ramlocal,Pattern,[]}]}}.


reverse_check([{And,AndNot,DAnd,DAndNot}|Rest],Tags,Fields)->
	Result=
		case ordsets:subtract(And,Tags) of
			[]->
				% The object satisfies all the ANDs
				case ordsets:intersection(AndNot,Tags) of
					[]->
						% The object does not have any ANDNOTs
						case direct_and(DAnd,Fields) of
							true->
								% The object satisfies all the Direct ANDs
								case direct_or(DAndNot,Fields) of
									false->
										% The object does not have any Direct ANDNOTs
										true;
									_->
										false
								end;
							_->
								false
						end;
					_->
						false
				end;
			_->
				false
		end,
	if
		Result -> Result;
		true ->
			reverse_check(Rest,Tags,Fields)
	end;
reverse_check([],_Tags,_Fields)->
	false.

direct_and([{Oper,Field,Value}|Rest],Fields)->
	FieldValue = maps:get(Field,Fields,none),
	case direct_compare(Oper,Value,FieldValue) of
		false->false;
		_->direct_and(Rest,Fields)
	end;
direct_and([],_Fields)->
	true.

direct_or([{Oper,Field,Value}|Rest],Fields)->
	FieldValue = maps:get(Field,Fields,none),
	case direct_compare(Oper,Value,FieldValue) of
		true->true;
		_->direct_or(Rest,Fields)
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
	Patterns = [PatternID|ecomet_pattern:get_children(PatternID)],
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
build_leaf({'=',<<".path">>,Value})->
	case re:run(Value,"(?<F>.*)/(?<N>[^/]+)",[{capture,['F','N'],binary}]) of
		{match,[Folder,Name]}->
			case ecomet_folder:path2oid(Folder) of
				{ok,FolderID}->
					{'AND',[
						{'TAG',{<<".folder">>,FolderID,'simple'},'UNDEFINED'},
						{'TAG',{<<".name">>,Name,'simple'},'UNDEFINED'}
					],'UNDEFINED'};
				_->
					?ERROR({invalid_path,Value})
			end;
		_->
			?ERROR({invalid_path,Value})
	end;
build_leaf({'=',<<".oid">>,Value})->
	Object = ecomet_object:construct(Value),
	#{
		<<".name">>:=Name,
		<<".folder">>:=Folder
	} = ecomet:read_fields(Object,[<<".name">>,<<".folder">>]),

	{'AND',[
		{'TAG',{<<".folder">>,Folder,'simple'},'UNDEFINED'},
		{'TAG',{<<".name">>,Name,'simple'},'UNDEFINED'}
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
	% Search steps:
	% 1. start remote databases search
	% 2. execute local databases search
	% 3. reduce remote search results
	% 4. Construct query result
	{LocalDBs,RemoteDBs}=lists:foldl(fun(DB,{Local,Remote})->
		case ecomet_db:is_local(DB) of
			true->{[DB|Local],Remote};
			false->{Local,[DB|Remote]}
		end
	end,{[],[]},DBs),
	% Step 1. Start remote search
	PID=execute_remote(RemoteDBs,Conditions,Map,Union),
	% Step 2. Local search
	LocalResult=lists:foldl(fun(DB,Result)->
		[{DB,execute_local(DB,Conditions,Map,Union)}|Result]
	end,[],LocalDBs),
	% Step 3. Reduce remote, order by database
	SearchResult=order_DBs(DBs,wait_remote(PID,LocalResult)),
	% Step 4. Return result
	Reduce(SearchResult).

order_DBs(DBs,Results)->
	order_DBs(DBs,Results,[]).
order_DBs([DB|Rest],Results,Acc)->
	case lists:keytake(DB,1,Results) of
		false->order_DBs(Rest,Results,Acc);
		{value,{_,DBResult},RestResults}->order_DBs(Rest,RestResults,[DBResult|Acc])
	end;
order_DBs([],_Results,Acc)->Acc.

% 1. Map search to remote nodes
% 2. Reduce results, reply
execute_remote([],_Conditions,_Map,_Union)->none;
execute_remote(DBs,Conditions,Map,Union)->
	ReplyPID=self(),
	IsTransaction=ecomet:is_transaction(),
	spawn_link(fun()->
		process_flag(trap_exit,true),
		StartedList=start_remote(DBs,Conditions,Map,Union,IsTransaction),
		ReplyPID!{remote_result,self(),reduce_remote(StartedList,[])}
	end).

% Wait for results from remote databases
wait_remote(none,RS)->RS;
wait_remote(PID,RS)->
	receive
		{remote_result,PID,RemoteRS}->RS++RemoteRS
	after
		?TIMEOUT->
			PID!query_timeout,
			receive
				{remote_result,PID,RemoteRS}->RS++RemoteRS
			after
				2000->?LOGWARNING(remote_request_timeout)
			end
	end.

%% Starting remote search
start_remote(DBs,Conditions,Map,Union,IsTransaction)->
	lists:foldl(fun(DB,Result)->
		case ecomet_db:get_search_node(DB,[]) of
			none->Result;
			Node->
				SearchParams=[DB,Conditions,Union,Map,IsTransaction],
				[{spawn_link(Node,?MODULE,remote_call,[self()|SearchParams]),SearchParams,[Node]}|Result]
		end
	end,[],DBs).

%% Search for remote process
remote_call(PID,DB,Conditions,Union,Map,IsTransaction)->
	LocalResult=
		if
			IsTransaction ->
				case ecomet:transaction(fun()->
					execute_local(DB,Conditions,Map,Union)
				end) of
					{ok,Result}->Result;
					{error,Error}->exit(Error)
				end;
			true ->execute_local(DB,Conditions,Map,Union)
		end,
	PID!{ecomet_resultset,self(),{DB,LocalResult}}.

%% Reducing remote results
reduce_remote([],ReadyResult)->ReadyResult;
% Wait remote results
reduce_remote(WaitList,ReadyResult)->
	receive
		% Result received
		{ecomet_resultset,PID,DBResult}->
			case lists:keyfind(PID,1,WaitList) of
				% We are waiting for this result
				{PID,_,_}->reduce_remote(lists:keydelete(PID,1,WaitList),[DBResult|ReadyResult]);
				% Unexpected result
				false->reduce_remote(WaitList,ReadyResult)
			end;
		% Remote process is down
		{'EXIT',PID,Reason}->
			case lists:keyfind(PID,1,WaitList) of
				% It's process that we are waiting result from. Node is node available now,
				% let's try another one form the cluster
				{PID,Params,TriedNodes} when Reason==noconnection->
					case ecomet_db:get_search_node(lists:nth(1,Params),TriedNodes) of
						% No other nodes can search this domain
						none->
							?LOGWARNING({no_search_nodes_available,lists:nth(1,Params)}),
							reduce_remote(lists:keydelete(PID,1,WaitList),ReadyResult);
						% Let's try another node
						NextNode->
							% Start task
							NewPID=spawn_link(NextNode,?MODULE,remote_call,[self()|Params]),
							reduce_remote(lists:keyreplace(PID,1,WaitList,{NewPID,Params,[NextNode|TriedNodes]}),ReadyResult)
					end;
				% We caught exit from some linked process.
				% Variant 1. Parent process get exit, stop the task
				% Variant 2. Process we are waiting is crashed due to error in the user fun
				_->exit(Reason)
			end;
		query_timeout->ReadyResult
	end.

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
							{error,undefined_field}->AccStorages
						end
					end,[],ExtBits,{none,none})),
				% Order is [ramlocal,ram,ramdisc,disc]
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
		lists:foldr(fun(Condition,{AccPatterns,AccCond})->
			PatternedCond=search_patterns(Condition,DB,AccPatterns),
			% If one branch can be true only for PATTERNS1, then hole AND can be true only for PATTERNS1
			{CBits,_}=element(3,PatternedCond),
			{bitmap_oper('AND',AccPatterns,CBits),[PatternedCond|AccCond]}
		end,{ExtBits,[]},Conditions),
	% 'UNDEFINED' only if AND contains no real tags.
	% !!! EMPTY {'AND',[]} MAY KILL ALL RESULTS
	Config=if ResPatterns=='UNDEFINED'->{none,[]}; true->{ResPatterns,[]} end,
	{'AND',ResConditions,Config};
search_patterns({'OR',Conditions,'UNDEFINED'},DB,ExtBits)->
	{ResPaterns,ResConditions}=
	lists:foldr(fun(Condition,{AccPatterns,AccCond})->
		PatternedCond=search_patterns(Condition,DB,ExtBits),
		{CBits,_}=element(3,PatternedCond),
		{bitmap_oper('OR',AccPatterns,CBits),[PatternedCond|AccCond]}
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
search_patterns({Oper,Conditions,{IntBits,IDHList}},_DB,ExtBits)->
	XBits=bitmap_oper('AND',ExtBits,IntBits),
	{Oper,Conditions,{XBits,IDHList}};
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
	lists:foldr(fun(Condition,{AccBits,AccConditions})->
		if
		% One of branches is empty, no sense to search others
			AccBits==none->{none,AccConditions};
			true->
				{CBits,Condition1}=seacrh_idhs(Condition,DB,IDP),
				{bitmap_oper('AND',AccBits,CBits),[Condition1|AccConditions]}
		end
	end,{start,[]},Conditions);
search_type('OR',Conditions,DB,IDP)->
	lists:foldr(fun(Condition,{AccBits,AccConditions})->
		{CBits,Condition1}=seacrh_idhs(Condition,DB,IDP),
		{bitmap_oper('OR',AccBits,CBits),[Condition1|AccConditions]}
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
			direct_compare(Oper,Value,FieldValue);
		{error,_}->false
	end.

direct_compare(Oper,Value,FieldValue)->
	case Oper of
		':='->FieldValue==Value;
		':>'->FieldValue>Value;
		':>='->FieldValue>=Value;
		':<'->FieldValue<Value;
		':=<'->FieldValue=<Value;
		':LIKE'->direct_like(FieldValue,Value);
		':<>'->FieldValue/=Value
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
	end.

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

