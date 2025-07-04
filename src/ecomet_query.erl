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

-module(ecomet_query).

-include("ecomet.hrl").
-include("ecomet_subscription.hrl").

-record(compiled_query,{conditions,map,reduce}).
-record(field,{alias,value}).
-record(get,{value,args,aggregate}).

-export([
  run/1,
  parse/1,
  run_statements/1,
  get/3,get/4,
  subscribe/4,subscribe/5,
  unsubscribe/1,
  set/3,set/4,
  insert/2,
  delete/2,delete/3,
  compile/3,compile/4,
  system/3,
  query_object/2
]).

%%====================================================================
%%		Parser
%%====================================================================
-export([
  compile_function/1,compile_function/3,
  wrap_transactions/1
]).


%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([
  get_alias/2,
  read_fun/2,
  read_map/2,
  is_aggregate/1,
  read_up/2,
  insert_to_group/4,
  sort_groups/2,
  merge_trees/4,
  reorder_fields/3,
  grouped_resut/4,
  groups_page/2,
  compare_rows/3,
  sort_leafs/3,
  compile_map_reduce/3
]).

-endif.

-define(TRANSACTION(Fun),
  case ecomet_transaction:get_type() of
    none->
      case ecomet_transaction:internal(Fun) of
        {ok,_TResult}->_TResult;
        {abort,_TError}->?ERROR(_TError)
      end;
    _->
      Fun()
  end).

%%=====================================================================
%%
%%			API
%%
%%=====================================================================
run(Statements)->
  ParsedStatements=parse(Statements),
  run_statements(ParsedStatements).

parse(Statements) when is_binary(Statements)->
  parse(unicode:characters_to_list(Statements));
parse(Statements) when is_list(Statements)->
  case ecomet_ql_lexer:string(Statements) of
    {ok,Tokens,_}->
      case ecomet_ql_parser:parse(Tokens) of
        {ok,StatementsList}->wrap_transactions(StatementsList);
        {error,Error}->?ERROR(Error)
      end;
    Error->?ERROR(Error)
  end;
parse(_Statements)->?ERROR(invalid_string).

run_statements(Statements)->
  run_statements(Statements,[]).
run_statements([S|Rest],Acc)->
  run_statements(Rest,safe_statement(S,Acc));
run_statements([],Acc)->
  case Acc of
    [Single]->Single;
    Multiple->lists:reverse(Multiple)
  end.

safe_statement(Statement,Acc)->
  try
    run_statement(Statement,Acc)
  catch
    _:Error ->[{error,Error}|Acc]
  end.

run_statement({get,Fields,DBs,Condition,Params},Acc)->
  [get(DBs,Fields,Condition,Params)|Acc];
run_statement({subscribe,ID,Fields,DBs,Condition,Params},Acc)->
  [subscribe(ID,DBs,Fields,Condition,Params)|Acc];
run_statement({unsubscribe,ID},Acc)->
  [unsubscribe(ID)|Acc];
run_statement({set,Fields,DBs,Condition,Params},Acc)->
  [set(DBs,Fields,Condition,Params)|Acc];
run_statement({insert,Fields,Params},Acc)->
  [insert(Fields,Params)|Acc];
run_statement({delete,DBs,Condition,Params},Acc)->
  [delete(DBs,Condition,Params)|Acc];
run_statement(transaction_start,Acc)->
  ecomet_transaction:start(),
  [ok|Acc];
run_statement(transaction_commit,Acc)->
  ecomet_transaction:commit(),
  [ok|Acc];
run_statement(transaction_rollback,Acc)->
  ecomet_transaction:rollback(),
  [ok|Acc];
run_statement({transaction,Statements},Acc)->
  case ecomet_transaction:internal(fun()->
    lists:foldl(fun run_statement/2,Acc,Statements)
  end) of
    {ok,Results}->Results;
    {abort,Error}->
      Results=[{error,Error}||_<-lists:seq(1,length(Statements))],
      Results++Acc
  end.

%%=====================================================================
%%	GET
%%=====================================================================
% Fields = [
%   <<"field1">>,                   - name of field
%   {<<"alias">>,<<"field2">>},     - alias for field
%   {fun,[Fields]},                 - fun on fields
%   {<<"alias">>,{fun,[Fields]}}    - alias for fun
% ]
% Params:
%%	- page => {Number,ItemsPerPage} - Paginating. Return only items for defined page
%%	- order => [{Field1,Order},{Field2,Order}] - Sorting results
%%	- group => [Field1,Field2] - Grouping results
%%  - format => fun/2 - Function to format field values
%%	- lock => none|read|write - lock level on objects
get(DBs,Fields,Conditions)->
  get(DBs,Fields,Conditions,#{}).
get(DBs,Fields,Conditions,Params) when is_list(Params)->
  get(DBs,Fields,Conditions,maps:from_list(Params));
get(DBs,Fields,Conditions,Params)->
  CompiledQuery=compile(get,Fields,Conditions,Params),
  Union=maps:get(union,Params,{'OR',ecomet_resultset:new()}),
  case maps:get(lock,Params,none) of
    none ->
      % All operations are dirty
      execute(no_transaction,CompiledQuery,DBs,Union);
    _->
      execute(execute,CompiledQuery,DBs,Union)
  end.

system(DBs,Fields,Conditions)->
  Conditions1=ecomet_resultset:prepare(Conditions),
  {Map,Reduce}=compile_map_reduce(get,Fields,#{}),
  Compiled = #compiled_query{
    conditions = Conditions1,
    map = Map,
    reduce = Reduce
  },
  execute(no_transaction,Compiled,DBs,{'OR',ecomet_resultset:new()}).

%%=====================================================================
%%	SUBSCRIBE
%%=====================================================================
% Arguments are the same as for 'get' (see get). Not all parameters are supported for
% subscriptions.
% Grouping, Sorting, Pagination are not supported.
% no_feedback, stateless are supported
subscribe(ID,DBs,Fields,Conditions)->
  subscribe(ID,DBs,Fields,Conditions,#{}).
subscribe(ID,DBs,Fields,Conditions,InParams) when is_list(InParams)->
  subscribe(ID,DBs,Fields,Conditions,maps:from_list(InParams));
subscribe(ID,DBs,Fields,Conditions,InParams)->

  Params = #{
    format := Formatter         % Format fields according to their types
  } = maps:merge(#{
    stateless => false,
    no_feedback => false,
    format => undefined
  },InParams),

  {Deps, Read} = compile_subscribe_read(Fields,Formatter),

  ecomet_subscription:subscribe(#subscription{
    id = ID,
    owner = self(),
    dbs = DBs,
    read = Read,
    deps = Deps,
    conditions = Conditions,
    params = Params
  }).


compile_subscribe_read([<<".oid">>],_Formatter)->
  {
    [],
    fun(_Object)-> #{} end
  };
compile_subscribe_read(['*'],Formatter) when is_function(Formatter,2)->
  ReadField =
    fun(Object,Field)->
      Value = maps:get(Field,Object,none),
      {ok,Type}=ecomet_object:field_type(maps:get(object,Object),Field),
      Formatter(Type,Value)
    end,

  Read =
    fun(Object)->
      maps:map(fun(Field,_)-> ReadField(Object,Field) end,Object)
    end,
  {['*'], Read};
compile_subscribe_read(['*'],_Formatter)->
  Read =
    fun(Object)->
      maps:map(fun(Field,_)-> maps:get(Field,Object,none) end,Object)
    end,
  { ['*'], Read };

compile_subscribe_read(Fields,Formatter)->

  % Per object reading plan
  ReadMap=read_map(Fields,Formatter),

  Deps =
    maps:fold(fun(_,#field{value = #get{args = Args}},Acc)->
      ordsets:union( Acc, Args )
    end,[],ReadMap),

  Read =
    fun(Object)->
      Changed = ordsets:from_list( maps:keys( Object )),
      maps:fold(fun(_,#field{alias = Alias,value = #get{args = Args,value=Fun}},Acc)->
        case ordsets:intersection(Changed,Args) of
          []->
            % If the changes are not among the function arguments list the field is not affected
            Acc;
          _->
            % The value has to be recalculated
            Acc#{Alias=>Fun(Object)}
        end
      end,#{},ReadMap)
    end,
  { Deps, Read }.


unsubscribe(ID)->
  ecomet_subscription:unsubscribe( ID ).

%%=====================================================================
%%	SET
%%=====================================================================
% Fields = #{
%   <<"field1">> => Value,           - name of field
%   <<"field2">> => {fun,[Fields]}   - value as fun
% }
set(DBs,Fields,Conditions)->
  set(DBs,Fields,Conditions,#{}).
set(DBs,Fields,Conditions,Params) when is_list(Params)->
  set(DBs,Fields,Conditions,maps:from_list(Params));
set(DBs,Fields,Conditions,Params)->
  CompiledQuery=compile(set,Fields,Conditions,Params),
  Union=maps:get(union,Params,{'OR',ecomet_resultset:new()}),
  execute(execute,CompiledQuery,DBs,Union).

%%=====================================================================
%%	INSERT
%%=====================================================================
% Fields the same format as for 'set',
% Params = #{
%   update => true|false,
%   lock  => none|read|write,
%   format => fun/2
% }
insert(Fields,Params) when is_list(Params)->
  insert(Fields,maps:from_list(Params));
insert(#{<<".folder">>:=Folder,<<".name">>:=Name }=Fields, #{update:=true}=Params)->
  % If update is allowed then try to find and update the object
  {Folder1, Name1 }=
    case Params of
      #{format:=Formatter}->
        { Formatter(link,Folder), Formatter(string,Name) };
      _->
        { Folder, Name }
    end,
  case ecomet_folder:find_object_system(Folder1,Name1) of
    {ok,OID}->
      % This is an update
      Object = ecomet:open(OID,maps:get(lock,Params,none)),
      ok = ecomet:edit_object(Object,Fields,Params),
      OID;
    _->
      % Create a new object
      insert(Fields,maps:remove(update,Params))
  end;
insert(Fields,Params)->
  Object = ecomet:create_object(Fields,Params),
  ecomet:to_oid(Object).

%%=====================================================================
%%	DELETE
%%=====================================================================
delete(DBs,Conditions)->
  delete(DBs,Conditions,#{}).
delete(DBs,Conditions,Params) when is_list(Params)->
  delete(DBs,Conditions,Params);
delete(DBs,Conditions,Params)->
  CompiledQuery=compile(delete,none,Conditions,Params),
  Union=maps:get(union,Params,{'OR',ecomet_resultset:new()}),
  execute(execute,CompiledQuery,DBs,Union).

%%=====================================================================
%%	COMPILE
%%=====================================================================
compile(Type,Fields,Conditions)->compile(Type,Fields,Conditions,#{}).
compile(Type,Fields,Conditions,Params) when is_list(Params)->
  compile(Type,Fields,Conditions,maps:from_list(Params));
compile(Type,Fields,Conditions,Params)->
  Conditions1=set_rights(Type,Conditions),
  Conditions2=ecomet_resultset:prepare(Conditions1),
  {Map,Reduce}=compile_map_reduce(Type,Fields,Params),
  #compiled_query{
    conditions = Conditions2,
    map = Map,
    reduce = Reduce
  }.

%%=====================================================================
%%	EXECUTE
%%=====================================================================
execute(Handler,#compiled_query{conditions = Conditions,map = Map,reduce = Reduce}, DBs, Union)->
  AvailableDBs = [ DB || DB <- prepare_dbs( DBs ), ecomet_db:is_available(DB)],
  ecomet_resultset:Handler(AvailableDBs, Conditions, Map, Reduce, Union).

prepare_dbs( '*' )->
  ecomet_db:get_databases();
prepare_dbs([ Tag | Rest ]) when is_binary( Tag )->
  ecomet_db:find_by_tag( Tag ) ++ prepare_dbs( Rest );
prepare_dbs([ DB | Rest ]) when is_atom( DB )->
  [DB| prepare_dbs( Rest )];
prepare_dbs([])->
  [].

%%-------------GET------------------------------------------------
%% Search params is a map:
%%	- page => {Number,ItemsPerPage} - Paginating. Return only items for defined page
%%	- order => [{Field1,Order},{Field2,Order}] - Sorting results
%%	- group => [Field1,Field2] - Grouping results
%%	- lock => none|read|write - lock level on objects
%%-----------HIGHLY OPTIMIZED (no objects open)-------------------
%% CASE 1. Raw result requested
compile_map_reduce(get,rs,_Params)->
  {fun(RS)->RS	end,fun(Results)->lists:append(Results) end};
%% CASE 2. Only objects count needed
compile_map_reduce(get,[count],_Params)->
  Map=
    fun(RS)->
      ecomet_resultset:count(RS)
    end,
  Reduce=fun lists:sum/1,
  {Map,Reduce};
%% CASE 3. Only objects oid needed
compile_map_reduce(get,[<<".oid">>],Params)->
  Page=maps:get(page,Params,none),
  Map=fun(RS)->RS	end,
  Order=
    case maps:get(order,Params,[]) of
      [{<<".oid">>,'DESC'}|_]->foldl;
      _->foldr
    end,
  Reduce=
    if
      Page=:=none ->
        fun(Results)->ecomet_resultset:fold(fun(OID,Acc)->[OID|Acc] end,[],lists:append(Results),Order,none) end;
      true ->
        Traverse=
          if
            Order=:=foldl ->foldr;
            true -> foldl
          end,
        fun(Results)->
          {Count,Res}=ecomet_resultset:fold(fun(OID,Acc)->[OID|Acc] end,[],lists:append(Results),Traverse,Page) ,
          {Count,lists:reverse(Res)}
        end
    end,
  {Map,Reduce};
%%-----------COMMON VARIANTS. Objects are opened for reading------------
compile_map_reduce(get,Fields,Params)->
  ReadMap=read_map(Fields,maps:get(format,Params,undefined)),
  AliasMap=maps:fold(fun(ID,#field{alias=Alias},Acc)->Acc#{Alias=>ID} end,#{},ReadMap),

  % All columns to order by
  OrderAll=[{maps:get(Name,AliasMap),OrderType}||{Name,OrderType}<-maps:get(order,Params,[])],

  %---Columns to group by---
  % Put sort order if defined.
  % 'group' has priority over 'order':
  % 	1. rows are grouped,
  % 	2. groups are sorted
  % 	3. rows are sorted within groups.
  Group=
    lists:map(fun(Name)->
      ID=maps:get(Name,AliasMap),
      % Check no grouping by aggregated fields requested
      case maps:get(ID,ReadMap) of
        #field{value = #get{ aggregate=undefined}}->ok;
        #field{alias=InvalidGroup}->erlang:error({group_by_aggregated,InvalidGroup})
      end,
      SortOrder=proplists:get_value(ID,OrderAll,none),
      {ID,SortOrder}
    end,maps:get(group,Params,[])),
  % Sorting within groups
  Order=
    case {Group,maps:get(order,Params,[])} of
      % No sorting, no grouping. Results from old to new
      {[],[]}->foldr;
      % OPTIMIZED. Sorting by oid, no grouping. No real sorting performed, task is solved while traversing search results
      {[],[{<<".oid">>,'DESC'}|_]}->foldl;
      {[],[{<<".oid">>,'ASC'}|_]}->foldr;
      % Common case
      _->OrderAll--Group
    end,
  % Define type of query (aggregated/search), check no mix of plain and aggregated fields
  Aggregate=is_aggregate(maps:without([ID||{ID,_}<-Group],ReadMap)),
  % Paginating
  Page=maps:get(page,Params,none),
  % Fun to read object fields from storage, default lock is none (dirty operations)
  ReadUp=read_up(maps:get(lock,Params,none), get),

  % Build Map/Reduce plan
  {Map,Reduce}=map_reduce_plan(#{
    read=>ReadMap,
    aggregate=>Aggregate,
    group=>Group,
    order=>Order,
    page=>Page,
    read_up=>ReadUp
  }),

  GroupFields=[ID||{ID,_}<-Group],
  GroupHeader=[begin #field{alias=Alias}=maps:get(ID,ReadMap), Alias end||ID<-GroupFields],
  Header=GroupHeader++[Alias||{_,#field{alias=Alias}}<-lists:usort(maps:to_list(maps:without(GroupFields,ReadMap)))],

  ReduceFun=
    if
      Page=:=none ->fun(Result)->{Header,Reduce(Result)} end ;
      true -> fun(Result)->{Count,Rows}=Reduce(Result), {Count,{Header,Rows}} end
    end,
  {Map,ReduceFun};

compile_map_reduce(set,Fields,Params)->
  Formatter = maps:get(format,Params,undefined),
  Updates=
    [case Value of
       {Fun,ArgList} when is_function(Fun,1) and is_list(ArgList)->
         {Field,read_fun(Value,undefined)};
       _
         when is_function(Formatter,2)->
         % Formatted input
         Fun =
           fun(Object)->
            {ok,Type} = ecomet_object:field_type(maps:get(object,Object),Field),
            Formatter(Type,Value)
           end,
         {Field,#get{
           value = Fun,
           args = []
         }};
       _->
         {Field,Value}
     end || {Field,Value} <- maps:to_list(Fields) ],
  % Fun to read object fields from storage, default lock is none (check rights is performed)
  ReadUp=read_up(maps:get(lock,Params,none), set),
  ReadFields=ordsets:union([Args||{_,#get{args = Args}}<-Updates]),
  Update =
    fun(OID, Acc)->
      case ReadUp(OID,ReadFields) of
        #{object := not_exists} -> Acc;
        ObjectMap ->
          NewValues=
            [{Name,case Value of
              #get{value = Fun}->Fun(ObjectMap);
              _->Value
             end}||{Name,Value}<-Updates],
          ecomet_object:edit(maps:get(object,ObjectMap),maps:from_list(NewValues)),
          Acc + 1
      end
    end,

  Map=fun(RS)->RS	end,
  Reduce =
    fun(Results)->
      ecomet_resultset:foldl(fun(OID,Acc)->
        try Update(OID, Acc) catch
          _:Error->
            ?LOGERROR("unable to update ~p, error ~p",[OID,Error]),
            Acc
        end
      end,0,lists:append(Results))
    end,

  {Map,Reduce};

compile_map_reduce(delete,none,Params)->
  % Fun to read object fields from storage, default lock is none (check rights is performed)
  ReadUp=read_up(maps:get(lock,Params,none), delete),
  Map=fun(RS)->RS	end,
  Reduce =
    fun(Results)->
      ecomet_resultset:foldl(fun(OID,Acc)->
        try
          #{object:=Object}=ReadUp(OID,[]),
          if
            Object =/= not_exists ->
              ecomet_object:delete(Object),
              Acc+1;
            true ->
              Acc
          end
        catch
          _:Error->
            ?LOGERROR("unable to delete ~p, error ~p",[OID,Error]),
            Acc
        end
      end,0,lists:append(Results))
    end,

  {Map,Reduce}.
%%-----------------------------------------------------------------
%%	MAP/REDUCE PLANNING
%%-----------------------------------------------------------------
%%--------SEARCH QUERIES (Not aggregated)--------------------------
%% CASE 1. Simple search - no grouping, no sorting
map_reduce_plan(#{aggregate:=false,group:=[],order:=Order}=Params) when (Order=:=foldl) or (Order=:=foldr)->
  % Per row reading plan
  Read=maps:get(read,Params),
  % Fields to read from storage
  StorageFields=
    maps:fold(fun(_,#field{value = #get{args = Args}},Acc)->
      ordsets:union(Args,Acc)
    end,[],Read),
  RowFieldsFunList=
    [field_read_fun(maps:get(ID,Read))||ID<-ordsets:from_list(maps:keys(Read))],
  % Fun for reading from storage
  ReadUp=maps:get(read_up,Params),
  % Fun to build per row results
  BuildRowFun=
    fun(OID)->
      Object=ReadUp(OID,StorageFields),
      [F(Object)||F<-RowFieldsFunList]
    end,
  % Build result
  case maps:get(page,Params) of
    % NO PAGING
    none->
      MapFun=
        fun(RS)->
          ecomet_resultset:fold(fun(OID,Acc)->[BuildRowFun(OID)|Acc] end,[],RS,Order,none)
        end,
      ReduceFun=
        fun([Init|Results])->
          % Traversing results from the right to optimize ++ operator (assume that Rows < Acc ).
          % If only database requested, no ++ is performed
          lists:foldr(fun(Rows,Acc)->Rows++Acc end,Init,Results)
        end,
      {MapFun,ReduceFun};
    % PAGE. Optimized case. Local worker sends raw resultSet, manager build rows itself only for requested page
    Page->
      MapFun=fun(RS)->RS end,
      Traverse=
        if
          Order=:=foldl -> foldr;
          true -> foldl
        end,
      ReduceFun=
        fun(Results)->
          {Count,Res}=ecomet_resultset:fold(fun(OID,Acc)->[BuildRowFun(OID)|Acc] end,[],lists:append(Results),Traverse,Page),
          {Count,lists:reverse(Res)}
        end,
      {MapFun,ReduceFun}
  end;
%% CASE 2. Grouping and/or sorting
map_reduce_plan(#{aggregate:=false}=Params)->
  % Per row reading plan
  Read=maps:get(read,Params),
  % Grouping/Sorting
  Grouping=maps:get(group,Params),
  Sorting=maps:get(order,Params),
  % Paginate result
  Page=maps:get(page,Params),

  % All fields to read from storage locally. If only page requested,
  % local worker reads fields needed to group and sort rows, query manager
  % reads other fields after merging results only for objects within requested page
  GroupAndSortFields=[ID||{ID,_}<-Grouping++Sorting],

  OtherFields=lists:usort(maps:keys(Read))--GroupAndSortFields,
  LocalReadUpFields=
    if
      Page=:=none ->lists:usort(maps:keys(Read));
      true -> GroupAndSortFields
    end,
  % Fields to read from storage needed for calculation object groups and local presorting
  LocalStorageFields=
    lists:foldl(fun(ID,Acc)->
      ordsets:union(field_args(maps:get(ID,Read)),Acc)
    end,[],LocalReadUpFields),
  GroupingFunList=
    [field_read_fun(maps:get(ID,Read))||ID<-GroupAndSortFields],
  OtherFieldsFunList=
    [field_read_fun(maps:get(ID,Read))||ID<-OtherFields],
  LeafObjectFun=
    fun(Object)->[F(Object)||F<-OtherFieldsFunList]	end,
  % If only page requested local worker puts OID instead of tail of values, query manager builds real values itself
  % only for objects within requested page
  LocalLeaf=
    if
      Page=:=none->LeafObjectFun;
      true ->fun(Object)-> maps:get(<<".oid">>,Object) end
    end,
  % Fun for reading from storage
  ReadUp=maps:get(read_up,Params),
  %-------Fun to build local result--------------
  MapFun=
    fun(RS)->
      Groups=
        ecomet_resultset:fold(fun(OID,Acc)->
          Object=ReadUp(OID,LocalStorageFields),
          GroupValues=[F(Object)||F<-GroupingFunList],
          Row=LocalLeaf(Object),
          insert_to_group(GroupValues,fun(GroupAcc)->[Row|GroupAcc] end,[],Acc)
        end,#{},RS,foldr,none),
      sort_groups(length(Grouping++Sorting),Groups)
    end,
  %------REDUCE--------------------------------------
  ResultLeaf=
    if
      Page=:=none ->fun(Values)->Values end;
      true->
        OtherStorageFields=
          lists:foldl(fun(ID,Acc)->
            ordsets:union(field_args(maps:get(ID,Read)),Acc)
          end,[],OtherFields),
        fun(OID)->
          Object=ReadUp(OID,OtherStorageFields),
          LeafObjectFun(Object)
        end
    end,
  % Build final row. If sorting within group is performed, then sorting fields put in the head of the row,
  % we need to restore requested order of fields
  Positions=field_positions([ID||{ID,_}<-Sorting]++OtherFields),
  Prototype=erlang:make_tuple(length(Positions),none),

  % Traversing groups
  TraverseGroups=[{true,Order}||{_,Order}<-Grouping]++[{false,Order}||{_,Order}<-Sorting],

  MergeTrees=
    fun([InitTree|Results])->
      lists:foldr(fun(GroupsTree,Acc)->
        merge_trees(length(TraverseGroups),GroupsTree,Acc,fun(RowList,GroupAcc)->
          RowList++GroupAcc
        end)
      end,InitTree,Results)
     end,
  %-------Fun to reduce results--------------
  ReduceFun=
    case Page of
      none->
        FinalRow=
          fun(Head,Rows)->
            [reorder_fields(Head++ResultLeaf(Tail),Positions,Prototype)||Tail<-Rows]
          end,
        fun(Results)->
          grouped_resut(TraverseGroups,MergeTrees(Results),FinalRow,[])
        end;
      _ ->
        PrepareRow=
          fun(Head,Rows)->
            [{Head,OID}||OID<-Rows]
          end,
        FinalRow=
          fun(_,Rows)->
            [reorder_fields(Head++ResultLeaf(OID),Positions,Prototype)||{Head,OID}<-Rows]
          end,
        fun(Results)->
          PreparedResults=grouped_resut(TraverseGroups,MergeTrees(Results),PrepareRow,[]),
          {length(PreparedResults),final_resut(length(Grouping),groups_page(PreparedResults,Page),FinalRow,[])}
        end
    end,
  {MapFun,ReduceFun};

%%--------AGGREGATING QUERIES--------------------------
%% CASE 3. Aggregated results, no grouping
map_reduce_plan(#{group:=[]}=Params)->
  % Per row reading plan
  Read=maps:get(read,Params),
  % Fields to read from storage
  StorageFields=
    maps:fold(fun(_,#field{value = #get{args = Args}},Acc)->
      ordsets:union(Args,Acc)
    end,[],Read),

  RowFieldsFunList=
    [F#field.value#get.value||{_,F}<-ordsets:from_list(maps:to_list(Read))],
  % Fun for reading from storage
  ReadUp=maps:get(read_up,Params),
  % Fun to build per row results
  BuildRowFun=
    fun(OID)->
      Object=ReadUp(OID,StorageFields),
      [F(Object)||F<-RowFieldsFunList]
    end,
  AggregateFunList=
    [field_aggregate_fun(F)||{_,F}<-ordsets:from_list(maps:to_list(Read))],
  AppendRowFun=
    fun(Row,GroupAcc)->
      [Fun(Value,Acc)||{Fun,Value,Acc}<-lists:zip3(AggregateFunList,Row,GroupAcc)]
    end,
  %-------Fun to build local result--------------
  MapFun=
    fun(RS)->
      ecomet_resultset:foldl(fun(OID,Acc)->
        AppendRowFun(BuildRowFun(OID),Acc)
      end,[none||_<-AggregateFunList],RS)
    end,
  %------REDUCE--------------------------------------
  ReduceFun=
    fun([Init|Results])->
      lists:foldl(fun(Res,Acc)->
        AppendRowFun(Res,Acc)
      end,Init,Results)
    end,
  {MapFun,ReduceFun};

%% CASE 4. Aggregated results, grouping
map_reduce_plan(Params)->
  % Per row reading plan
  Read=maps:get(read,Params),
  % Grouping/Sorting
  Grouping=maps:get(group,Params),

  GroupFields=[ID||{ID,_}<-Grouping],
  OtherFields=lists:usort(maps:keys(Read))--GroupFields,

  % Fields to read from storage
  StorageFields=
    lists:foldl(fun(ID,Acc)->
      ordsets:union(field_args(maps:get(ID,Read)),Acc)
    end,[],GroupFields++OtherFields),

  GroupingFunList=
    [field_read_fun(maps:get(ID,Read))||ID<-GroupFields],
  OtherFieldsFunList=
    [field_read_fun(maps:get(ID,Read))||ID<-OtherFields],
  AggregateFunList=
    [field_aggregate_fun(maps:get(ID,Read))||ID<-OtherFields],

  % Fun for reading from storage
  ReadUp=maps:get(read_up,Params),
  % Fun to append row to group
  AppendRowFun=
    fun(Row,GroupAcc)->
      [Fun(Value,Acc)||{Fun,Value,Acc}<-lists:zip3(AggregateFunList,Row,GroupAcc)]
    end,
  % Fun for reading from storage
  ReadUp=maps:get(read_up,Params),

  InitGroup=[none||_<-AggregateFunList],
  %-------Fun to build local result--------------
  MapFun=
    fun(RS)->
      Groups=
        ecomet_resultset:foldl(fun(OID,Acc)->
          Object=ReadUp(OID,StorageFields),
          GroupValues=[F(Object)||F<-GroupingFunList],
          Row=[F(Object)||F<-OtherFieldsFunList],
          insert_to_group(GroupValues,fun(GroupAcc)->AppendRowFun(Row,GroupAcc) end,InitGroup,Acc)
        end,#{},RS),
      sort_groups(length(Grouping),Groups)
    end,
  %------REDUCE--------------------------------------
  % Traversing groups
  TraverseGroups=[{true,Order}||{_,Order}<-Grouping],
  LeafSorting=
    case {maps:get(order,Params),lists:last(Grouping)} of
      % No sorting by aggregates
      {[],{_,OrderType}}->OrderType;
      % Leaf level already sorted. TODO reorder leaf if sorting by aggregates has priority over leaf group
      {_,{_,OrderType}} when OrderType=/=none->OrderType;
      % Sorting by aggregates
      {Sorting,_}->
        SortList=
          [case orddict:find(ID,Sorting) of
             {ok,OrderType}->OrderType;
             error->none
           end||ID<-OtherFields],
        fun({_,Row1},{_,Row2})->
          compare_rows(SortList,Row1,Row2)
        end
    end,
  FinalRow=fun(_Head,Row)->Row end,
  %-------Fun to reduce results--------------
  Page=maps:get(page,Params),
  ReduceFun=
    fun([InitTree|Results])->
      % Traversing results from the right to optimize ++ operator (assume that RowList < GroupAcc )
      MergedGroups=
        lists:foldr(fun(GroupsTree,Acc)->
          merge_trees(length(TraverseGroups),GroupsTree,Acc,AppendRowFun)
        end,InitTree,Results),
      SortedGroups=sort_leafs(length(Grouping)-1,MergedGroups,LeafSorting),
      if
        Page=:=none -> grouped_resut(lists:droplast(TraverseGroups),SortedGroups,FinalRow,[]);
        true ->
          PreparedResults=grouped_resut(lists:droplast(TraverseGroups),SortedGroups,FinalRow,[]),
          {length(PreparedResults),groups_page(PreparedResults,Page)}
      end
    end,
  {MapFun,ReduceFun}.

%%====================UTILITIES================================
read_map(Fields,Formatter)->
  read_map(lists:zip(lists:seq(1,length(Fields)),Fields),Formatter,#{}).
read_map([{I,Field}|Rest],Formatter,Acc)->
  {Alias,Value}=get_alias(Field,I),
  Get=read_fun(Value,Formatter),
  read_map(Rest,Formatter,Acc#{I=>#field{
    alias = Alias,
    value = Get
  }});
read_map([],_Formatter,Acc)->Acc.

get_alias(Field,I)->
  case Field of
    {Alias,Value} when is_binary(Alias)->{Alias,Value};
    FieldName when is_binary(FieldName)->{FieldName,FieldName};
    _->{integer_to_binary(I),Field}
  end.

read_fun(count,_Formatter)->
  #get{
    value = fun(_Object)->1 end,
    args = [],
    aggregate = fun(Value,Acc)->
      if
        Acc=:=none ->Value ;
        true->Acc+Value
      end
    end
  };
read_fun('*',Formatter)->
  Read =
    if
      is_function(Formatter,2) ->
        fun(#{object:=Object})->ecomet_object:read_all(Object,#{format=>Formatter}) end;
      true ->
        fun(#{object:=Object})->ecomet_object:read_all(Object) end
    end,

  #get{
    value = Read,
    args = []
  };
read_fun({Aggregate,Value},_Formatter) when (Aggregate=:=sum) or (Aggregate=:=max) or (Aggregate=:=min)->
  Get=read_fun(Value,undefined),
  Fun=
    case Aggregate of
      sum->fun(V,Acc)->Acc+V end;
      max->fun(V,Acc)->if V>Acc->V; true->Acc end end;
      min->fun(V,Acc)->if V<Acc->V; true->Acc end end
    end,
  AggregateFun=
    fun(V,Acc)->
      if
        V=:=none->Acc;
        true->
          if
            Acc=:=none->V;
            true->Fun(V,Acc)
          end
      end
    end,
  Get#get{ aggregate= AggregateFun};
read_fun({Fun,Arguments},_Formatter) when is_function(Fun,1)->
  ArgumentList=[read_fun(Arg,undefined)||Arg<-Arguments],
  #get{
    value = fun(Object)->Fun([ArgFun(Object)||#get{value=ArgFun}<-ArgumentList]) end,
    args = lists:foldl(fun(#get{args=Args},Acc)->ordsets:union(Args,Acc) end,[],ArgumentList)
  };
read_fun(FieldName,Formatter) when is_binary(FieldName)->
  GetValue =
    fun(Fields)->
      case Fields of
        #{FieldName:=Value}->Value;
        #{object:=Object} when FieldName=:=<<".path">>->
          ecomet:to_path(Object);
        #{object:=Object}->
          case ecomet_object:field_type(Object,FieldName) of
            {ok,_}->none;
            _->undefined_field
          end
      end
    end,
  Fun =
    if
      is_function(Formatter,2) ->
        fun(Object)->
          case GetValue(Object) of
            undefined_field->
              Formatter(string,undefined_field);
            Value->
              {ok,Type}=ecomet_object:field_type(maps:get(object,Object),FieldName),
              Formatter(Type,Value)
          end
        end;
      true ->
        GetValue
    end,
  #get{
    value = Fun,
    args = [FieldName]
  };
read_fun(Field,_Formatter)->erlang:error({invalid_query_field,Field}).


field_read_fun(#field{value = Get})->Get#get.value.
field_args(#field{value = Get})->Get#get.args.
field_aggregate_fun(#field{value = Get})->Get#get.aggregate.

is_aggregate(Map)->
  [F|Tail]=[Field||{_,Field}<-maps:to_list(Map)],
  is_aggregate(Tail,F#field.value#get.aggregate=/=undefined).
is_aggregate([Field|Rest],Result)->
  if
    (Field#field.value#get.aggregate=/=undefined)=:=Result->is_aggregate(Rest,Result);
    true->erlang:error(mix_aggegated_and_plain_fields)
  end;
is_aggregate([],Result)->Result.

read_up(none, get)->
  % Optimized for reading case
  fun(OID,Fields)->
    Object=ecomet_object:construct(OID),
    query_object( Object, ecomet_object:read_fields(Object,Fields) )
  end;
read_up(Lock, _Any)->
  fun(OID,Fields)->
    try
      Object = ecomet_object:open(OID,Lock),
      query_object( Object, ecomet_object:read_fields(Object,Fields) )
    catch
      _:not_exists->
        NoneValues = maps:from_list([{F,none}||F<-Fields]),
        NoneValues#{ object => not_exists, <<".oid">> => OID }
    end
  end.

query_object( Object, Fields )->
  Fields#{
    <<".oid">> => ecomet_object:get_oid( Object ),
    object => Object
  }.

%%--------Local presorting----------------------------------
% Tree level is sorted list of tuples - { Key, ItemList }.
% ItemList can be subtree.
%-----------------------------------------------------------
insert_to_group([LeafGroup],InsertFun,Init,Acc)->
  GroupAcc=maps:get(LeafGroup,Acc,Init),
  Acc#{LeafGroup=>InsertFun(GroupAcc)};
insert_to_group([Group|Rest],InsertFun,Init,Acc)->
  Branch=maps:get(Group,Acc,#{}),
  Acc#{Group=>insert_to_group(Rest,InsertFun,Init,Branch)}.

sort_groups(L,Tree) when L>0->
  lists:usort([{K,sort_groups(L-1,V)}||{K,V}<-maps:to_list(Tree)]);
% Leaf group
sort_groups(_L,Leaf)->Leaf.

%---------Merging groups----------------------------
% Not sorted level, Tree is map
merge_trees(L,Tree1,Tree2,Fun) when L>0->
  Merge=
    fun(SubTree1,SubTree2)->merge_trees(L-1,SubTree1,SubTree2,Fun) end,
  union_groups(Tree1,Tree2,Merge);
merge_trees(_L,Leaf1,Leaf2,Fun)->Fun(Leaf1,Leaf2).

% Algorithm from ordsets with merging
union_groups([{K1,V1}|Rest1],[{K2,_}|_]=Set2,Merge) when K1 < K2 ->
  [{K1,V1}|union_groups(Rest1,Set2,Merge)];
union_groups([{K1,_}|_]=Set1, [{K2,V2}|Rest2],Merge) when K1 > K2 ->
  [{K2,V2}|union_groups(Rest2,Set1,Merge)];
% K1 == K2. Merge subtrees
union_groups([{K,V1}|Rest1],[{_,V2}|Rest2],Merge)->
  [{K,Merge(V1,V2)}|union_groups(Rest1,Rest2,Merge)];
union_groups([], Rest2,_) -> Rest2;
union_groups(Rest1,[],_) -> Rest1.

field_positions(FieldOrder)->
  Map=lists:zip(lists:usort(FieldOrder),lists:seq(1,length(FieldOrder))),
  [proplists:get_value(ID,Map)||ID<-FieldOrder].

reorder_fields([V|RestV],[P|RestP],Acc)->
  reorder_fields(RestV,RestP,setelement(P,Acc,V));
reorder_fields([],[],Acc)->
  tuple_to_list(Acc).

%-----------Output grouped results-----------------------
% Traversing in descending order
grouped_resut([{Type,'DESC'}|Rest],Tree,BuildRow,Head)->
  grouped_resut([{Type,'ASC'}|Rest],lists:reverse(Tree),BuildRow,Head);
% Group level
grouped_resut([{true,_}|Rest],Tree,BuildRow,Head)->
  [[Key,grouped_resut(Rest,Branch,BuildRow,Head)]||{Key,Branch}<-Tree];
% Sorted level
grouped_resut([{false,_}|Rest],Tree,BuildRow,Head)->
  lists:append([grouped_resut(Rest,Branch,BuildRow,Head++[Key])||{Key,Branch}<-Tree]);
% Leaf level
grouped_resut([],Rows,BuildRow,Head)->
  BuildRow(Head,Rows).

groups_page(Tree,{Number,Count})->
  Skip=(Number-1)*Count,
  Length=length(Tree),
  if
    Length=<Skip ->[] ;
    true->lists:sublist(Tree,Skip+1,Count)
  end.

% Group level
final_resut(L,Tree,BuildRow,Head) when L>0->
  [[Key,final_resut(L-1,Branch,BuildRow,Head)]||[Key,Branch]<-Tree];
% Leaf level
final_resut(_L,Rows,BuildRow,Head)->
  BuildRow(Head,Rows).


compare_rows([none|Rest],[_|R1],[_|R2])->
  compare_rows(Rest,R1,R2);
compare_rows([_|Rest],[E1|R1],[E2|R2]) when E1==E2->
  compare_rows(Rest,R1,R2);
compare_rows([Order|_],[E1|_],[E2|_])->
  if
    Order=:='DESC'->E1>E2;
    true->E1=<E2
  end;
compare_rows([],[],[])->true.

sort_leafs(L,Tree,Fun) when L>0->
  [{Key,sort_leafs(L-1,Branch,Fun)}||{Key,Branch}<-Tree];
sort_leafs(L,Leaf,Fun) when is_function(Fun)->
  sort_leafs(L,lists:sort(Fun,Leaf),'ASC');
sort_leafs(L,Rows,'DESC')->
  sort_leafs(L,lists:reverse(Rows),'ASC');
sort_leafs(_L,Rows,_Order)->
  [[Head|Tail]||{Head,Tail}<-Rows].

%% ====================================================================
%% PARSER
%% ====================================================================
compile_function(Arguments)->
  case collect_deps(Arguments) of
    []->
      % This is a constant
      Arguments;
    Deps->
      % The list contains context dependent items
      Map = values_map(Deps),
      Fun =
        fun(Values)->
          ValueMap = Map(Values),
          [arg_value(A,ValueMap)||A<-Arguments]
        end,
      { Fun, Deps }
  end.

%---------------Aggregate functions---------------------
compile_function(ecomet_ql_util,count,[])->
  count;
compile_function(ecomet_ql_util,count,_)->
  ?ERROR(wrong_count_arguments);
compile_function(ecomet_ql_util,min,[Value])->
  {min,Value};
compile_function(ecomet_ql_util,min,_)->
  ?ERROR(wrong_min_arguments);
compile_function(ecomet_ql_util,max,[Value])->
  {max,Value};
compile_function(ecomet_ql_util,max,_)->
  ?ERROR(wrong_max_arguments);
compile_function(ecomet_ql_util,sum,[Value])->
  {sum,Value};
compile_function(ecomet_ql_util,sum,_)->
  ?ERROR(wrong_sum_arguments);

% Join
compile_function(ecomet_ql_util,join,Arguments)->
  case collect_deps(Arguments) of
    []->none;
    Deps ->
      Map = values_map(Deps),
      Fun =
        fun(Values)->
          ValueMap = Map(Values),

          % Evaluate the arguments
          ArgValues=[arg_value(A,ValueMap)||A<-Arguments],

          ecomet_ql_util:join(ArgValues)
        end,
      { Fun, Deps }
  end;

%---------------Utility functions---------------------
compile_function(Module,Function,Arguments)->
  case collect_deps(Arguments) of
    []->
      % This is the constant actually
      apply(Module,Function,Arguments);
    Deps ->
      Map = values_map(Deps),
      Fun =
        fun(Values)->
          ValueMap = Map(Values),

          % Evaluate the arguments
          ArgValues=[arg_value(A,ValueMap)||A<-Arguments],

          % Evaluate the function
          apply(Module,Function,ArgValues)
        end,
      { Fun, Deps }
  end.

values_map(Deps)->
  NameMap=maps:from_list(lists:zip( lists:seq(1,length(Deps)), Deps )) ,
  fun(Values)->
    % Build dependencies name map
    Values1=lists:zip( lists:seq(1,length(Values)), Values ),
    lists:foldl(fun({I,V},Acc)->
      Acc#{maps:get(I,NameMap)=>V}
    end,#{},Values1)
  end.

collect_deps(Arguments)->
  collect_deps(Arguments,#{}).
collect_deps([{F,Args}|Rest],Acc)
  when is_function(F,1),is_list(Args)->
  Acc1 = maps:merge(Acc,maps:from_list([{V,1}||V<-Args])),
  collect_deps(Rest,Acc1);
collect_deps([List|Rest],Acc) when is_list(List)->
  Acc1=
    lists:foldl(fun(V,InAcc)->
      InAcc#{V=>1}
    end,Acc,collect_deps(List)),
  collect_deps(Rest,Acc1);
collect_deps([_Const|Rest],Acc)->
  collect_deps(Rest,Acc);
collect_deps([],Acc)->
  [V||{V,_}<-ordsets:from_list(maps:to_list(Acc))].

arg_value({F,Args},Values)->
  ArgValues = [ maps:get(A,Values) || A<-Args ],
  F(ArgValues);
arg_value(List,Values) when is_list(List)->
  [arg_value(I,Values)||I<-List];
arg_value(Const,_Values)->
  Const.

wrap_transactions(Statements)->
  wrap_transactions(Statements,0,[]).
wrap_transactions([transaction_start|Rest],Level,Acc)->
  case wrap_transactions(Rest,Level+1,[]) of
    {TransactionStatements,Tail}->
      Acc1=
        if
          length(TransactionStatements)>0 -> [{transaction,TransactionStatements}|Acc];
          true -> Acc
        end,
      wrap_transactions(Tail,Level,Acc1);
    Tail->lists:reverse(Acc)++[transaction_start|Tail]
  end;
wrap_transactions([End|Rest],Level,Acc)
  when End=:=transaction_commit; End=:=transaction_rollback->
  if
    Level>0 ->
      if
        End=:=transaction_commit-> {lists:reverse(Acc),Rest};
        true ->
          % What is the sense to start transaction if you know you roll it back?
          ?ERROR(rollback_internal_transaction)
      end;
    true -> wrap_transactions(Rest,Level,[End|Acc])
  end;
wrap_transactions([S|Rest],Level,Acc)->
  wrap_transactions(Rest,Level,[S|Acc]);
wrap_transactions([],_Level,Acc)->lists:reverse(Acc).

%% ====================================================================
%% Internal helpers
%% ====================================================================
set_rights(system,Conditions)->
  Conditions;
set_rights(Type,Conditions)->
  case ecomet_user:is_admin() of
    {ok,true}->Conditions;
    {error,Error}->?ERROR(Error);
    _->
      {ok,UserGroups}=ecomet_user:get_usergroups(),
      Level=
        if
          Type=:=get -> <<".readgroups">>;
          true -> <<".writegroups">>
        end,
      {'AND',[
        Conditions,
        {'OR',[{Level,'=',GID}||GID<-UserGroups]}
      ]}
  end.
