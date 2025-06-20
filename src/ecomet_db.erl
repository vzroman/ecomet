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
-module(ecomet_db).

-include("ecomet.hrl").

%%=================================================================
%%	ZAYA API
%%=================================================================
-record(key,{type,storage,key}).

%%	SERVICE API
-export([
  create/1,
  open/1,
  close/1,
  remove/1
]).

%%	LOW_LEVEL API
-export([
  read/2,
  write/2,
  delete/2
]).

%%	ITERATOR API
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%	HIGH-LEVEL API
-export([
  find/2,
  foldl/4,
  foldr/4
]).

%%	COPY API
-export([
  copy/3,
  dump_batch/2
]).

%%	TRANSACTION API
-export([
  commit/3,
  commit1/3,
  commit2/2,
  rollback/2
]).

%%	INFO API
-export([
  get_size/1
]).

%%=================================================================
%%	ECOMET API
%%=================================================================
-export([
  read/4, read/5, bulk_read/4, bulk_read/5,
  write/5, write/6, bulk_write/4, bulk_write/5,
  delete/4, delete/5, bulk_delete/4, bulk_delete/5,
  transaction/1
]).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  init/0,
  wait_local_dbs/0,
  wait_dbs/1,
  is_local/1,
  available_nodes/1,
  is_available/1,
  get_databases/0,
  get_name/1,
  get_by_name/1,
  find_by_tag/1,
  sync/0
]).

%%===========================================================================
%% Ecomet object behaviour
%%===========================================================================
-export([
  on_create/1,
  on_edit/1,
  on_delete/1
]).

%%===========================================================================
%% This functions we need to export ONLY for testing them
%%===========================================================================
-ifdef(TEST).
-export([
  check_id/1,
  check_name/1
]).
-endif.

%%=================================================================
%%	INIT
%%=================================================================
init()->
  case zaya:start() of
    {ok,_}->ok;
    {error,{already_started,_}}->ok;
    ignore -> ok;
    {error,Error}->
      ?LOGERROR("backend init error ~p, close the application fix the error and try to start again",[Error]),
      timer:sleep(infinity)
  end,
  wait_local_dbs().

wait_local_dbs()->
  LocalDBs = zaya:node_dbs( node() ),
  ReadyDBs =
    [DB || DB <- LocalDBs, is_available(DB, node())],
  case LocalDBs -- ReadyDBs of
    []-> ok;
    NotReady->
      ?LOGINFO("~p databases are not ready yet, waiting...",[NotReady]),
      timer:sleep( 5000 ),
      wait_local_dbs()

  end.

is_available(DB, Node)->
  case zaya:db_available_nodes(DB) of
    Ns when is_list(Ns) -> lists:member(Node, Ns);
    _-> false
  end.

wait_dbs( DBs )->
  ReadyDBs =
    [DB || DB <- DBs, zaya:is_db_available(DB)],
  case DBs -- ReadyDBs of
    []->ok;
    NotReady->
      ?LOGINFO("~p databases are not ready yet, waiting...",[NotReady]),
      timer:sleep(5000),
      wait_dbs( DBs )
  end.

%%=================================================================
%%	ZAYA
%%=================================================================
%%	SERVICE
create( Params )->
  TypesParams = maps:with( ?STORAGE_TYPES, Params ),
  OtherParams = maps:without(?STORAGE_TYPES, Params),
  maps:fold(fun(T, #{ module := M, params := Ps }, Acc)->
    try
      TypeRef = M:create( type_params(T, Ps, OtherParams) ),
      Acc#{ T => {M,TypeRef}}
    catch
      _:E->
        ?LOGERROR("~p type database create error ~p",[T,E]),
        maps:map(fun(_T,{_M,_Ref})->
          try
            _M:close( _Ref ),
            _M:remove( type_params(_T, maps:get(_T,TypesParams), OtherParams) )
          catch
            _:_E-> ?LOGERROR("~p type database rollback create error ~p",[_T,_E])
          end
        end, Acc),
        throw(E)
    end
  end,#{}, TypesParams ).

open( Params )->
  TypesParams = maps:with( ?STORAGE_TYPES, Params ),
  OtherParams = maps:without(?STORAGE_TYPES, Params),
  maps:fold(fun(T, #{ module := M, params := Ps }, Acc)->
    try
      TypeRef = M:open( type_params(T, Ps, OtherParams) ),
      Acc#{ T => {M, TypeRef} }
    catch
      _:E->
        ?LOGERROR("~p type database open error ~p",[T,E]),
        maps:map(fun(_T,{_M,_Ref})->
          try
            _M:close( _Ref )
          catch
            _:_E-> ?LOGERROR("~p type database rollback open error ~p",[_T,_E])
          end
        end, Acc),
        throw(E)
    end
  end,#{}, TypesParams ).

type_params(Type, Params, #{dir := Dir} = OtherParams )->
  maps:merge( OtherParams#{ dir => Dir ++ "/" ++ atom_to_list(Type) }, maps:without([dir],Params) ).

close( Ref )->
  case maps:fold(fun(_Type,{Module, TRef},Errs)->
    try
      Module:close( TRef ),
      Errs
    catch
      _:E->[E|Errs]
    end
  end,[], Ref ) of
    []->ok;
    Errors->
      throw(Errors)
  end.

remove( Params )->
  TypesParams = maps:with( ?STORAGE_TYPES, Params ),
  OtherParams = maps:without(?STORAGE_TYPES, Params),
  case maps:fold(fun(T,#{ module := M, params := Ps }, Errs)->
    try
      M:remove( type_params(T, Ps, OtherParams) ),
      Errs
    catch
      _:E->[E|Errs]
    end
  end,[], TypesParams ) of
    []->ok;
    Errors->
      throw(Errors)
  end.

%%	LOW_LEVEL
read( Ref, [#key{type = T,storage = S,key = K}=Key|Rest])->
  case Ref of
    #{ T := {Module, TRef} }->
      case try Module:read(TRef, [{S,[K]}]) catch _:_->error end of
        [{_,V}]->
          [{Key,V}| read(Ref, Rest ) ];
        _->
          read(Ref, Rest)
      end;
    _->
      read( Ref, Rest )
  end;
read( Ref, [_InvalidKey| Rest] )->
  read( Ref, Rest );
read(_Ref,[])->
  [].

write(Ref, KVs)->

  % Not transactional writes only used by zaya copy engine
  % there is no need in to cross module two phase commit
  % because is something goes wrong the copy will be destroyed and restarted

  {Data, IndexLog} = prepare_write( KVs ),

  commit( Ref, Data, _Delete = #{}, IndexLog ).


delete(Ref, Keys)->

  % Not transactional deletes only used by zaya copy engine
  % there is no need in to cross module two phase commit
  % because is something goes wrong the copy will be destroyed and restarted

  Delete = prepare_delete( Keys ),

  commit( Ref, _Data = #{}, Delete, _IndexLog = #{} ).


%%	ITERATOR
first( Ref )->
  Types = lists:usort(maps:keys( Ref )),
  first(Types, Ref).
first([T|Rest], Ref)->
  #{T := {Module, TRef}} = Ref,
  try
    {{S,[Key]}, V} = Module:first(TRef),
    {#key{type = T, storage = S, key = Key }, V}
  catch
    _:_->first( Rest, Ref )
  end;
first([], _Ref)->
  undefined.

last( Ref )->
  Types = lists:usort(maps:keys( Ref )),
  last(lists:reverse(Types), Ref).
last([T|Rest], Ref)->
  #{T := {Module, TRef}} = Ref,
  try
    {{S,[Key]}, V} = Module:last(TRef),
    {#key{type = T, storage = S, key = Key }, V}
  catch
    _:_->last( Rest, Ref )
  end;
last([], _Ref)->
  undefined.

next( Ref, #key{type = T, storage = S, key = K}=Key)->
  case Ref of
    #{T := {Module, TRef}}->
      case Module:next(TRef,{S,[K]}) of
        {{S,[Next]}, V}->
          {Key#key{ key = Next }, V};
        _->
          first( maps:filter(fun(Type,_)-> Type > T end, Ref) )
      end;
    _->
      throw(invalid_type)
  end.

prev( Ref, #key{type = T, storage = S, key = K}=Key)->
  case Ref of
    #{T := {Module, TRef}}->
      case Module:prev(TRef,{S,[K]}) of
        {{S,[Prev]}, V}->
          {Key#key{ key = Prev }, V};
        _->
          last( maps:filter(fun(Type,_)-> Type < T end, Ref) )
      end;
    _->
      throw(invalid_type)
  end.

%%	HIGH-LEVEL
%----------------------FIND------------------------------------------
find( Ref, InQuery )->
  {Types, Query} = query_types( InQuery, Ref),
  find( Types, Ref, Query, [] ).
find([T|Rest], Ref, Query, Acc)->
  #{T := {Module, TRef}} = Ref,
  TypeResult = [{ #key{type = T, storage = S, key = K}, V } || {{S,[K]}, V} <- Module:find( TRef, Query )],
  find(Rest, Ref, Query, [TypeResult|Acc]);
find([], _Ref, _Query, Acc)->
  lists:append( lists:reverse(Acc) ).

%----------------------FOLD LEFT------------------------------------------
foldl( Ref, InQuery, Fun, InAcc )->
  {Types, Query} = query_types( InQuery, Ref),
  foldl(Types, Ref, Query, Fun, InAcc).
foldl([T|Rest], Ref, Query, InFun, InAcc )->
  #{T := {Module, TRef}} = Ref,
  Fun =
    fun({{S,[K]},V}, Acc)->
      InFun({#key{type = T, storage = S, key = K}, V}, Acc)
    end,
  Acc = Module:foldl(TRef, Query, Fun, InAcc),
  foldl(Rest, Ref, Query, InFun, Acc);
foldl([], _Ref, _Query, _Fun, Acc )->
  Acc.


%----------------------FOLD RIGHT------------------------------------------
foldr( Ref, InQuery, Fun, InAcc )->
  {Types, Query} = query_types( InQuery, Ref),
  foldr(lists:reverse(Types), Ref, Query, Fun, InAcc).
foldr([T|Rest], Ref, Query, InFun, InAcc )->
  #{T := {Module, TRef}} = Ref,
  Fun =
    fun({{S,[K]},V}, Acc)->
      InFun({#key{type = T, storage = S, key = K}, V}, Acc)
    end,
  Acc = Module:foldr(TRef, Query, Fun, InAcc),
  foldr(Rest, Ref, Query, InFun, Acc);
foldr([], _Ref, _Query, _Fun, Acc )->
  Acc.

query_types( #{types := QueryTypes} = Query, Ref)->
  Types = lists:usort([T || T <- QueryTypes, maps:is_key(T, Ref)]),
  {Types, maps:remove(types, Query)};
query_types(Query, Ref)->
  Types = lists:usort(maps:keys( Ref )),
  {Types, Query}.

%%	COPY
copy(Ref, Fun, InAcc)->
  foldl(Ref, #{}, Fun, InAcc).

dump_batch(Ref, KVs)->
  write(Ref, KVs).

%%	TRANSACTION
%-------------Commit to a single storage
commit(Ref, KVs, Keys) when map_size( Ref ) =:= 1->
  {Data, IndexLog} = prepare_write( KVs ),
  Delete = prepare_delete( Keys ),
  commit( Ref, Data, Delete, IndexLog );

commit(Ref, KVs, Keys)->

  {Data, IndexLog} = prepare_write( KVs ),
  Delete = prepare_delete( Keys ),
  case needs_log( Data, Delete ) of
    false -> commit( Ref, Data, Delete, IndexLog );
    true -> two_phase_commit( Ref, Data, Delete, IndexLog )
  end.

commit1(_Ref, KVs, Keys)->
  {KVs, Keys}.

commit2(Ref, {KVs, Keys})->
  commit( Ref, KVs, Keys ).

rollback( _Ref, _TRef )->
  ok.

commit(Ref, Data, Delete, IndexLog)->

  Storages = get_commit_storages( Data, Delete ),
  case Storages -- maps:keys( Ref ) of
    [] -> ok;
    Invalid ->
      %% ATTENTION! If the user tries to save object with storage type
      %% that is not in the DB then it will crash here
      ?LOGERROR("attempt to save to not configured storage types: ~p",[ Invalid ]),
      throw({ invalid_storage_type, Invalid })
  end,

  % Order commit the heavier types go first
  CommitOrder = [ ramdisc, disc, ram ],
  Ordered = CommitOrder -- ( CommitOrder -- Storages ),

  [ begin
      { Module, TRef } = maps:get(T, Ref),
      TData = maps:get( T, Data, none ),
      TDelete = maps:get( T, Delete, none ),
      TIndexLog = maps:get( T, IndexLog, none ),
      commit( TRef, Module, TData, TDelete, TIndexLog )
    end || T <- Ordered],

  ok.

%%-----------------Only write commit (no index, no delete)----------------------------------
commit(Ref, Module, Data, _Delete = none, _IndexLog = none) ->
  Module:write( Ref, Data);

%%-----------------Only delete commit (no index, no write)----------------------------------
commit(Ref, Module, _Data = none, Delete, _IndexLog = none) ->
  Module:delete( Ref, Delete);

%%-----------------Write and delete with no index----------------------------------
commit(Ref, Module, Data , Delete , _IndexLog = none) ->
  Module:commit( Ref, Data, Delete);

%%-----------------Write commit with index (no delete)----------------------------------
commit(Ref, Module, Data, _Delete = none, IndexLog) ->

  {ok, Unlock} = elock:lock(?LOCKS, Ref, _IsShared = false, _Timeout = infinity),
  try
    case ecomet_index:prepare_write(Module, Ref, IndexLog ) of
      { IndexWrite, IndexDel } when length( IndexDel ) > 0 ->
        Module:commit( Ref, Data ++ IndexWrite, IndexDel);
      { IndexWrite, _IndexDel }->
        Module:write( Ref, Data ++ IndexWrite)
    end
  after
    Unlock()
  end;

%%-----------------Delete commit with index (no write)----------------------------------
commit(Ref, Module, _Data = none, Delete, IndexLog) ->

  {ok, Unlock} = elock:lock(?LOCKS, Ref, _IsShared = false, _Timeout = infinity),
  try
    case ecomet_index:prepare_write(Module, Ref, IndexLog ) of
      { IndexWrite, IndexDel } when length( IndexWrite ) > 0 ->
        Module:commit( Ref, IndexWrite, Delete ++ IndexDel);
      { _IndexWrite, IndexDel }->
        Module:delete( Ref, Delete ++ IndexDel)
    end
  after
    Unlock()
  end;

commit(Ref, Module, Data, Delete, IndexLog)->
  {ok, Unlock} = elock:lock(?LOCKS, Ref, _IsShared = false, _Timeout = infinity),

  try
    { IndexWrite, IndexDel } = ecomet_index:prepare_write(Module, Ref, IndexLog ),
    if
      length( IndexDel ) =:= 0->
        Module:commit( Ref, Data ++ IndexWrite, Delete);
      length( IndexWrite ) =:= 0->
        Module:commit( Ref, Data, Delete ++ IndexDel);
      true ->
        Module:commit( Ref, Data ++ IndexWrite, Delete ++ IndexDel)
    end
  after
    Unlock()
  end.

two_phase_commit( Ref, Data, Delete, IndexLog )->
  % TODO
  commit( Ref, Data, Delete, IndexLog ).

prepare_write( Write )->
  lists:foldl(fun( { #key{ type = T, storage = S, key = K }, V}, {DAcc, IAcc})->
    if
      S =:= ?INDEX, is_boolean( V )->
        % The trick.
        % If the value of the index is a boolean it's an index update as a result of commit
        % but not real value. This write must be done by ecomet_index module
        TIAcc = maps:get( T, IAcc, [] ),
        { DAcc, IAcc#{ T => [{K,V}|TIAcc] } };
      true ->
        TDAcc = maps:get( T, DAcc, [] ),
        { DAcc#{ T => [{{S,[K]}, V}|TDAcc] } , IAcc}
    end
  end, { #{}, #{} }, Write ).

prepare_delete( Delete )->
  lists:foldl(fun(#key{ type = T, storage = S, key = K }, Acc)->
    TypeAcc = maps:get(T,Acc,[]),
    Acc#{ T => [{S,[K]} | TypeAcc]}
  end,#{}, Delete).

needs_log( Data, Delete )->
  Storages = get_commit_storages( Data, Delete ),
  lists:member( disc, Storages ) andalso lists:member( ramdisc, Storages ).

get_commit_storages( Data, Delete )->
  lists:usort( maps:keys( Data ) ++ maps:keys( Delete )).

%%=================================================================
%%	INFO
%%=================================================================
get_size( Ref )->
  maps:map(fun(_Type,{Module,TRef})->
    Module:get_size( TRef )
  end, Ref).

%%================================================================
%% ECOMET
%%================================================================
read(DB, Storage, Type, Key)->
  case zaya:read(DB, [#key{ type = Type, storage = Storage, key = Key }]) of
    [{_,Value}] -> Value;
    _->not_found
  end.
read(DB, Storage, Type, Key, Lock)->
  case zaya:read(DB, [#key{ type = Type, storage = Storage, key = Key }], Lock) of
    [{_,Value}] -> Value;
    _->not_found
  end.
bulk_read(DB, Storage, Type, Keys)->
  KVs = zaya:read(DB, [#key{ type = Type, storage = Storage, key = K } || K <- Keys]),
  [{K,V} || {#key{key = K},V} <- KVs].
bulk_read(DB, Storage, Type, Keys, Lock)->
  KVs = zaya:read(DB, [#key{ type = Type, storage = Storage, key = K } || K <- Keys], Lock),
  [{K,V} || {#key{key = K},V} <- KVs].

write(DB, Storage, Type, Key, Value)->
  zaya:write(DB, [{#key{type = Type, storage = Storage, key = Key}, Value}]).
write(DB, Storage, Type, Key, Value, Lock)->
  zaya:write(DB, [{#key{type = Type, storage = Storage, key = Key}, Value}], Lock).

bulk_write(DB, Storage, Type, KVs)->
  zaya:write(DB, [{#key{type = Type, storage = Storage, key = K},V} || {K,V} <- KVs]).
bulk_write(DB, Storage, Type, KVs, Lock)->
  zaya:write(DB, [{#key{type = Type, storage = Storage, key = K},V} || {K,V} <- KVs], Lock).

delete(DB, Storage, Type, Key)->
  zaya:delete(DB, [#key{type = Type, storage = Storage, key = Key}]).
delete(DB, Storage, Type, Key, Lock)->
  zaya:delete(DB, [#key{type = Type, storage = Storage, key = Key}], Lock).

bulk_delete(DB, Storage, Type, Keys)->
  zaya:delete(DB, [#key{type = Type, storage = Storage, key = K} || K <-Keys]).
bulk_delete(DB, Storage, Type, Keys, Lock)->
  zaya:delete(DB, [#key{type = Type, storage = Storage, key = K} || K <-Keys],Lock).


transaction(Fun)->
  zaya:transaction(Fun).

%%=================================================================
%%	SERVICE API
%%=================================================================
get_name(DB)->
  {ok,Name}=ecomet:read_field(?OBJECT(DB),<<".name">>),
  binary_to_atom(Name,utf8).

is_local(DB)->
  lists:member(node(),zaya:db_available_nodes(DB)).

available_nodes( DB )->
  zaya:db_available_nodes(DB).

is_available(DB)->
  zaya:is_db_available( DB ).

get_databases()->
  ecomet_schema:get_registered_databases().

get_by_name(Name) when is_atom(Name)->
  get_by_name(atom_to_binary(Name,utf8));
get_by_name(Name) when is_binary(Name)->
  case ecomet_query:system([?ROOT],[<<".oid">>],{'AND',[
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.database">>)},
    {<<".name">>,'=',Name }
  ]}) of
    [OID]->{ok,OID};
    _->{error,not_found}
  end.

find_by_tag( Tag )->
  {_, DBs} =  ecomet_query:system([?ROOT],[<<".name">>],{'AND',[
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.database">>)},
    {<<"tags">>,'=',Tag }
  ]}),
  [ binary_to_atom( DB, utf8 ) || [DB] <- DBs ].

sync()->
  % Run through all databases
  Unlock = ecomet_schema:lock(),

  try
    RegisteredDBs = get_databases(),
    ActualDBs =
      [DB || DB <- zaya:all_dbs(), zaya:db_module( DB ) =:= ?MODULE],

    [ try update_masters( DB )
      catch
        _:E-> ?LOGERROR("~p database update masters error ~p",[DB,E])
      end|| DB <- ActualDBs -- RegisteredDBs, is_master(DB) ],

    [ try remove_database( DB )
     catch
        _:E-> ?LOGERROR("~p database remove error ~p",[DB,E])
      end|| DB <- ActualDBs -- RegisteredDBs, is_master(DB) ],

    [ try sync_copies(DB)
      catch
        _:E-> ?LOGERROR("~p database sync copies error ~p",[DB,E])
      end|| DB <- RegisteredDBs ],

    [ try update_info(DB)
     catch
        _:E->?LOGERROR("~p database update info error ~p",[DB,E])
      end|| DB <- RegisteredDBs, is_master(DB) ],

    [ try sync_read_only(DB)
      catch
        _:E->?LOGERROR("~p database to read-only error ~p",[DB,E])
      end|| DB <- RegisteredDBs, is_master(DB) ],

    ok
  after
    Unlock()
  end.

sync_read_only(DB) ->
  {ok, OID} = get_by_name( DB ),
  #{<<"read_only">> := ReadOnly} = ecomet:read_fields(?OBJECT(OID), #{<<"read_only">> => false} ),
  ActualReadOnly = zaya:db_read_only(DB),
  if
    ReadOnly =/= ActualReadOnly ->
      case zaya:db_read_only(DB, ReadOnly) of
        {[], Errors} ->
          throw({failed_switch_read_only_mode, Errors});
        _ ->
          ok
      end;
    true ->
      ok
  end.

is_master(DB)->
  Node = node(),
  Masters = zaya:db_masters( DB ),
  ReadyNodes = zaya:ready_nodes(),
  if
    length(Masters)>0->
      case Masters -- (Masters -- ReadyNodes) of
        [Node|_]->
          true;
        _->
          false
      end;
    true->
      DBNodes = lists:usort( zaya:db_all_nodes(DB) ),
      case DBNodes -- (DBNodes -- ReadyNodes) of
        [Node|_]->
          true;
        _->
          false
      end
  end.

update_masters( DB )->
  case ecomet:get([?ROOT],[<<"masters">>],{'AND',[
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.database">>)},
    {<<".name">>,'=',atom_to_binary(DB,utf8)}
  ]}) of
    {_,[[Masters0]]}->
      Masters =
        if
          is_list( Masters0 )-> Masters0;
          true -> []
        end,
      case zaya:db_masters( DB ) of
        Masters -> ok;
        _->
          zaya:db_masters( DB, Masters ),
          if
            DB =/= ?ROOT-> ok;
            true->
              zaya:db_masters( ecomet_schema, Masters )
          end
      end;
    _->
      ?LOGWARNING("~p database is registered but not found the corresponding object, skip synchronization",[DB])
  end.

remove_database( DB )->
  ?LOGINFO("~p database remove",[DB]),
  zaya:db_close( DB ),
  wait_close( DB ),
  zaya:db_remove( DB ).

wait_close( DB )->
  case zaya:db_available_nodes( DB ) of
    []->ok;
    _->
      ?LOGINFO("~p database wait close",[DB]),
      timer:sleep(5000),
      wait_close( DB )
  end.

sync_copies(DB)->
  case ecomet:get([?ROOT],[<<"params">>],{'AND',[
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.database">>)},
    {<<".name">>,'=',atom_to_binary(DB,utf8)}
  ]}) of
    {_,[[Params]]}->
      Node = node(),
      EcometParams = maps:get(Node,Params,undefined),
      ZayaParams = zaya:db_node_params(DB, Node),
      case {EcometParams,ZayaParams} of
        {Same,Same}->
          ok;
        {undefined,_}->
          zaya:db_close(DB, Node),
          wait_close(DB, Node),
          zaya:db_remove_copy(DB, Node);
        {_, undefined}->
          zaya:db_add_copy(DB, Node, EcometParams );
        {_,_}->
          zaya:db_set_copy_params(DB, Node, EcometParams )
      end;
    _->
      ?LOGWARNING("~p database is registered but not found the corresponding object, skip synchronization",[DB])
  end.

wait_close( DB, Node )->
  case lists:member(Node,zaya:db_available_nodes( DB )) of
    false->ok;
    _->
      ?LOGINFO("~p database wait close",[DB]),
      timer:sleep(5000),
      wait_close( DB, Node )
  end.

update_info(DB)->
  case get_by_name( DB ) of
    {ok, OID}->
      ok = ecomet:edit_object(ecomet:open(OID),#{
        <<"nodes">> => zaya:db_all_nodes( DB ),
        <<"available_nodes">> => zaya:db_available_nodes(DB),
        <<"not_ready_nodes">> => zaya:db_not_ready_nodes(DB),
        <<"is_available">> => zaya:is_db_available(DB),
        <<"size">> => zaya:db_size(DB)
      });
    _->
      ?LOGWARNING("~p database update info is not possible as the corresponding object is not found",[DB])
  end.

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  Name=check_name(Object),
  %<<"params">>=>#{ Node => #{ Type => Params } },

  Types = check_types(Object),
  Params = check_params(Object),

  %-------------Create a new database-------------------------
  ?LOGINFO("creating a new database ~p, params ~p",[Name,Params]),
  Unlock = ecomet_schema:lock(),
  try
    case zaya:db_create(Name,?MODULE,Params) of
      {_,[]}->
        ok;
      {_,CreateErrors}->
        ?LOGERROR("~p database create errors ~p",[Name,CreateErrors])
    end,

    % If the transaction fails the database won't be registered in ecomet schema
    % and will be removed during the synchronization
    {ok,Id} = ecomet_schema:add_db( Name ),
    check_tags( Object ),

    ecomet:edit_object(Object,#{
      <<"id">> => Id,
      <<"params">> => Params,
      <<"types">>=>maps:keys( Types )
    })
  after
    Unlock()
  end.

on_edit(Object)->
  check_name(Object),
  check_id(Object),
  check_types(Object),
  check_tags(Object),
  case check_params(Object) of
    Params when is_map(Params)->
      ok = ecomet:edit_object(Object,#{<<"params">> => Params});
    _->
      ok
  end.

on_delete(Object)->
  OID = ?OID( Object ),
  MountedFolders = ecomet_folder:find_mount_points(OID),

  Unlock = ecomet_schema:lock(),
  try
    % Unmount folders
    [ok = ecomet_schema:unmount_db(F) || F <- MountedFolders],

    % Unregister the DB
    {ok, Name} = ecomet:read_field(Object, <<".name">>),
    ok = ecomet_schema:remove_db( binary_to_atom(Name,utf8) ),

    % Cleanup folders
    ecomet:on_commit(fun()->
      [ catch ecomet:edit_object(ecomet:open(F), #{<<"database">> => none}) || F <- MountedFolders]
    end)
  after
    Unlock()
  end.

check_name(Object)->
  case ecomet:field_changes(Object,<<".name">>) of
    none->ok;
    { Name, none }->
      case re:run(Name,"^(\\w+)$") of
        {match,_}->
          binary_to_atom(Name,utf8);
        _->
          ?ERROR(invalid_name)
      end;
    {_New, _Old}->
      ?ERROR(name_is_final)
  end.

check_id(Object)->
  case ecomet:field_changes(Object,<<"id">>) of
    none->ok;
    { _New, none }->
      % This is the creation
      ok;
    {_New, _Old}->
      ?ERROR(change_id_is_not_allowed)
  end.

check_types(Object)->
  case ecomet:field_changes(Object,<<"modules">>) of
    none->ok;
    { Modules, none }->
      if
        is_map(Modules) andalso map_size(Modules)>0->
          case maps:keys(Modules) -- ?STORAGE_TYPES of
            []->
              [case is_atom(M) of true->ok; _->throw({invalid_module,M}) end || M <- maps:values(Modules)],
              Modules;
            InvalidTypes->
              throw({invalid_types,InvalidTypes})
          end;
        true->
          throw(invalid_types)
      end;
    {_New, _Old}->
      throw(types_are_final)
  end.

check_params(Object)->
  case ecomet:field_changes(Object,<<"params">>) of
    none->ok;
    { NewParams, _OldParams } when is_map(NewParams)->
      {ok,Modules}= ecomet:read_field(Object,<<"modules">>),
      params_diff( NewParams, Modules );
    _->
      throw(invalid_params)
  end.

params_diff( NewParams, Modules )->
  Types = maps:keys(Modules),
  %-------Add new nodes or change copies params---------------
  maps:fold(fun(Node,NodeParams,Acc)->
    if
      is_map(NodeParams)-> ok;
      true-> throw({invalid_node_params,Node})
    end,
    OtherParams = maps:without(?STORAGE_TYPES, NodeParams),
    NodeTypesParams =
      lists:foldl(fun(Type,TAcc)->
        Module = maps:get(Type,Modules),
        TypeParams =
          case NodeParams of
            #{Type:= #{ params:= _TypeParams }}->
              if
                is_map(_TypeParams)->
                  _TypeParams;
                true->
                  throw({invalid_node_params,Type,_TypeParams})
              end;
            _->
              #{}
          end,
        TAcc#{Type => #{module => Module, params => TypeParams} }
      end,#{},Types),
    NewNodeParams = maps:merge(OtherParams, NodeTypesParams),

    Acc#{Node => NewNodeParams}

  end, #{}, NewParams).

check_tags(Object)->
  case ecomet:field_changes(Object,<<"tags">>) of
    none->ok;
    { NewTags, _OldParams }->
      {ok, Name} = ecomet:read_field(Object, <<".name">>),
      DB = binary_to_atom(Name,utf8),
      case ecomet_schema:set_db_tags( DB, NewTags ) of
        ok -> ok;
        {error, Error} -> throw({ set_db_tags, DB, NewTags, Error })
      end
  end.