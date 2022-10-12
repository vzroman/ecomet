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

%%	INFO API
-export([
  get_size/1
]).

%%=================================================================
%%	ECOMET API
%%=================================================================
-export([
  read/4, read/5,
  write/5, write/6,
  delete/4, delete/5,
  transaction/1
]).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  init/0,
  wait_local_dbs/0,
  is_local/1,
  get_search_node/2,
  get_databases/0,
  get_name/1,
  get_by_name/1,
  storage_name/2,
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
  check_name/1,
  create_database/1,
  remove_database/1
]).
-endif.

%%=================================================================
%%	INIT
%%=================================================================
init()->
  case application:ensure_started( zaya ) of
    ok->ok;
    {error,Error}->
      ?LOGERROR("backend init error ~p, close the application fix the error and try to start again",[Error])
  end,
  wait_local_dbs().

wait_local_dbs()->
  DBs = zaya:node_dbs( node() ),
  ReadyDBs =
    [DB || DB <- DBs, lists:member(node(), zaya:db_available_nodes(DB))],
  case DBs -- ReadyDBs of
    []->ok;
    NotReady->
      ?LOGINFO("~p databases are not ready yet, waiting...",[NotReady]),
      timer:sleep(5000),
      wait_local_dbs()
  end.

%%=================================================================
%%	ZAYA
%%=================================================================
%%	SERVICE
create( Params )->
  maps:fold(fun(Type, #{ module := Module, params := TypeParams }, Acc)->
    try
      Ref = Module:create( TypeParams ),
      [{Module,Ref,TypeParams}|Acc]
    catch
      _:E->
        ?LOGERROR("~p type database create error ~p",[Type,E]),
        [try M:close(TRef), M:remove( TPs ) catch _:_->ignore end || {M,TRef,TPs} <- Acc],
        throw(E)
    end
  end,[], Params ),
  ok.

open( Params )->
  maps:fold(fun(Type, #{ module := Module, params := TypeParams }, Acc)->
    try
      Ref = Module:open( TypeParams ),
      Acc#{ Type => {Module, Ref} }
    catch
      _:E->
        ?LOGERROR("~p type database open error ~p",[Type,E]),
        [ catch M:close(TRef) || {_T,{M,TRef}} <- maps:to_list(Acc) ],
        throw(E)
    end
  end,#{}, Params ).

close( Ref )->
  case maps:fold(fun(_Type,{Module, Ref},Errs)->
    try
      Module:close( Ref ),
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
  case maps:fold(fun(_Type,#{ module := Module, params := TypeParams }, Errs)->
    try
      Module:remove( TypeParams ),
      Errs
    catch
      _:E->[E|Errs]
    end
  end,[], Params ) of
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
  ByTypes =
    lists:foldl(fun({#key{ type = T, storage = S, key = K }, V}, Acc)->
      TypeAcc = maps:get(T,Acc,[]),
      Acc#{ T => [{{S,[K]}, V} | TypeAcc]}
    end,#{}, KVs),
  maps:map(fun(Type, Recs)->
    { Module, TRef } = maps:get(Type, Ref),
    ok = Module:write( TRef, Recs )
  end, ByTypes),
  ok.

delete(Ref, Keys)->
  ByTypes =
    lists:foldl(fun(#key{ type = T, storage = S, key = K }, Acc)->
      TypeAcc = maps:get(T,Acc,[]),
      Acc#{ T => [{S,[K]} | TypeAcc]}
    end,#{}, Keys),
  maps:map(fun(Type, TKeys)->
    { Module, TRef } = maps:get(Type, Ref),
    ok = Module:delete( TRef, TKeys )
  end, ByTypes),
  ok.

%%	ITERATOR
first( _Ref )->
  throw(not_supported).

last( _Ref )->
  throw(not_supported).

next( Ref, #key{type = T, storage = S, key = K}=Key)->
  case Ref of
    #{T := {Module, TRef}}->
      case Module:next(TRef,{S,[K]}) of
        {{S,[Next]}, V}->
          {Key#key{ key = Next }, V};
        _->
          throw(undefined)
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
          throw(undefined)
      end;
    _->
      throw(invalid_type)
  end.

%%	HIGH-LEVEL
%----------------------FIND------------------------------------------
find(_Ref, _Query)->
  % TODO
  [].

%----------------------FOLD LEFT------------------------------------------
foldl( _Ref, _Query, _Fun, InAcc )->
  % TODO
  InAcc.

%----------------------FOLD RIGHT------------------------------------------
foldr( _Ref, _Query, _Fun, InAcc )->
  % TODO
  InAcc.

%%=================================================================
%%	INFO
%%=================================================================
get_size( Ref )->
  maps:fold(fun(_Type,{Module,TRef},Acc)->
    Acc + Module:get_size( TRef )
  end,0, Ref).

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

write(DB, Storage, Type, Key, Value)->
  zaya:write(DB, [{#key{type = Type, storage = Storage, key = Key}, Value}]).
write(DB, Storage, Type, Key, Value, Lock)->
  zaya:write(DB, [{#key{type = Type, storage = Storage, key = Key}, Value}], Lock).

delete(DB, Storage, Type, Key)->
  zaya:delete(DB, [#key{type = Type, storage = Storage, key = Key}]).
delete(DB, Storage, Type, Key, Lock)->
  zaya:delete(DB, [#key{type = Type, storage = Storage, key = Key}], Lock).

transaction(Fun)->
  zaya:transaction(Fun).

%%=================================================================
%%	SERVICE API
%%=================================================================
get_name(DB)->
  {ok,Name}=ecomet:read_field(?OBJECT(DB),<<".name">>),
  binary_to_atom(Name,utf8).

is_local(_Name)->
  % TODO
  true.

get_search_node(_Name,_Exclude)->
  % TODO
  node().

get_databases()->
  ecomet_schema:get_registered_databases().

get_by_name(Name) when is_atom(Name)->
  get_by_name(atom_to_binary(Name,utf8));
get_by_name(Name) when is_binary(Name)->
  case ecomet_query:system([?ROOT],[<<".oid">>],{'AND',[
    {<<".pattern">>,':=',?OID(<<"/root/.patterns/.database">>)},
    {<<".name">>,'=',Name }
  ]}) of
    [OID]->{ok,OID};
    _->{error,not_found}
  end.

sync()->
  % Run through all databases
  [ sync_database(DB) || DB <- get_databases() ].

sync_database(DB)->
  % Run through all storage types in the database where the node is the master
  [ sync_storage(DB,S,T) || S<-[?DATA,?INDEX], T<-?STORAGE_TYPES, is_master(DB,S,T) ],

  % The database master is the master of the data disc storage
  case is_master(DB,?DATA,?DISC) of
    true->
      {ok,OID}=get_by_name(DB),
      update_db(OID),
      ok;
    _->
      ok
  end.

sync_storage(DB,Storage,Type)->
  {ok,StorageID} = get_storage_oid(DB,Storage,Type),
  Configured = get_storage_segments(StorageID),
  Actual = ecomet_db:get_segments(DB,Storage,Type),

  % remove segments that does not exist any more
  case Configured--Actual of
    []->ok;
    ToRemove->
      ecomet:delete(get_databases(),{'AND',[
        {<<".folder">>,'=',StorageID},
        {'OR',[ {<<".name">>,'=',atom_to_binary(N,utf8) } || N <-ToRemove ]}
      ]})
  end,

  % add new segments
  [ ecomet:create_object(#{
    <<".name">>=>atom_to_binary(N,utf8),
    <<".folder">>=>StorageID,
    <<".pattern">>=>?OID(<<"/root/.patterns/.segment">>)
  }) || N<- Actual -- Configured],

  % Update segments metrics
  [ sync_segment(S) || S<-get_storage_segments(StorageID), is_master(S) ],

  % Update storage merics
  update_storage(StorageID),

  ok.

sync_segment(Segment)->
  {ok,OID} = get_segment_oid(Segment),
  Object = ecomet:open(OID,none),
  #{ local:= IsLocal } = ecomet_db:get_segment_info(Segment),
  if
    not IsLocal ->
      {ok, #{ copies := Copies }} = ecomet_db:get_segment_params(Segment),
      Actual = maps:keys( Copies ),
      case ecomet:read_field(Object,<<"nodes">>,#{default=>[]}) of
        {ok,[]}->
          ok = ecomet:edit_object(Object,#{<<"nodes">>=>Actual});
        {ok,Configured}->

          % Add copies of the segment to nodes
          [ ecomet_db:add_segment_copy(Segment,N) || N <- Configured -- Actual ],

          % Remove copies
          [ ecomet_db:remove_segment_copy(Segment,N) || N <- Actual -- Configured ]

      end;
    true ->
      Node = node(),
      case ecomet:read_field(Object,<<"nodes">>) of
        {ok,[Node]}->ok;
        _->
          ecomet:edit_object(Object,#{<<"nodes">>=>[Node]})
      end
  end,

  update_segment(OID),

  ok.


is_master(Segment)->
  % The master of the segment is the first ready node in the list of
  % the nodes hosting the segment
  #{nodes := RootNodes } = ecomet_db:get_segment_info(Segment),
  ReadyNodes = ecomet_node:get_ready_nodes(),
  Node = node(),

  case [N || N <- RootNodes, lists:member(N,ReadyNodes)] of
    [Node|_]->
      true;
    _->
      false
  end.

is_master(DB,Storage,Type)->
  % The master of a storage is the master of its root segment
  Root = ecomet_db:get_root_segment(DB,Storage,Type),
  is_master(Root).

update_db(OID)->
  {_, StorageTypes } = ecomet:get(get_databases(),[<<"nodes">>,<<"size">>,<<"segments_count">>,<<"root_segment">>],{'AND',[
    {<<".folder">>,'=',OID},
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.storage">>)}
  ]}),

  Update =
    lists:foldl(fun([Nodes,Size,Count,Root],#{<<"nodes">>:=AccNodes,<<"size">>:=AccSize,<<"segments_count">>:=AccCount}=Acc)->
      case ecomet_db:get_segment_info(Root) of
        #{local:=true}->
          % Local storage are not counted
          Acc;
        _->Acc#{
          <<"nodes">>=>ordsets:union(AccNodes,ordsets:from_list(Nodes)),
          <<"size">>=>if is_number(Size)->AccSize + Size; true->AccSize end,
          <<"segments_count">>=>if is_number(Count)->AccCount + Count; true->AccCount end
        }
      end
    end,#{<<"nodes">>=>[],<<"size">>=>0,<<"segments_count">>=>0},StorageTypes),

  ok = ecomet:edit_object(ecomet:open(OID,none),Update).


update_storage(OID)->
  {_, Segments } = ecomet:get(get_databases(),[<<"nodes">>,<<"size">>],{'AND',[
    {<<".folder">>,'=',OID},
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.segment">>)}
  ]}),

  Update0 =
    lists:foldl(fun([Nodes,Size],#{<<"nodes">>:=AccNodes,<<"size">>:=AccSize})->
      #{
        <<"nodes">>=>ordsets:union(AccNodes,ordsets:from_list(Nodes)),
        <<"size">>=>if is_number(Size)->AccSize + Size; true->AccSize end
      }
    end,#{<<"nodes">>=>[],<<"size">>=>0},Segments),

  Object = ecomet:open(OID,none),
  #{
    <<".name">> := Name,
    <<".folder">> := DB_OID,
    <<"limits">> := Limits
  } = ecomet:read_fields( Object, [<<".name">>,<<".folder">>,<<"limits">>]),

  [S,T] = binary:split(Name,<<"@">>),
  {ok,DB_Name} = ecomet:read_field( ecomet:open(DB_OID,none), <<".name">> ),

  DB = binary_to_atom(DB_Name,utf8),
  Storage = binary_to_atom(S,utf8),
  Type = binary_to_atom(T,utf8),

  Root = ecomet_db:get_root_segment(DB,Storage,Type),
  ActualLimits = ecomet_db:storage_limits( DB,Storage,Type ),

  Update =
    case Limits of
      ActualLimits -> Update0;
      _ when is_map( Limits )->
        % Set storage limits
        ecomet_db:storage_limits( DB,Storage,Type, Limits ),
        Update0;
      _ ->
        % Update actual limits
        Update0#{ <<"limits">> => ActualLimits }
    end,

  ok = ecomet:edit_object(Object,Update#{
    <<"root_segment">> => Root,
    <<"segments_count">> => length(Segments)
  }).


update_segment(OID)->
  Object = ecomet:open(OID,none),
  {ok,Name}=ecomet:read_field(Object,<<".name">>),
  Size = ecomet_db:get_segment_size(binary_to_atom(Name,utf8)),
  ok = ecomet:edit_object(Object,#{<<"size">>=>Size}).

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  Name=check_name(Object),

  %<<"modules">> - #{ Type => Module },
  %<<"params">>=>#{ Type => #{ Node => Params } },

  Types = check_types(Object),
  Params = check_params(Object),

  %-------------Create a new database-------------------------
  ?LOGINFO("creating a new database ~p, params ~p",[Name,Params]),
  case zaya:db_create(Name,?MODULE,Params) of
    {_,[]}->
      ok;
    {_,CreateErrors}->
      ?LOGERROR("~p database create errors ~p",[Name,CreateErrors])
  end,

  ecomet_transaction:on_abort(fun()->
    catch zaya:db_close( Name ),
    wait_close( Name ),
    catch zaya:db_remove( Name ),
    ecomet_schema:remove_db(Name)
  end),

  {ok,Id} = ecomet_schema:add_db( Name ),

  ecomet:edit_object(Object,#{
    <<"id">> => Id,
    <<"types">>=>maps:keys( Types )
  }).

on_edit(Object)->
  check_name(Object),
  check_id(Object),
  check_types(Object),
  check_params(Object),
  ok.

on_delete(Object)->
  case ecomet_folder:find_mount_points(?OID(Object)) of
    []->
      {ok,Name}=ecomet:read_field(Object,<<".name">>),
      NameAtom = binary_to_atom(Name,utf8),
      zaya:db_remove(NameAtom),
      ecomet_schema:remove_db( NameAtom ),
      ok;
    _->
      ?ERROR(is_mounted)
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
      ?ERROR(renaming_is_not_allowed)
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

wait_close( DB )->
  case zaya:db_available_nodes( DB ) of
    []->ok;
    _->
      ?LOGINFO("~p database wait close",[DB]),
      timer:sleep(5000),
      wait_close( DB )
  end.
