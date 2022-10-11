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
%%	SERVICE API
%%=================================================================
-export([
  is_local/1,
  get_search_node/2,
  get_databases/0,
  get_name/1,
  get_by_name/1,
  get_storage_oid/3,
  get_segment_oid/1,
  get_storage_segments/1,
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
%%	ZAYA
%%=================================================================
%%	SERVICE
create( Params )->
  Types = maps:to_list( Params ),
  try
    [ ok = Module:create( TypeParams ) ||{_Type,#{ module := Module, params := TypeParams }} <- Types],
    ok
  catch
    _:E->
      [ catch Module:remove( TypeParams ) ||{_Type,#{ module := Module, params := TypeParams }} <- Types],
      throw(E)
  end.

open( Params )->
  try
    maps:fold(fun(Type, #{ module := Module, params := TypeParams }, Acc)->
      Ref = Module:open( TypeParams ),
      case get({?MODULE,open_ref}) of
        Refs when is_list( Refs )->
          put({?MODULE,open_ref},[{Module,Ref}|Refs]);
        _->
          put({?MODULE,open_ref},[{Module,Ref}])
      end,
      Acc#{ Type => {Module, Module:open( TypeParams )} }
    end,#{}, Params )
  catch
    _:E->
      case get({?MODULE,open_ref}) of
        Refs when is_list( Refs )->
          [ catch Module:close( Ref ) || {Module,Ref} <- Refs ];
        _->
          nothing_is_opened
      end,
      throw(E)
  after
    erase({?MODULE,open_ref})
  end.

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

get_storage_oid(DB,Storage,Type)->
  ecomet_folder:path2oid(<<
    "/root/.databases/",
    (atom_to_binary(DB,utf8))/binary,
    "/",
    (storage_name(Storage,Type))/binary
  >>).

get_segment_oid(Name) when is_atom(Name)->
  get_segment_oid(atom_to_binary(Name,utf8));
get_segment_oid(Name) when is_binary(Name)->
  case ecomet_query:system([?ROOT],[<<".oid">>],{'AND',[
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.segment">>)},
    {<<".name">>,'=',Name}
  ]}) of
    [OID]->{ok,OID};
    _->{error,not_found}
  end.

get_storage_segments(StorageID)->
  {_,Segments} = ecomet_query:system([?ROOT],[<<".name">>],{'AND',[
    {<<".pattern">>,':=',?OID(<<"/root/.patterns/.segment">>)},
    {<<".folder">>,'=',StorageID }
  ]}),
  [ binary_to_atom(S,utf8) || [S]<-Segments].

storage_name(Storage,Type)->
  <<(atom_to_binary(Storage,utf8))/binary,"@",(atom_to_binary(Type,utf8))/binary>>.

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
  Actual = ecomet_backend:get_segments(DB,Storage,Type),

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
  #{ local:= IsLocal } = ecomet_backend:get_segment_info(Segment),
  if
    not IsLocal ->
      {ok, #{ copies := Copies }} = ecomet_backend:get_segment_params(Segment),
      Actual = maps:keys( Copies ),
      case ecomet:read_field(Object,<<"nodes">>,#{default=>[]}) of
        {ok,[]}->
          ok = ecomet:edit_object(Object,#{<<"nodes">>=>Actual});
        {ok,Configured}->

          % Add copies of the segment to nodes
          [ ecomet_backend:add_segment_copy(Segment,N) || N <- Configured -- Actual ],

          % Remove copies
          [ ecomet_backend:remove_segment_copy(Segment,N) || N <- Actual -- Configured ]

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
  #{nodes := RootNodes } = ecomet_backend:get_segment_info(Segment),
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
  Root = ecomet_backend:get_root_segment(DB,Storage,Type),
  is_master(Root).

update_db(OID)->
  {_, StorageTypes } = ecomet:get(get_databases(),[<<"nodes">>,<<"size">>,<<"segments_count">>,<<"root_segment">>],{'AND',[
    {<<".folder">>,'=',OID},
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.storage">>)}
  ]}),

  Update =
    lists:foldl(fun([Nodes,Size,Count,Root],#{<<"nodes">>:=AccNodes,<<"size">>:=AccSize,<<"segments_count">>:=AccCount}=Acc)->
      case ecomet_backend:get_segment_info(Root) of
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

  Root = ecomet_backend:get_root_segment(DB,Storage,Type),
  ActualLimits = ecomet_backend:storage_limits( DB,Storage,Type ),

  Update =
    case Limits of
      ActualLimits -> Update0;
      _ when is_map( Limits )->
        % Set storage limits
        ecomet_backend:storage_limits( DB,Storage,Type, Limits ),
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
  Size = ecomet_backend:get_segment_size(binary_to_atom(Name,utf8)),
  ok = ecomet:edit_object(Object,#{<<"size">>=>Size}).

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  check_name(Object),
  %-------------Create a new database-------------------------
  { ok, Name } = ecomet:read_field(Object,<<".name">>),
  NameAtom = binary_to_atom(Name,utf8),
  ?LOGINFO("creating a new database ~p",[NameAtom]),

  % Create storage types
  [ ecomet:create_object(#{
    <<".name">>=>storage_name(S,T),
    <<".folder">>=>?OID(Object),
    <<".pattern">>=>?OID(<<"/root/.patterns/.storage">>)
  }) || S<-[?DATA,?INDEX], T<-?STORAGE_TYPES ],

  % We need to create the backend out of a transaction, otherwise it throws 'nested_transaction'
  ecomet:on_commit(fun()->
    case create_database(NameAtom) of
      {ok,ID}->
        ?LOGINFO("database ~p created successfully, id = ~p",[NameAtom,ID]),
        ok = ecomet:edit_object(Object,#{<<"id">>=>ID});
      _->
        ok = ecomet:delete_object(Object)
    end
  end),
  ok.

on_edit(Object)->
  check_name(Object),
  check_id(Object),
  ok.

on_delete(Object)->
  case ecomet_folder:find_mount_points(?OID(Object)) of
    []->
      {ok,Name}=ecomet:read_field(Object,<<".name">>),
      NameAtom = binary_to_atom(Name,utf8),
      ecomet:on_commit(fun()->
        remove_database(NameAtom)
      end),
      ok;
    _->?ERROR(is_mounted)
  end.

check_name(Object)->
  case ecomet:field_changes(Object,<<".name">>) of
    none->ok;
    { Name, none }->
      case re:run(Name,"^(\\w+)$") of
        {match,_}->ok;
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

create_database(Name)->
  try
    ?LOGINFO("creating backend for ~p",[Name]),
    ok = ecomet_backend:create_db(Name),
    ?LOGINFO("backend for ~p database is created successfully",[Name]),

    ?LOGINFO("registering the ~p database in the schema",[Name]),
    case ecomet_schema:add_db(Name) of
      {ok,ID}->{ok,ID};
      {error,Error}->
        try ecomet_backend:remove_db(Name) catch
          _:CleanError->
            ?LOGERROR("error on removing ~p database, error ~p",[Name,CleanError])
        end,
        throw(Error)
    end
  catch
    _:CreateError:Stack->
      ?LOGERROR("unable to create the ~p database, error ~p, stack ~p",[
        Name,
        CreateError,
        Stack
      ]),
      error
  end.

remove_database(Name)->
  ?LOGINFO("unregistering the ~p database",[Name]),
  case ecomet_schema:remove_db(Name) of
    ok->ok;
    {error,Error}->
      ?LOGERROR("error unregistering a database ~p, error ~p",[Name,Error])
  end,
  try ecomet_backend:remove_db(Name) catch
    _:CleanError->
      ?LOGERROR("error on removing ~p database, error ~p",[Name,CleanError])
  end,
  ok.