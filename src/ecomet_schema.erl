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
-module(ecomet_schema).

-include("ecomet.hrl").
-include("ecomet_schema.hrl").

-behaviour(gen_server).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  %--------Database------------------
  add_db/1,
  remove_db/1,
  mount_db/2,
  unmount_db/1,
  get_mounted_db/1,
  get_db_id/1,
  get_db_name/1,
  get_registered_databases/0,

  %--------Node----------------------
  add_node/1,
  remove_node/1,
  get_node_id/1,

  %-------Pattern--------------------
  get_pattern/1,
  set_pattern/2,

  local_increment/1
]).

%%=================================================================
%%	OTP
%%=================================================================
-export([
  start_link/0,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

%%====================================================================
%%		Test API
%%====================================================================

-define(DEFAULT_SCHEMA_CYCLE, 1000).
-define(WAIT_SCHEMA_TIMEOUT,5000).

-define(INCREMENT,list_to_atom("ecomet_"++atom_to_list(node()))).
-define(SCHEMA,ecomet_schema).

% Database indexing
-record(dbId,{k}).
-record(dbName,{k}).
% Mount point indexing
-record(mntOID,{k}).
-record(mntPath,{k}).


% Node indexing
-record(nodeId,{k}).
-record(nodeName,{k}).

% Patterns
-record(pattern,{id}).

-record(state,{
  cycle
}).

%%=================================================================
%%	DATABASE API
%%=================================================================
%%------------Add a new database to the schema---------------
add_db(Name)->
  case mnesia:transaction(fun()->
    % Adding a new mount point requires lock on the schema
    mnesia:lock({table,?SCHEMA},write),

    Id = new_db_id(),

    % Index on id
    ok = mnesia:write( ?SCHEMA, #kv{ key = #dbId{k=Id}, value = Name }, write ),
    % name 2 id
    ok = mnesia:write( ?SCHEMA, #kv{ key = #dbName{k=Name}, value = Id }, write ),

    Id
  end) of
    { atomic, Id }-> { ok, Id};
    { aborted, Reason }->{error,Reason}
  end.

%%------------Remove a database from the schema---------------
remove_db(Name)->
  case mnesia:transaction(fun()->

    mnesia:lock({table,?SCHEMA},write),

    [ #kv{ value = Id } ] = mnesia:read( ?SCHEMA, #dbName{k=Name} ),

    ok = mnesia:delete( ?SCHEMA, #dbId{k=Id}, write ),

    ok = mnesia:delete( ?SCHEMA, #dbName{k=Name}, write )

  end) of
    { atomic, ok }-> ok;
    { aborted, Reason }->{error,Reason}
  end.

%%---------------Mount a new database to a folder------------------
mount_db(FolderID,DB)->
  case mnesia:transaction(fun()->
    % Adding a new mount point requires lock on the schema
    mnesia:lock({table,?SCHEMA},write),

    { ok, Path } = ecomet:oid2path( FolderID ),

    % Index on OID
    ok = mnesia:write( ?SCHEMA, #kv{ key = #mntOID{k=FolderID}, value = DB }, write ),
    % Index on PATH
    ok = mnesia:write( ?SCHEMA, #kv{ key = #mntPath{k=Path}, value = DB }, write )

  end) of
    { atomic, ok }-> ok;
    { aborted, Reason }->{error,Reason}
  end.

%%---------------Unmount a database from a folder------------------
unmount_db(FolderID)->
  case mnesia:transaction(fun()->

    mnesia:lock({table,?SCHEMA},write),

    { ok, Path } = ecomet:oid2path( FolderID ),

    ok = mnesia:delete( ?SCHEMA, #mntOID{k=FolderID}, write ),

    ok = mnesia:delete( ?SCHEMA, #mntPath{k=Path}, write )

  end) of
    { atomic, ok }-> ok;
    { aborted, Reason }->{error,Reason}
  end.

get_db_id(Name)->
  [#kv{value = ID}] = mnesia:dirty_read(?SCHEMA,#dbName{k=Name}),
  ID.

get_db_name(ID)->
  [#kv{value = Name}] = mnesia:dirty_read(?SCHEMA,#dbId{k=ID}),
  Name.

get_mounted_db(OID)->
  case mnesia:dirty_read(?SCHEMA,#mntOID{k=OID}) of
    [#kv{value = DB}]->DB;
    _->none
  end.

get_registered_databases()->
  MS=[{
    #kv{key = #dbName{k = '$1'}, value = '_'},
    [],
    ['$1']
  }],
  mnesia:dirty_select(?SCHEMA,MS).

%%=================================================================
%%	NODE API
%%=================================================================
%%------------Add a new node to the schema---------------
add_node(Name)->
  case mnesia:transaction(fun()->
    % Adding a new mount point requires lock on the schema
    mnesia:lock({table,?SCHEMA},write),

    Id = new_node_id(),

    % Index on id
    ok = mnesia:write( ?SCHEMA, #kv{ key = #nodeId{k=Id}, value = Name }, write ),
    % name 2 id
    ok = mnesia:write( ?SCHEMA, #kv{ key = #nodeName{k=Name}, value = Id }, write ),

    Id
  end) of
    { atomic, Id }-> { ok, Id };
    { aborted, Reason }->{error,Reason}
  end.

%%------------Remove a node from the schema---------------
remove_node(Name)->
  case mnesia:transaction(fun()->

    mnesia:lock({table,?SCHEMA},write),

    [ #kv{ value = Id } ] = mnesia:read( ?SCHEMA, #nodeName{k=Name} ),

    ok = mnesia:delete( ?SCHEMA, #nodeId{k=Id}, write ),

    ok = mnesia:delete( ?SCHEMA, #nodeName{k=Name}, write )

  end) of
    { atomic, ok }-> ok;
    { aborted, Reason }->{error,Reason}
  end.

get_node_id(Name)->
  [ #kv{ value = Id } ] = mnesia:dirty_read( ?SCHEMA, #nodeName{k=Name} ),
  Id.

%%=================================================================
%%	PATTERN API
%%=================================================================
get_pattern(ID)->
  case mnesia:dirty_read(?SCHEMA,#pattern{id=ID}) of
    [#kv{value = Value}] -> Value;
    _->none
  end.

set_pattern(ID,Value)->
  ok = mnesia:write(?SCHEMA,#kv{key = #pattern{id=ID}, value = Value },write).

local_increment(Key)->
  mnesia:dirty_update_counter(?INCREMENT,Key,1).

%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

init([])->

  ?LOGINFO("starting ecomet schema server ~p",[self()]),

  ok = init_increment_table(),

  ok = init_schema(),

  Cycle=?ENV(schema_server_cycle,?DEFAULT_SCHEMA_CYCLE),

  {ok,#state{cycle = Cycle}}.

handle_call(Request, From, State) ->
  ?LOGWARNING("backend got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.


handle_cast(Request,State)->
  ?LOGWARNING("backend got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
handle_info(Message,State)->
  ?LOGWARNING("backend got an unexpected message ~p",[Message]),
  {noreply,State}.

terminate(Reason,_State)->
  ?LOGINFO("terminating backend reason ~p",[Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%============================================================================
%%	Internal helpers
%%============================================================================
init_increment_table()->
  case table_exists(?INCREMENT) of
    true->
      ?LOGINFO("the increment table already exists"),
      ok;
    _->
      ?LOGINFO("creating the increment table"),
      case mnesia:create_table(?INCREMENT,[
        {attributes, record_info(fields, kv)},
        {record_name, kv},
        {type,ordered_set},
        {disc_copies,[node()]},
        {ram_copies,[]},
        {local_content,true}     % IMPORTANT!!! The increment table belongs only to the relevant node
      ]) of
        {atomic,ok} -> ok;
        {aborted,Reason} ->
          ?LOGERROR("unable to create the increment table for the node ~p",[Reason]),
          ?ERROR(Reason)
      end
  end.

init_schema()->
  case table_exists(?SCHEMA) of
    true->
      ?LOGINFO("wait for schema"),
      mnesia:wait_for_tables([?SCHEMA],?WAIT_SCHEMA_TIMEOUT),
      case table_has_copy(?SCHEMA,disc_copies) of
        true->
          ?LOGINFO("schema already has a local copy");
        _->
          ?LOGINFO("creating a local copy of the schema"),
          case mnesia:add_table_copy(?SCHEMA,node(),disc_copies) of
            {atomic,ok}->
              ?LOGINFO("schema is copied successfully");
            {aborted,CopyError}->
              ?LOGERROR("unable to copy schema ~p",[CopyError]),
              ?ERROR(CopyError)
          end
      end,
      ok;
    false->
      ?LOGINFO("schema does not exist yet, creating..."),
      create_schema(),
      ?LOGINFO("initialize ecomet environment"),
      init_environment()
  end.

create_schema()->
  % Create the table itself
  case mnesia:create_table(?SCHEMA,[
    {attributes, record_info(fields, kv)},
    {record_name, kv},
    {type,ordered_set},
    {disc_copies,[node()]},
    {ram_copies,[]}
  ]) of
    {atomic,ok} ->
      % Initialize the minimum required environment
      prepare_schema();
    {aborted,Reason} ->
      ?LOGERROR("unable to create the schema table for the node ~p",[Reason]),
      ?ERROR(Reason)
  end.

prepare_schema()->
  % Create a DB for the /root
  ecomet_backend:remove_db(?ROOT),
  ?LOGINFO("creating the root database"),
  ecomet_backend:create_db(?ROOT),

  % Add the root db to the schema
  add_db(?ROOT),
  % Mount the root folder to the root DB
  mount_db({?FOLDER_PATTERN,?ROOT_FOLDER},?ROOT),

  % Add the node to the schema
  ?LOGINFO("adding the ~p to the schema",[node()]),
  add_node(node()),

  ok.

init_environment()->
  % Add the low-level patterns to the schema
  ?LOGINFO("initializing low-level patterns"),
  init_low_level_patterns(),

  % Init root
  ecomet_user:on_init_state(),
  ?LOGINFO("creating the root directory"),
  init_root(),

  % Attach low-level behaviours
  ?LOGINFO("attaching low-level behaviours"),
  attach_low_level_begaviours(),

  % Init ecomet environment
  ?LOGINFO("initializing the ecomet environment"),
  init_ecomet_env(),

  ok.

init_low_level_patterns()->

  %-------Object------------------
  Object=#{
    <<".name">>=>#{ type => string, index=> [simple], required => true },
    <<".folder">>=>#{ type => link, index=> [simple], required => true },
    <<".pattern">>=>#{ type => link, index=> [simple], required => true },
    <<".readgroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<".writegroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<".ts">> =>#{ type => integer }
  },
  ObjectFieldsMap = build_pattern_schema(Object),
  ObjectMap= ecomet_pattern:set_behaviours(ObjectFieldsMap,[]),
  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:edit_map({?PATTERN_PATTERN,?OBJECT_PATTERN},ObjectMap)
  end),

  %-----Folder---------------------
  Folder = maps:merge(Object,#{
    <<".contentreadgroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<".contentwritegroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<"only_patterns">>=>#{ type => list, subtype => link },
    <<"exclude_patterns">>=>#{ type => list, subtype => link },
    <<"recursive_rights">> =>#{ type => bool }
  }),
  FolderFieldsMap=build_pattern_schema(Folder),
  FolderMap= ecomet_pattern:set_behaviours(FolderFieldsMap,[]),
  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:edit_map({?PATTERN_PATTERN,?FOLDER_PATTERN},FolderMap)
  end),

  %-----Pattern---------------------
  Pattern = maps:merge(Folder,#{
    <<"behaviour_module">>=>#{ type => atom },
    <<"parent_pattern">>=>#{ type => link, index=> [simple], required => true },
    <<"parents">>=>#{ type => list, subtype => link, index=> [simple] }
  }),
  PatternFieldsMap=build_pattern_schema(Pattern),
  PatternMap= ecomet_pattern:set_behaviours(PatternFieldsMap,[]),
  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:edit_map({?PATTERN_PATTERN,?PATTERN_PATTERN},PatternMap)
  end),

  %-----Field---------------------
  Field = maps:merge(Object,#{
    <<"type">>=>#{ type => atom, required => true },
    <<"subtype">>=>#{ type => atom },
    <<"index">>=>#{ type => list, subtype => atom },
    <<"required">>=>#{ type => bool },
    <<"default_value">> =>#{ type => term },
    <<"storage">>=>#{ type => atom, default_value => disc  },
    <<"autoincrement">>=>#{ type => bool }
  }),
  FieldFieldsMap=build_pattern_schema(Field),
  FieldMap= ecomet_pattern:set_behaviours(FieldFieldsMap,[]),
  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:edit_map({?PATTERN_PATTERN,?FIELD_PATTERN},FieldMap)
  end),

  ok.

build_pattern_schema(Fields)->
  maps:fold(fun(Name,Config,Acc)->
    C = ecomet_field:build_description(Config),
    ecomet_field:map_add(Acc,Name,C)
  end,#{},Fields).


init_root()->
  Root = ecomet:create_object(#{
    <<".name">>=><<"root">>,
    <<".folder">>=> {?FOLDER_PATTERN,?ROOT_FOLDER},
    <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN},
    <<".ts">>=>ecomet_lib:log_ts()
  }),
  _Patterns = ecomet:create_object(#{
    <<".name">>=><<".pattern">>,
    <<".folder">>=> ?OID(Root),
    <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN},
    <<".ts">>=>ecomet_lib:log_ts()
  }),

  ok.

attach_low_level_begaviours()->
  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:set_behaviours({?PATTERN_PATTERN,?OBJECT_PATTERN},[ecomet_object])
  end),

  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:set_behaviours({?PATTERN_PATTERN,?FOLDER_PATTERN},[ecomet_folder,ecomet_object])
  end),

  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:set_behaviours({?PATTERN_PATTERN,?PATTERN_PATTERN},[ecomet_pattern,ecomet_folder,ecomet_object])
  end),

  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:set_behaviours({?PATTERN_PATTERN,?FIELD_PATTERN},[ecomet_field,ecomet_object])
  end),
  ok.

init_ecomet_env()->
  % TODO
  ok.

table_exists(Table)->
  Known = mnesia:system_info(tables),
  lists:member(Table,Known).

table_has_copy(Table,Type)->
  Copies=mnesia:table_info(?SCHEMA,Type),
  lists:member(Table,Copies).

new_db_id()->
  new_db_id(mnesia:dirty_next(?SCHEMA, #dbId{k=-1}), -1).
new_db_id(#dbId{k=NextID}=DB,Id)->
  if
    (NextID - Id) > 1 -> Id + 1 ;
    true -> new_db_id( mnesia:dirty_next(?SCHEMA,DB), NextID )
  end;
new_db_id(_Other,Id)->
  % '$end_of_table' or other keys range
  Id + 1.

new_node_id()->
  new_node_id(mnesia:dirty_next(?SCHEMA, #nodeId{k=-1}), -1).
new_node_id(#nodeId{k=NextID}=Node,Id)->
  if
    (NextID - Id) > 1 -> Id + 1 ;
    true -> new_node_id( mnesia:dirty_next(?SCHEMA,Node), NextID )
  end;
new_node_id(_Other,Id)->
  % '$end_of_table' or other keys range
  Id + 1.