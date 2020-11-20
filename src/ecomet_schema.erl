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
  add_mount_point/2,
  remove_mount_point/1,

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

-define(DEFAULT_SCHEMA_CYCLE, 1000).
-define(WAIT_SCHEMA_TIMEOUT,5000).

-define(INCREMENT,list_to_atom("ecomet_"++atom_to_list(node()))).
-define(SCHEMA,ecomet_schema).
-define(ROOT,root).

% Mount point indexing
-record(mntId,{k}).
-record(mntOID,{k}).
-record(mntPath,{k}).
-record(mntName,{k}).

% Patterns
-record(pattern,{id}).

-record(state,{
  cycle
}).

%%=================================================================
%%	SERVICE API
%%=================================================================
add_mount_point(FolderID,DB)->
  case mnesia:transaction(fun()->
    % Adding a new mount point requires lock on the schema
    mnesia:lock({table,?SCHEMA},write),

    Id = new_mount_id(),

    { ok, Path } = ecomet:oid2path( FolderID ),

    % Index on id
    ok = mnesia:write( ?SCHEMA, #kv{ key = #mntId{k=Id}, value = DB }, write ),
    % Index on OID
    ok = mnesia:write( ?SCHEMA, #kv{ key = #mntOID{k=FolderID}, value = DB }, write ),
    % Index on PATH
    ok = mnesia:write( ?SCHEMA, #kv{ key = #mntPath{k=Path}, value = DB }, write ),
    % name 2 id
    ok = mnesia:write( ?SCHEMA, #kv{ key = #mntName{k=DB}, value = Id }, write )

  end) of
    { atomic, ok }-> ok;
    { aborted, Reason }->{error,Reason}
  end.

remove_mount_point(FolderID)->
  case mnesia:transaction(fun()->

    mnesia:lock({table,?SCHEMA},write),

    { ok, Path } = ecomet:oid2path( FolderID ),
    [ #kv{ value = DB } ] = mnesia:read( ?SCHEMA, #mntOID{k=FolderID} ),
    [ #kv{ value = Id } ] = mnesia:read( ?SCHEMA, #mntName{k=DB} ),

    ok = mnesia:delete( ?SCHEMA, #mntId{k=Id}, write ),

    ok = mnesia:delete( ?SCHEMA, #mntOID{k=FolderID}, write ),

    ok = mnesia:delete( ?SCHEMA, #mntPath{k=Path}, write ),

    ok = mnesia:delete( ?SCHEMA, #mntName{k=DB}, write )

  end) of
    { atomic, ok }-> ok;
    { aborted, Reason }->{error,Reason}
  end.

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
  ecomet_backend:create_db(?ROOT),

  % Mount the root to the root DB
  add_mount_point({?FOLDER_PATTERN,?ROOT_FOLDER},?ROOT),
  ok.

init_environment()->
  % Add the low-level patterns to the schema
  init_low_level_patterns(),

  create_root(),

  ok.

init_low_level_patterns()->

  %-------Object------------------
  Object=#{
    <<".name">>=>#{ type => string, index=> [simple], required => true },
    <<".folder">>=>#{ type => link, index=> [simple], required => true },
    <<".pattern">>=>#{ type => link, index=> [simple], required => true },
    <<".readgroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<".readgroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<".writegroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<".ts">> =>#{ type => integer }
  },
  ObjectFieldsMap = build_pattern_schema(Object),
  ObjectMap= ecomet_pattern:set_behaviours(ObjectFieldsMap,[ecomet_object]),
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
  FolderMap= ecomet_pattern:set_behaviours(FolderFieldsMap,[ecomet_folder,ecomet_object]),
  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:edit_map({?PATTERN_PATTERN,?FOLDER_PATTERN},FolderMap)
  end),

  %-----Pattern---------------------
  Pattern = maps:merge(Folder,#{
    <<".contentreadgroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<".contentwritegroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<"only_patterns">>=>#{ type => list, subtype => link },
    <<"exclude_patterns">>=>#{ type => list, subtype => link },
    <<"recursive_rights">> =>#{ type => bool }
  }),
  PatternFieldsMap=build_pattern_schema(Pattern),
  PatternMap= ecomet_pattern:set_behaviours(PatternFieldsMap,[ecomet_pattern,ecomet_folder,ecomet_object]),
  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:edit_map({?PATTERN_PATTERN,?PATTERN_PATTERN},PatternMap)
  end),

  %-----Field---------------------
  Pattern = maps:merge(Folder,#{
    <<".contentreadgroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<".contentwritegroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<"only_patterns">>=>#{ type => list, subtype => link },
    <<"exclude_patterns">>=>#{ type => list, subtype => link },
    <<"recursive_rights">> =>#{ type => bool }
  }),
  PatternFieldsMap=build_pattern_schema(Pattern),
  PatternMap= ecomet_pattern:set_behaviours(PatternFieldsMap,[ecomet_pattern,ecomet_folder,ecomet_object]),
  {ok,_} = ecomet:transaction(fun()->
    ecomet_pattern:edit_map({?PATTERN_PATTERN,?PATTERN_PATTERN},PatternMap)
  end),

  ok.

build_pattern_schema(Fields)->
  maps:fold(fun(Name,Config,Acc)->
    C = ecomet_field:build_description(Config),
    ecomet_field:map_add(Acc,Name,C)
  end,#{},Fields).



create_root()->
  % TODO
  ok.

table_exists(Table)->
  Known = mnesia:system_info(tables),
  lists:member(Table,Known).

table_has_copy(Table,Type)->
  Copies=mnesia:table_info(?SCHEMA,Type),
  lists:member(Table,Copies).

new_mount_id()->
  new_mount_id(mnesia:dirty_next(?SCHEMA, #mntId{k=-1}), -1).
new_mount_id(#mntId{k=NextID}=Mnt,Id)->
  if
    (NextID - Id) > 1 -> Id + 1 ;
    true -> new_mount_id( mnesia:dirty_next(?SCHEMA,Mnt), NextID )
  end;
new_mount_id(_Other,Id)->
  % '$end_of_table' or other keys range
  Id + 1.