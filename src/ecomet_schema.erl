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
  get_mounted_folder/1,
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
  list_patterns/0,

  field_description/7,
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
-ifdef(TEST).

-export([
  object_schema/0,
  register_type/2
]).

-endif.
%%====================================================================

-define(DEFAULT_SCHEMA_CYCLE, 1000).
-define(WAIT_SCHEMA_TIMEOUT,5000).

-define(INCREMENT,list_to_atom("ecomet_"++atom_to_list(node()))).
-define(SCHEMA,ecomet_schema).

%%====================================================================
%%		System patterns
%%====================================================================
object_schema() ->
  #{
    <<".name">> => #{type => string, index => [simple], required => true},
    <<".folder">> => #{type => link, index => [simple], required => true},
    <<".pattern">> => #{type => link, index => [simple], required => true},
    <<".readgroups">> => #{type => list, subtype => link, index => [simple]},
    <<".writegroups">> => #{type => list, subtype => link, index => [simple]},
    <<".ts">> => #{type => integer}
  }.

folder_schema() ->
  BaseObj = object_schema(),
  BaseObj#{
    <<".contentreadgroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<".contentwritegroups">>=>#{ type => list, subtype => link, index=> [simple] },
    <<"only_patterns">>=>#{ type => list, subtype => link },
    <<"exclude_patterns">>=>#{ type => list, subtype => link },
    <<"recursive_rights">> =>#{ type => bool },
    <<"database">> =>#{ type => link, index=> [simple] }
  }.

pattern_schema() ->
  FolderObj = folder_schema(),
  FolderObj#{
    <<"behaviour_module">>=>#{ type => atom },
    <<"parent_pattern">>=>#{ type => link, index=> [simple], required => true },
    <<"parents">>=>#{ type => list, subtype => link, index=> [simple] }
  }.

field_schema() ->
  BaseObj = object_schema(),
  BaseObj#{
    <<"type">>=>#{ type => atom, required => true },
    <<"subtype">>=>#{ type => atom },
    <<"index">>=>#{ type => list, subtype => atom },
    <<"required">>=>#{ type => bool },
    <<"default">> =>#{ type => term },
    <<"storage">>=>#{ type => atom, default_value => disc  },
    <<"autoincrement">>=>#{ type => bool }
  }.

field_description(Type, Subtype, Index, Required, Storage, Default, Inc) ->
  #{
    type => Type,
    subtype => Subtype,
    index => Index,
    required => Required,
    storage => Storage,
    default => Default,
    autoincrement => Inc
  }.

%-------------STORAGE PATTERNS--------------------------------------------
-define(DATABASE_SCHEMA,#{
  <<"id">>=>#{ type => integer, index=> [simple] }
}).
-define(SEGMENT_SCHEMA,#{
  <<"size">>=>#{ type => integer },
  <<"nodes">>=>#{ type => list, subtype => link, index=> [simple] }
}).
-define(NODE_SCHEMA,#{
  <<"id">>=>#{ type => integer, index=> [simple] },
  <<"is_ready">>=>#{ type => bool, index=> [simple] }
}).

%-------------USER PATTERNS--------------------------------------------
-define(USER_SCHEMA,#{
  <<"password">>=>#{ type => string, required => true },
  <<"usergroups">>=>#{ type => list, subtype => link, index=> [simple] }
  }).
-define(USERGROUP_SCHEMA,#{
  % No additional fields
}).
-define(SESSION_SCHEMA,#{
  <<"close">>=>#{ type => integer, index=> [simple,datetime] },
  <<"node">>=>#{ type => atom, index=> [simple] },
  <<"PID">>=>#{ type => term, index=> [simple] },
  <<"info">> =>#{ type => term }
}).
-define(SUBSCRIPTION_SCHEMA,#{
  <<"PID">>=>#{ type => term, required => true, index=> [simple], storage => ?RAMLOCAL },
  <<"session_PID">>=>#{ type => term, required => true, storage => ?RAMLOCAL },
  <<"rights">>=>#{ type => list, subtype => link, required => true,index=> [simple], storage => ?RAMLOCAL },
  <<"is_admin">>=>#{ type => bool, subtype => none, index=> [simple], storage => ?RAMLOCAL },
  <<"reper_tags">>=>#{ type => list, subtype => term, required => true, index=> [simple], storage => ?RAMLOCAL },
  <<"query_tags">>=>#{ type => list, subtype => term, required => true, index=> [simple], storage => ?RAMLOCAL },
  <<"fields">>=>#{ type => list, subtype => term, required => true, index=> [simple], storage => ?RAMLOCAL },
  <<"feedback">>=>#{ type => bool, subtype => none, index=> [simple], storage => ?RAMLOCAL }
}).

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
% A database can be mounted to more than one folder
mount_db(FolderID,DB)->
  case mnesia:transaction(fun()->
    % Adding a new mount point requires lock on the schema
    mnesia:lock({table,?SCHEMA},write),

    Path = ecomet:oid2path( FolderID ),

    % Index on OID
    ok = mnesia:write( ?SCHEMA, #kv{ key = #mntOID{k=FolderID}, value = DB }, write ),
    % Index on PATH
    ok = mnesia:write( ?SCHEMA, #kv{ key = #mntPath{k=Path}, value = FolderID }, write )

  end) of
    { atomic, ok }-> ok;
    { aborted, Reason }->{error,Reason}
  end.

%%---------------Unmount a database from a folder------------------
unmount_db(FolderID)->
  case mnesia:transaction(fun()->

    mnesia:lock({table,?SCHEMA},write),

    Path = ecomet:oid2path( FolderID ),

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

get_mounted_folder(Path)->
  Closest = #mntPath{k=MountedPat} = mnesia:dirty_prev(?SCHEMA,#mntPath{ k= Path }),
  [#kv{value = FolderID}] = mnesia:dirty_read(?SCHEMA,Closest),
  {MountedPat,FolderID}.

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

list_patterns() ->
  Matcher = #kv{key = #pattern{id = '$1'}, value = '$2'},
  Result = ['$1'],
  lists:flatten(mnesia:dirty_select(?SCHEMA, [{Matcher, [], [Result]}])).

local_increment(Key)->
  mnesia:dirty_update_counter(?INCREMENT,Key,1).

%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

init([])->

  ?LOGINFO("starting ecomet schema server ~p",[self()]),

  ?LOGINFO("initialize local increment table"),
  ok = init_increment_table(),

  ?LOGINFO("intialize schema"),
  ok = init_schema_table(),

  ?LOGINFO("initialize root database"),
  ok = add_root_database(),

  ok = register_node(),

  ok = init_base_types(),

  % Set the init context
  ecomet_user:on_init_state(),

  ok = init_base_types_objects(),

  ok = init_storage_objects(),

  ok = init_default_users(),

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

init_schema_table()->
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
      ?LOGINFO("schema table does not exist yet, creating..."),
      case mnesia:create_table(?SCHEMA,[
        {attributes, record_info(fields, kv)},
        {record_name, kv},
        {type,ordered_set},
        {disc_copies,[node()]},
        {ram_copies,[]}
      ]) of
        {atomic,ok} ->
          ok;
        {aborted,Reason} ->
          ?LOGERROR("unable to create the schema table for the node ~p",[Reason]),
          ?ERROR(Reason)
      end
  end.

add_root_database()->
  try
     0 = get_db_id(?ROOT),
     ?LOGINFO("the root database already exists"),
     ok
  catch
      _:_  ->
        ?LOGINFO("the root database does not exist, creating..."),
        ecomet_backend:remove_db(?ROOT),
        ecomet_backend:create_db(?ROOT),

        % Add the root db to the schema
        add_db(?ROOT),
        % Mount the root folder to the root DB
        mount_db({?FOLDER_PATTERN,?ROOT_FOLDER},?ROOT),
        ok
  end.

register_node()->
  try
    0 = get_node_id(node()),
    ?LOGINFO("the node ~p is already registered",[node()]),
    ok
  catch
    _:_  ->
      % Add the node to the schema
      ?LOGINFO("adding the ~p node to the schema",[node()]),
      add_node(node()),
      ok
  end.

init_base_types()->
  %-------Object------------------
  ok = register_type({?PATTERN_PATTERN,?OBJECT_PATTERN}, object_schema()),

  %-----Folder---------------------
  ok = register_type({?PATTERN_PATTERN,?FOLDER_PATTERN}, folder_schema()),

  %-----Pattern---------------------
  ok = register_type({?PATTERN_PATTERN,?PATTERN_PATTERN}, pattern_schema()),

  %-----Field---------------------
  ok = register_type({?PATTERN_PATTERN,?FIELD_PATTERN}, field_schema()),

  ok.

register_type(ID,Fields)->
  case get_pattern(ID) of
    none->
      % Register the pattern in the schema (without a behaviour)
      {ok,_} = ecomet:transaction(fun()->
        ecomet_pattern:set_behaviours(ID,[])
      end),
      % Add the fields
      [ {ok,_} = ecomet:transaction(fun()->
        Config1 = ecomet_field:build_description(Config),
        ecomet_pattern:append_field(ID, Field, Config1)
      end) || {Field,Config} <- maps:to_list(Fields) ],

      ok;
    _->
      % The pattern is already registered
      ok
  end.

init_base_types_objects()->

  % Step 1. Create objects
  Tree = [
    { <<"root">>, #{
      fields=>#{
        <<".folder">>=> {?FOLDER_PATTERN,0},
        <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN}
      },
      children=>[
        { <<".patterns">>, #{
          fields=>#{
            <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN}
          },
          children=>[
            % #1. ?OBJECT_PATTERN
            { <<".object">>, #{
              fields=>#{
                <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
                <<"behaviour_module">>=>ecomet_object,
                <<"parent_pattern">>=>{?PATTERN_PATTERN,?OBJECT_PATTERN},
                <<"parents">>=>[]
              },
              children=>init_pattern_fields(object_schema())
            }},
            % #2. ?FOLDER_PATTERN
            { <<".folder">>, #{
              fields=>#{
                <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
                <<"behaviour_module">>=>ecomet_folder,
                <<"parent_pattern">>=>{?PATTERN_PATTERN,?OBJECT_PATTERN},
                <<"parents">>=>[{?PATTERN_PATTERN,?OBJECT_PATTERN}]
              },
              children=>init_pattern_fields(folder_schema())
            }},
            % IMPORTANT! Patterns describing objects are created WITHOUT behaviours
            % #3. ?PATTERN_PATTERN
            { <<".pattern">>, #{
              fields=>#{
                <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
                <<"behaviour_module">>=>ecomet_pattern,
                <<"parent_pattern">>=>{?PATTERN_PATTERN,?FOLDER_PATTERN},
                <<"parents">>=>[{?PATTERN_PATTERN,?FOLDER_PATTERN},{?PATTERN_PATTERN,?OBJECT_PATTERN}]
              },
              children=>init_pattern_fields(pattern_schema())
            }},
            % #4. ?FIELD_PATTERN
            { <<".field">>, #{
              fields=>#{
                <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
                <<"behaviour_module">>=>ecomet_field,
                <<"parent_pattern">>=>{?PATTERN_PATTERN,?OBJECT_PATTERN},
                <<"parents">>=>[{?PATTERN_PATTERN,?OBJECT_PATTERN}]
              },
              children=>init_pattern_fields(field_schema())
            }}
          ]
        }}
      ]
    }}
  ],

  init_tree({?FOLDER_PATTERN,0},Tree),

  % Step 2. Attach behaviours to the base types
  [ case ecomet_pattern:get_behaviours(ID) of
      Modules -> ok;
      _->
        {ok,_} = ecomet:transaction(fun()->
          ecomet_pattern:set_behaviours(ID, Modules )
        end)
    end || { ID, Modules } <- [
    { {?PATTERN_PATTERN,?OBJECT_PATTERN}, [ecomet_object] },
    { {?PATTERN_PATTERN,?FOLDER_PATTERN}, [ecomet_folder,ecomet_object] },
    { {?PATTERN_PATTERN,?PATTERN_PATTERN}, [ecomet_pattern,ecomet_folder,ecomet_object] },
    { {?PATTERN_PATTERN,?FIELD_PATTERN}, [ecomet_field,ecomet_object] }
  ] ],

  ok.

init_pattern_fields(Fields)->
  [ begin
      Config1 = ecomet_field:from_schema(ecomet_field:build_description(Config)),
      { Name, #{ fields=> Config1#{
        <<".pattern">> => { ?PATTERN_PATTERN, ?FIELD_PATTERN }
      }} }
    end || { Name, Config } <- ordsets:from_list(maps:to_list(Fields)) ].


init_storage_objects()->

  % Step 1, Register the patterns (without behaviours). We create patterns without behaviours first
  % because the root database and the first are already added to the schema, therefore when we will
  % be creating the related objects we don't want the behaviours do it again
  PatternsTree = [
    { <<".patterns">>, #{
      children=>[
        % DATABASE
        { <<".database">>, #{
          fields=>#{
            <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
            <<"parent_pattern">>=>{?PATTERN_PATTERN,?FOLDER_PATTERN}
          },
          children=>init_pattern_fields(?DATABASE_SCHEMA)
        }},
        % SEGMENT
        { <<".segment">>, #{
          fields=>#{
            <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
            <<"parent_pattern">>=>{?PATTERN_PATTERN,?OBJECT_PATTERN}
          },
          children=>init_pattern_fields(?SEGMENT_SCHEMA)
        }},
        % NODE
        { <<".node">>, #{
          fields=>#{
            <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
            <<"parent_pattern">>=>{?PATTERN_PATTERN,?FOLDER_PATTERN}
          },
          children=>init_pattern_fields(?NODE_SCHEMA)
        }}
      ]
    }}
  ],
  init_tree({?FOLDER_PATTERN,?ROOT_FOLDER}, PatternsTree),

  % Step 2. Create storage objects
  StorageTree = [
    { <<".nodes">>, #{
      fields=>#{ <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN} },
      children=>[
        { atom_to_binary(node(),utf8), #{
          fields=>#{
            <<".pattern">>=>?OID(<<"/root/.patterns/.node">>),
            <<"id">>=>0
          }
        }}
      ]
    }},
    { <<".databases">>, #{
      fields=>#{ <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN} },
      children=>[
        { <<"root">>, #{
          fields=>#{
            <<".pattern">>=>?OID(<<"/root/.patterns/.database">>),
            <<"id">>=>0
          }
        }}
      ]
    }}
  ],
  init_tree({?FOLDER_PATTERN,?ROOT_FOLDER}, StorageTree),

  % Step 3. Attach behaviours
  ok = ecomet:edit_object(ecomet:open(<<"/root/.patterns/.node">>),#{
    <<"behaviour_module">>=>ecomet_node
  }),
  ok = ecomet:edit_object(ecomet:open(<<"/root/.patterns/.database">>),#{
    <<"behaviour_module">>=>ecomet_db
  }),

  ok.

init_default_users()->

  PatternsTree = [
    { <<".patterns">>, #{
      children=>[
        % USER
        { <<".user">>, #{
          fields=>#{
            <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
            <<"parent_pattern">>=>{?PATTERN_PATTERN,?FOLDER_PATTERN},
            <<"behaviour_module">>=>ecomet_user
          },
          children=>init_pattern_fields(?USER_SCHEMA)
        }},
        % USERGROUP
        { <<".usergroup">>, #{
          fields=>#{
            <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
            <<"parent_pattern">>=>{?PATTERN_PATTERN,?OBJECT_PATTERN}
          }
        }},
        % SESSION
        { <<".session">>, #{
          fields=>#{
            <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
            <<"parent_pattern">>=>{?PATTERN_PATTERN,?FOLDER_PATTERN}
          },
          children=>
            [ { Name, #{ edit => true, fields=> ecomet_field:from_schema(Config)} }
             || { Name, Config } <- [
                {<<".name">>,#{ autoincrement => true } },
                { <<".ts">> , #{ index=> [ simple, datetime ] }}
            ]] ++ init_pattern_fields(?SESSION_SCHEMA)
        }},
        % SUBSCRIPTION
        { <<".subscription">>, #{
          fields=>#{
            <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
            <<"parent_pattern">>=>{?PATTERN_PATTERN,?OBJECT_PATTERN}
          },
          children=>
          [ begin
              { Name, #{ edit => true, fields=> ecomet_field:from_schema(#{ storage=>?RAMLOCAL })} }
            end || Name <- maps:keys(object_schema())]
          ++ init_pattern_fields(?SUBSCRIPTION_SCHEMA)
        }}
      ]
    }}
  ],
  init_tree({?FOLDER_PATTERN,?ROOT_FOLDER}, PatternsTree),

  % .administrators usergroup
  init_tree({?FOLDER_PATTERN,?ROOT_FOLDER},[
    { <<".usergroups">>, #{
      fields=>#{ <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN} },
      children=>[
        { <<".administrators">>, #{
          fields=>#{
            <<".pattern">>=>?OID(<<"/root/.patterns/.usergroup">>)
          }
        }}
      ]
    }}
  ]),
  % system user
  init_tree({?FOLDER_PATTERN,?ROOT_FOLDER},[
    { <<".users">>, #{
      fields=>#{ <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN} },
      children=>[
        { <<"system">>, #{
          fields=>#{
            <<".pattern">>=>?OID(<<"/root/.patterns/.user">>),
            <<"usergroups">>=>[?OID(<<"/root/.usergroups/.administrators">>)],
            <<"password">>=><<"111111">>
          }
        }}
      ]
    }}
  ]),
  ok.

table_exists(Table)->
  Known = mnesia:system_info(tables),
  lists:member(Table,Known).

table_has_copy(Table,Type)->
  Copies=mnesia:table_info(Table,Type),
  lists:member(node(),Copies).

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

init_tree(FolderID,Items)->
  [
    begin
      ItemID=
        case ecomet_folder:find_object_system(FolderID,Name) of
          {ok,OID}->
            case Params of
              #{ edit := true }->
                Fields = maps:get(fields,Params),
                Object = ecomet:open(OID,none),
                ok = ecomet:edit_object(Object,Fields);
              _->ok
            end,
            OID;
          _->
            Fields = maps:get(fields,Params),
            Object = ecomet:create_object(Fields#{
              <<".name">>=>Name,
              <<".folder">>=>FolderID,
              <<".ts">>=>ecomet_lib:log_ts()
            }),
            ?OID(Object)
        end,
      init_tree(ItemID,maps:get(children,Params,[]))

    end || { Name, Params } <- Items ].
