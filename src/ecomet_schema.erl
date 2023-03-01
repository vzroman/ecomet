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
  get_nodes/0,

  %-------Pattern--------------------
  new_pattern_id/0,
  get_pattern/1,
  set_pattern/2,
  list_patterns/0
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
  register_type/2,
  init_tree/2
]).

-endif.
%%====================================================================

%%=================================================================
%%	SCHEMA
%%=================================================================
-define(DEFAULT_LEVELDB_PARAMS(Sync),#{
  eleveldb => #{
    open_options=>#{
      paranoid_checks => false,
      verify_compactions => false,
      compression => false
    },
    read => #{
      verify_checksums => false
    },
    write => #{
      sync => Sync
    }
  }
}).

-define(SCHEMA,'ECOMET_SCHEMA').
-define(schemaModule,zaya_ets_leveldb).
-define(schemaParams,
  #{
    ets => #{},
    leveldb => ?DEFAULT_LEVELDB_PARAMS(true)
  }
).

%%=================================================================
%%	ROOT
%%=================================================================
-define(DEFAULT_ROCKSDB_PARAMS(Sync),#{
  rocksdb => #{
    open_options=>#{
      paranoid_checks => false,
      compression => none
    },
    read => #{
      verify_checksums => false
    },
    write => #{
      sync => Sync
    }
  }
}).

-define(rootModule,ecomet_db).
-define(rootParams,
  #{
    ?RAM => #{
      module => zaya_ets,
      params => #{}
    },
    ?RAMDISC => #{
      module => zaya_ets_rocksdb,
      params => #{
        ets => #{},
        rocksdb => ?DEFAULT_ROCKSDB_PARAMS(false)
      }
    },
    ?DISC =>#{
      module => zaya_rocksdb,
      params => ?DEFAULT_ROCKSDB_PARAMS(false)
    }
  }
).

-define(onError,"close the application, fix the problem and try to start again").
-define(DEFAULT_SCHEMA_CYCLE, 5000).

%%====================================================================
%%		System patterns
%%====================================================================
-define(OBJECT_SCHEMA,#{
  <<".name">>=>#{ type => string, index=> [simple], required => true },
  <<".folder">>=>#{ type => link, index=> [simple], required => true },
  <<".pattern">>=>#{ type => link, index=> [simple], required => true },
  <<".readgroups">>=>#{ type => list, subtype => link, index=> [simple] },
  <<".writegroups">>=>#{ type => list, subtype => link, index=> [simple] },
  <<".ts">> =>#{ type => integer }
}).
-define(FOLDER_SCHEMA,?OBJECT_SCHEMA#{
  <<".contentreadgroups">>=>#{ type => list, subtype => link, index=> [simple] },
  <<".contentwritegroups">>=>#{ type => list, subtype => link, index=> [simple] },
  <<"recursive_rights">> =>#{ type => bool },
  <<"database">> =>#{ type => link, index=> [simple] }
}).
-define(PATTERN_SCHEMA,?FOLDER_SCHEMA#{
  <<"behaviour_module">>=>#{ type => atom , index=> [simple] },
  <<"parent_pattern">>=>#{ type => link, index=> [simple], required => true },
  <<"parents">>=>#{ type => list, subtype => link, index=> [simple] }
}).
-define(FIELD_SCHEMA,?OBJECT_SCHEMA#{
  <<"type">>=>#{ type => atom, required => true, index=> [simple] },
  <<"subtype">>=>#{ type => atom, index=> [simple] },
  <<"index">>=>#{ type => list, subtype => atom, index=> [simple] },
  <<"required">>=>#{ type => bool, index=> [simple] },
  <<"default">> =>#{ type => term },
  <<"storage">>=>#{ type => atom, default_value => disc, index=> [simple]  },
  <<"autoincrement">>=>#{ type => bool, index=> [simple] }
}).
%-------------STORAGE PATTERNS--------------------------------------------
-define(DATABASE_SCHEMA,#{
  <<"id">>=>#{ type => integer, index=> [simple] },
  <<"masters">>=>#{ type => list, subtype => atom, index=> [simple] },
  <<"types">>=>#{ type => list, subtype => atom, index=> [simple] },
  <<"modules">>=>#{ type => term },
  <<"nodes">>=>#{ type => list, subtype => atom, index=> [simple] },
  <<"params">>=>#{ type => term },
  <<"tags">>=>#{ type => list, subtype => string, index=>[simple] },
  <<"available_nodes">> => #{ type => list, subtype => atom, index=> [simple] },
  <<"not_ready_nodes">> => #{ type => list, subtype => atom, index=> [simple] },
  <<"is_available">> => #{ type => bool, index=> [simple] },
  <<"size">>=>#{ type => term }
}).

-define(NODE_SCHEMA,#{
  <<"id">>=>#{ type => integer, index=> [simple] },
  <<"is_ready">>=>#{ type => bool, index=> [simple] }
}).

%-------------USER PATTERNS--------------------------------------------
-define(USER_SCHEMA,#{
  <<"password">>=>#{ type => string, required => true },
  <<"usergroups">>=>#{ type => list, subtype => link, index=> [simple] },
  <<"memory_limit">>=>#{ type => integer }
  }).
-define(USERGROUP_SCHEMA,#{
  <<"extend_groups">>=>#{ type => list, subtype => link, index=> [simple] }
}).

-define(schema_lock,{transform,?SCHEMA}).

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
  % Adding a new database requires lock on the schema
  {ok,Unlock} = elock:lock(?LOCKS, ?schema_lock ,_IsShared = false, _Timeout = infinity, zaya:db_available_nodes(?SCHEMA)),
  Result = zaya:transaction(fun()->
    Id = new_db_id(),
    ok = zaya:write( ?SCHEMA, [{ #dbId{k=Id}, Name }, {#dbName{k=Name}, Id}], write ),
    Id
  end),
  Unlock(),
  case Result of
    { ok, Id }-> { ok, Id};
    { abort, Reason }->{error,Reason}
  end.

%%------------Remove a database from the schema---------------
remove_db(Name)->
  case zaya:transaction(fun()->

    [{_,Id}] = zaya:read( ?SCHEMA, [#dbName{k=Name}], write ),
    ok = zaya:delete(?SCHEMA,[#dbId{k=Id},#dbName{k=Name}], write)

  end) of
    { ok, ok }-> ok;
    { abort, Reason }->{error,Reason}
  end.

%%---------------Mount a new database to a folder------------------
% A database can be mounted to more than one folder
mount_db(FolderID,DB)->
  case zaya:transaction(fun()->

    Path = ecomet:to_path( FolderID ),

    ok = zaya:write(?SCHEMA,[{#mntOID{k=FolderID}, DB}, {#mntPath{k=Path}, FolderID}])

  end) of
    { ok, ok }-> ok;
    { abort, Reason }->{error,Reason}
  end.

%%---------------Unmount a database from a folder------------------
unmount_db(FolderID)->
  case zaya:transaction(fun()->

    Path = ecomet:to_path( FolderID ),

    ok = zaya:delete(?SCHEMA,[#mntOID{k=FolderID}, #mntPath{k=Path} ], write)

  end) of
    { ok, ok }-> ok;
    { abort, Reason }->{error,Reason}
  end.

get_db_id(Name)->
  [{_,ID}] = zaya:read(?SCHEMA,[#dbName{k=Name}]),
  ID.

get_db_name(ID)->
  [{_,Name}] = zaya:read(?SCHEMA,[#dbId{k=ID}]),
  Name.

get_mounted_db(OID)->
  case zaya:read(?SCHEMA,[#mntOID{k=OID}]) of
    [{_,DB}]->DB;
    _->none
  end.

get_mounted_folder(Path)->
  get_mounted_folder(Path, zaya:prev(?SCHEMA,#mntPath{ k= Path }) ).

get_mounted_folder(Path, {#mntPath{k=MountedPath} = Closest, FolderID} )->
  S = size(MountedPath),
  case Path of
    <<MountedPath:S/binary,_/binary>>->
      {MountedPath,FolderID};
    _->
      get_mounted_folder(Path, zaya:prev(?SCHEMA,Closest) )
  end.

get_registered_databases()->
  MS=[{
    {#dbName{k = '$1'},'_'},
    [],
    ['$1']
  }],
  zaya:find(?SCHEMA,#{start => #dbName{k = -1}, stop => #dbName{k=[]}, ms => MS}).

%%=================================================================
%%	NODE API
%%=================================================================
%%------------Add a new node to the schema---------------
add_node(Name)->
  % Adding a new node requires lock on the schema
  {ok,Unlock} = elock:lock(?LOCKS, ?schema_lock ,_IsShared = false, _Timeout = infinity, zaya:db_available_nodes(?SCHEMA)),
  Result = zaya:transaction(fun()->
    Id = new_node_id(),
    ok = zaya:write(?SCHEMA,[{#nodeId{k=Id}, Name},{ #nodeName{k=Name}, Id }], write),
    Id
  end),
  Unlock(),
  case Result of
    { ok, Id }-> { ok, Id };
    { abort, Reason }->{error,Reason}
  end.

%%------------Remove a node from the schema---------------
remove_node(Name)->
  case zaya:transaction(fun()->
    [{_, Id}] = zaya:read( ?SCHEMA, [#nodeName{k=Name}], write ),
    ok = zaya:delete(?SCHEMA,[#nodeId{k=Id}, #nodeName{k=Name}], write)
  end) of
    { ok, ok }-> ok;
    { abort, Reason }->{error,Reason}
  end.

get_node_id(Name)->
  [{_, Id}] = zaya:read( ?SCHEMA, [#nodeName{k=Name}] ),
  Id.

get_nodes()->
  MS=[{
    {#nodeName{k='$1'},'_'},
    [],
    ['$1']
  }],
  zaya:find(?SCHEMA,#{start => #nodeName{k = -1}, stop => #nodeName{k=[]}, ms => MS}).

%%=================================================================
%%	PATTERN API
%%=================================================================
new_pattern_id()->
  case zaya:transaction(fun()->
    NewID=
      case zaya:read(?SCHEMA,[pattern_id],write) of
        []->1;
        [{_,ID}]->ID + 1
      end,
    ok = zaya:write(?SCHEMA,[{pattern_id,NewID}],write),
    NewID
  end) of
    { ok, ID }-> ID;
    { abort, Reason }->?ERROR(Reason)
  end.

get_pattern(ID)->
  case zaya:read(?SCHEMA,[#pattern{id=ID}]) of
    [{_, Value}] -> Value;
    _->none
  end.

set_pattern(ID,Value)->
  case zaya:transaction(fun()->
    ok = zaya:write(?SCHEMA,[{#pattern{id=ID},Value }],write)
  end) of
    { ok, ok }-> ok;
    { abort, Reason }->?ERROR(Reason)
  end.

list_patterns() ->
  MS=[{
    {#pattern{id = '$1'},'_'},
    [],
    ['$1']
  }],
  zaya:find(?SCHEMA,#{start => #pattern{id = -1}, stop => #pattern{id=[]}, ms => MS}).

%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

init([])->

  ?LOGINFO("starting ecomet schema server ~p",[self()]),

  ?LOGINFO("initialize sessions"),
  ok = ecomet_session:on_init(),

  ?LOGINFO("initialize local id database"),
  ok = ecomet_id:init(),

  ?LOGINFO("init backend"),
  ok = ecomet_db:init(),

  ?LOGINFO("intialize schema"),
  ok = init_schema(),

  ?LOGINFO("initialize root database"),
  ok = add_root_database(),

  ok = register_node(),

  ?LOGINFO("initialize base types"),
  ok = init_base_types(),

  % Set the init context
  ecomet_user:on_init_state(),

  ?LOGINFO("initialize base type objects"),
  ok = init_base_types_objects(),

  ?LOGINFO("initialize storage objects"),
  ok = init_storage_objects(),

  ?LOGINFO("initialize default users"),
  ok = init_default_users(),

  Cycle=?ENV(schema_server_cycle,?DEFAULT_SCHEMA_CYCLE),

  % Enter the loop
  self()!on_cycle,

  ?LOGINFO("finish ecomet schema initialization"),
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
handle_info(on_cycle,#state{cycle = Cycle}=State)->
  timer:send_after(Cycle,on_cycle),

  try
    % synchronize nodes configuration
    ecomet_node:sync(),

    % synchronize database configuration
    ecomet_db:sync(),
    ok
  catch
    _:Error:Stack->
      ?LOGERROR("Error on schema synchronizaton ~p, stack ~p",[Error,Stack])
  end,

  {noreply,State};

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
init_schema()->
  case lists:member(?SCHEMA,zaya:all_dbs()) of
    true->
      case lists:member(node(),zaya:db_all_nodes(?SCHEMA)) of
        true->
          ok;
        _->
          try zaya:db_add_copy(?SCHEMA,node(),?schemaParams)
          catch
            _:E->
              ?LOGERROR("schema add copy error ~p, "++?onError,[E]),
              timer:sleep(infinity)
          end
      end;
    false->
      try
        assert_ok( zaya:db_create(?SCHEMA,?schemaModule,#{node()=>?schemaParams}) )
      catch
        _:CreateError->
          ?LOGERROR("schema create error ~p, "++?onError,[CreateError]),
          timer:sleep(infinity)
      end
  end,
  wait_schema().

assert_ok({OKs,Errors})->
  Node = node(),
  case maps:from_list(OKs) of
    #{Node:=_}->ok;
    _->
      throw( lists:keyfind(Node,1,Errors) )
  end.

wait_schema()->
  case lists:member(node(),zaya:db_available_nodes(?SCHEMA)) of
    true-> ok;
    _->
      ?LOGINFO("wait for schema..."),
      timer:sleep(1000),
      wait_schema()
  end.

add_root_database()->
  case lists:member(?ROOT,zaya:all_dbs()) of
    true -> ok;
    _->
      ?LOGINFO("creating root database"),
      try
        assert_ok( zaya:db_create(?ROOT, ?rootModule, #{node() => ?rootParams}) )
      catch
        _:CreateError->
          ?LOGERROR("root database create error ~p, "++?onError,[CreateError])
      end
  end,

  case zaya:read(?SCHEMA,[#dbName{k=?ROOT}]) of
    [_]->ok;
    _->
      ?LOGINFO("register root database"),
      case add_db( ?ROOT ) of
        {ok,_}->ok;
        {error,RegisterError}->
          ?LOGERROR("register root database error ~p",[RegisterError]),
          timer:sleep(infinity)
      end,
      case mount_db({?FOLDER_PATTERN,?ROOT_FOLDER},?ROOT) of
        ok->ok;
        {error,MountError}->
          ?LOGERROR("mount root database error ~p",[MountError]),
          timer:sleep(infinity)
      end,
      wait_root()
  end.

wait_root()->
  case zaya:db_available_nodes( ?ROOT ) of
    []->
      ?LOGINFO("wait for root database..."),
      timer:sleep(1000),
      wait_root();
    _->
      ok
  end.

register_node()->
  case zaya:read( ?SCHEMA, [#nodeName{k=node()}] ) of
    [_]->ok;
    _->
      ?LOGINFO("register node"),
      case add_node( node() ) of
        {ok,_}->ok;
        {error,Error}->
          ?LOGERROR("register node error ~p",[Error]),
          timer:sleep(infinity)
      end
  end.

init_base_types()->
  %-------Object------------------
  ok = register_type({?PATTERN_PATTERN,?OBJECT_PATTERN}, ?OBJECT_SCHEMA),

  %-----Folder---------------------
  ok = register_type({?PATTERN_PATTERN,?FOLDER_PATTERN}, ?FOLDER_SCHEMA),

  %-----Pattern---------------------
  ok = register_type({?PATTERN_PATTERN,?PATTERN_PATTERN}, ?PATTERN_SCHEMA),

  %-----Field---------------------
  ok = register_type({?PATTERN_PATTERN,?FIELD_PATTERN}, ?FIELD_SCHEMA),

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
              children=>init_pattern_fields(?OBJECT_SCHEMA)
            }},
            % #2. ?FOLDER_PATTERN
            { <<".folder">>, #{
              fields=>#{
                <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
                <<"behaviour_module">>=>ecomet_folder,
                <<"parent_pattern">>=>{?PATTERN_PATTERN,?OBJECT_PATTERN},
                <<"parents">>=>[{?PATTERN_PATTERN,?OBJECT_PATTERN}]
              },
              children=>init_pattern_fields(?FOLDER_SCHEMA)
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
              children=>init_pattern_fields(?PATTERN_SCHEMA)
            }},
            % #4. ?FIELD_PATTERN
            { <<".field">>, #{
              fields=>#{
                <<".pattern">> => {?PATTERN_PATTERN,?PATTERN_PATTERN},
                <<"behaviour_module">>=>ecomet_field,
                <<"parent_pattern">>=>{?PATTERN_PATTERN,?OBJECT_PATTERN},
                <<"parents">>=>[{?PATTERN_PATTERN,?OBJECT_PATTERN}]
              },
              children=>init_pattern_fields(?FIELD_SCHEMA)
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
  % because the root database and the first node are already added to the schema, therefore when we will
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
  ?LOGINFO("DEBUG: STEP1"),

  % Step 2. Create storage objects
  StorageTree = [
    { <<".nodes">>, #{
      fields=>#{ <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN} },
      children=>[
        { atom_to_binary(node(),utf8), #{
          fields=>#{
            <<".pattern">>=>?OID(<<"/root/.patterns/.node">>),
            <<"id">>=> get_node_id( node() )
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
            <<"id">>=> get_db_id(?ROOT),
            <<"types">>=>[?RAM,?RAMDISC,?DISC],
            <<"modules">>=>#{ ?RAM => zaya_ets, ?RAMDISC => zaya_ets_rocksdb, ?DISC => zaya_rocksdb },
            <<"nodes">>=>[ node() ],
            <<"params">>=>#{ node() => ?rootParams }
          }
        }}
      ]
    }}
  ],
  init_tree({?FOLDER_PATTERN,?ROOT_FOLDER}, StorageTree),

  ?LOGINFO("DEBUG: STEP2"),

  % Step 3. Attach behaviours
  ok = ecomet:edit_object(ecomet:open(<<"/root/.patterns/.node">>),#{
    <<"behaviour_module">>=>ecomet_node
  }),
  ?LOGINFO("DEBUG: STEP3"),
  ok = ecomet:edit_object(ecomet:open(<<"/root/.patterns/.database">>),#{
    <<"behaviour_module">>=>ecomet_db
  }),
  ?LOGINFO("DEBUG: STEP4"),
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
          },
          children=>init_pattern_fields(?USERGROUP_SCHEMA)
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
  Password =
    case ?ENV(password,<<"111111">>) of
      P when is_binary(P)->P;
      P when is_list(P)-> unicode:characters_to_binary(P);
      P when is_atom(P)-> atom_to_binary(P,utf8);
      P when is_integer(P)-> integer_to_binary(P);
      P->?ERROR({invalid_password,P})
    end,
  init_tree({?FOLDER_PATTERN,?ROOT_FOLDER},[
    { <<".users">>, #{
      fields=>#{ <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN} },
      children=>[
        { <<"system">>, #{
          fields=>#{
            <<".pattern">>=>?OID(<<"/root/.patterns/.user">>),
            <<"usergroups">>=>[?OID(<<"/root/.usergroups/.administrators">>)],
            <<"password">>=>Password
          }
        }}
      ]
    }}
  ]),
  ok.

new_db_id()->
  case zaya:prev( ?SCHEMA, #dbId{k='$end'} ) of
    {#dbId{k= MaxId},_}-> MaxId + 1;
    _-> 0
  end.

new_node_id()->
  case zaya:prev( ?SCHEMA, #nodeId{k='$end'} ) of
    {#nodeId{k= MaxId},_}-> MaxId + 1;
    _-> 0
  end.

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
