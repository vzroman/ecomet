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
-module(ecomet_object_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").

%% API
-export([
  all/0,
  groups/0,
  init_per_testcase/2,
  end_per_testcase/2,
  init_per_group/2,
  end_per_group/2,
  init_per_suite/1,
  end_per_suite/1
]).


-export([
  new_id/1,
  create/1,
  construct/1,
  dirty_open/1,
  read_lock/1,
  write_lock/1,
  storage/1,
  admin_rights/1,
  write_rights/1,
  read_rights/1,
  copy/1,
  helpers/1
]).

all()->
  [
    new_id,
    create,
    {group,edit},
    storage,
    {group,check_rights},
    copy,
    helpers
  ].

groups()->[
  {edit,
    [sequence],
    [
      construct,
      dirty_open,
      read_lock,
      write_lock
    ]
  },
  {check_rights,
    [sequence],
    [
      admin_rights,
      write_rights,
      read_rights
    ]
  }
].

init_per_suite(Config)->
  ?BACKEND_INIT(),
  SuitePID=?SUITE_PROCESS_START(),

  FolderOID={2,1000},
  PatternOID={3,1000},

  % {name,type,subtype,index,required,storage,default,autoincrement}
  Object=#{
    <<".name">>=>#{ type=> string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".folder">>=>#{ type=> link, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".pattern">>=>#{ type=> link, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".readgroups">>=>#{ type=> list, subtype=>link, index=>[simple], required=>false, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".writegroups">>=>#{ type=> list, subtype=>link, index=>[simple], required=>false, storage=>ramdisc, default=>none, autoincrement=>false},
    <<"ram_field">>=>#{ type=> string, subtype=>none, index=>[simple], required=>true, storage=>ram, default=>none, autoincrement=>false} ,
    <<"disc_field">>=>#{ type=> string, subtype=>none, index=>[simple], required=>true, storage=>disc, default=>none, autoincrement=>false}
  },
  ObjectMap = ecomet_schema:register_type(PatternOID,Object),
  ct:pal("ObjectMap: ~p, ", [ObjectMap]),
  A1 = ecomet_schema:get_pattern(PatternOID),
  ct:pal("PatternOID: ~p, ", [A1]),
  Obj = ecomet_schema:get_pattern({3,1}),
  ct:pal("Obj: ~p, ", [Obj]),
  Folder = ecomet_schema:get_pattern({3,2}),
  ct:pal("Folder: ~p, ", [Folder]),
  Pat = ecomet_schema:get_pattern({3,3}),
  ct:pal("Pat: ~p, ", [Pat]),
  Field = ecomet_schema:get_pattern({3,4}),
  ct:pal("Fielld: ~p, ", [Field]),
  [{suite_pid,SuitePID},{folder_id,FolderOID},{pattern_id,PatternOID},{object_map,ObjectMap}|Config].

end_per_suite(Config)->
  ?SUITE_PROCESS_STOP(?GET(suite_pid,Config)),
  ?BACKEND_STOP(30000),
  ok.

init_per_group(check_rights,Config)->
  % Build patterns
  ecomet_user:on_init_state(),
  FolderOID=?GET(folder_id,Config),

  ObjectPatternOID={3,1001},
  UserGroupPatternOID={3,1002},
  Object=#{
    <<".name">>=>#{ type=> string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".folder">>=>#{ type=> link, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".pattern">>=>#{ type=> link, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".readgroups">>=>#{ type=> list, subtype=>link, index=>[simple], required=>false, storage=>ramdisc, default=>none, autoincrement=>false} ,
    <<".writegroups">>=>#{ type=> list, subtype=>link, index=>[simple], required=>false, storage=>ramdisc, default=>none, autoincrement=>false}
  },
  ObjectMap = ecomet_schema:register_type(ObjectPatternOID,Object),
  UserGroupMap = ecomet_schema:register_type(UserGroupPatternOID,Object),
  ct:pal("ObjectMap : ~p,UserGroupMap : ~p ",[ObjectMap,UserGroupMap]),

  UserPatternOID={3,1003},
  UserMap=#{
    <<".name">>=>#{ type=> string, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".folder">>=>#{ type=> link, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".pattern">>=>#{ type=> link, subtype=>none, index=>[simple], required=>true, storage=>ramdisc, default=>none, autoincrement=>false},
    <<".readgroups">>=>#{ type=> list, subtype=>link, index=>[simple], required=>false, storage=>ramdisc, default=>none, autoincrement=>false} ,
    <<".writegroups">>=>#{ type=> list, subtype=>link, index=>[simple], required=>false, storage=>ramdisc, default=>none, autoincrement=>false},
    <<"usergroups">>=>#{ type=> list, subtype=>link, index=>[simple], required=>false, storage=>ramdisc, default=>none, autoincrement=>false},
    <<"domains">>=>#{ type=> list, subtype=>link, index=>[simple], required=>false, storage=>ramdisc, default=>none, autoincrement=>false}
  },
  UserGroupMap1 = ecomet_schema:register_type(UserPatternOID,UserMap),
  ct:pal("UserGroupMap : ~p ",[UserGroupMap1]),

  % Build objects
  AdminGroup=ecomet:create_object(#{
    <<".name">>=><<".administrators">>,
    <<".folder">>=> FolderOID,
    <<".pattern">> => UserGroupPatternOID
  }),
  AdminGroupOID=ecomet_object:get_oid(AdminGroup),
  WriteGroup= ecomet:create_object(#{
      <<".name">>=><<"writegroup">>,
      <<".folder">>=> FolderOID,
      <<".pattern">> => UserGroupPatternOID
    }),
  WriteGroupOID=ecomet_object:get_oid(WriteGroup),
  ReadGroup=
    ecomet:create_object(#{
      <<".name">>=><<"readgroup">>,
      <<".folder">>=> FolderOID,
      <<".pattern">> => UserGroupPatternOID
    }),
  ReadGroupOID=ecomet_object:get_oid(ReadGroup),
  User=
    ecomet:create_object(#{
      <<".name">>=><<"test_user">>,
      <<".folder">>=> FolderOID,
      <<".pattern">> => UserPatternOID
    }),
  UserOID=ecomet_object:get_oid(User),
  Object1=ecomet:create_object(#{
    <<".name">>=><<"test_object">>,
    <<".folder">>=> FolderOID,
    <<".pattern">> => UserPatternOID,
    <<".readgroups">> => [ReadGroupOID],
    <<".writegroups">> => [WriteGroupOID]
  }),
  ObjectOID=ecomet_object:get_oid(Object1),
  [
    {admingroup,AdminGroupOID},
    {read_group,ReadGroupOID},
    {write_group,WriteGroupOID},
    {user,UserOID},
    {object,ObjectOID},
    {object_pattern,ObjectPatternOID},
    {usergroup_pattern,UserGroupPatternOID},
    {user_pattern,UserPatternOID}
    |Config];
init_per_group(_,Config)->
  Config.

end_per_group(check_rights,Config)->
  ecomet_user:on_init_state(),
  AdminGroupOID=?GET(admingroup,Config),
  ReadGroupOID=?GET(read_group,Config),
  WriteGroupOID=?GET(write_group,Config),
  ct:pal("WriteGroupOID :~p", [WriteGroupOID]),
  UserOID=?GET(user,Config),
  ObjectOID=?GET(object,Config),
  ObjectPatternOID=?GET(object_pattern,Config),
  UserGroupPatternOID=?GET(usergroup_pattern,Config),
  UserPatternOID=?GET(user_pattern,Config),
  AdminGroup=ecomet_object:open(AdminGroupOID,none),
  ReadGroup=ecomet_object:open(ReadGroupOID,none),
  WriteGroup=ecomet_object:open(WriteGroupOID,none),
  ct:pal("ReadGroup :~p", [ReadGroup]),
  ct:pal("WriteGroup :~p", [WriteGroup]),
  User=ecomet_object:open(UserOID,none),
  Object=ecomet_object:open(ObjectOID,none),
  DB = ecomet_object:get_db_name(AdminGroupOID),
  ecomet_object:delete(AdminGroup),
  ecomet_object:delete(ReadGroup),
  ecomet_object:delete(WriteGroup),
  ecomet_object:delete(User),
  ecomet_object:delete(Object),
  ecomet_backend:dirty_delete(DB, ?DATA, ramdisc,{ObjectPatternOID,fields}),
  ecomet_backend:dirty_delete(DB, ?DATA, ramdisc,{UserGroupPatternOID,fields}),
  ecomet_backend:dirty_delete(DB, ?DATA, ramdisc,{UserPatternOID,fields}),
  ok;
end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

new_id(_Config)->
  ecomet_user:on_init_state(),

%%  -root    -> mnt (root)
%%      |
%%      |
%%      - folder_1
%%                |
%%                |
%%                -folder_2   -> mnt (test_db)
%%                        |
%%                        |
%%                        - folder_3
%%                        |
%%                        |
%%                        - folder_4

%% FOLDER 1
  Folder1 = ecomet:create_object(#{
    <<".name">>=><<"folder_1">>,
    <<".folder">>=> {2,1},          %% On root folder
    <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN},
    <<".ts">>=>ecomet_lib:log_ts()
  }),
  ct:pal("Folder1: ~p", [Folder1]),    %% FOLDER 1 Object   OID = {2,3}
  {?FOLDER_PATTERN,ID1} = ecomet_object:get_oid(Folder1),
  IDL1 = ID1 rem ?BITSTRING_LENGTH,

%% FOLDER 2
  Folder2 = ecomet:create_object(#{
    <<".name">>=><<"folder_2">>,
    <<".folder">>=> {?FOLDER_PATTERN,ID1},     %%  OID of folder_1, to create a folder_2 in folder_1
    <<".pattern">> => {?PATTERN_PATTERN,?FOLDER_PATTERN},
    <<".ts">>=>ecomet_lib:log_ts()
  }),
  ct:pal("Folder2: ~p", [Folder2]),     %% FOLDER 1 Object   OID = {2,4}
  {?FOLDER_PATTERN,ID2} = ecomet_object:get_oid(Folder2),
  IDH2 = ID2 div ?BITSTRING_LENGTH,
  1 = ID2 rem ?BITSTRING_LENGTH - IDL1, %% It is the low id which is increment for local pattern
  0 = IDH2 rem (1 bsl 8),      %% to find the id of DB
  0 = ( IDH2 bsr 8 ) rem (1 bsl 16 ),   %% to find the id of Node
  IDHIGH2 = IDH2 bsr 24,
  ct:pal("IDHIGH2: ~p", [IDHIGH2]),


%% Add DB to the schema and mount it to folder_2
  {ok, ID_DB_TEST} = ecomet_schema:add_db(test_db),
  ok = ecomet_schema:mount_db({?FOLDER_PATTERN,ID2}, test_db),
  ct:pal("ID_DB: ~p ", [ID_DB_TEST]),

%% FOLDER 3
  {?FOLDER_PATTERN,ID3}=ecomet_object:new_id({?FOLDER_PATTERN,ID2},{?PATTERN_PATTERN,?FOLDER_PATTERN}),
  ct:pal("Folder3: ~p", [{?FOLDER_PATTERN,ID3}]),     %% FOLDER 3 Object   OID = {2,65541}
  IDH3 = ID3 div ?BITSTRING_LENGTH,
  2 = ID3 rem ?BITSTRING_LENGTH - IDL1,   %% It is the low id which is incremnt for local pattern
%%  ct:pal("IDD: ~p", [IDD]),
  1 = IDH3 rem (1 bsl 8),      %% to find the id of DB, as we've added a new db it is incremented by 1
  0 = ( IDH3 bsr 8 ) rem (1 bsl 16 ),   %% to find the id of Node
  IDHIGH3 = IDH3 bsr 24,
  ct:pal("IDHIGH3: ~p", [IDHIGH3]),
%% Add node
  { ok, NodeId } = ecomet_schema:add_node(node()),
  ct:pal("NodeId: ~p", [NodeId]),   %% new update to 1

%% FOLDER 4
  {?FOLDER_PATTERN,ID4}=ecomet_object:new_id({?FOLDER_PATTERN,ID2},{?PATTERN_PATTERN,?FOLDER_PATTERN}),
  ct:pal("Folder4: ~p", [{?FOLDER_PATTERN,ID4}]),    %% FOLDER 4 which have to have a new node_id
  IDH4 = ID4 div ?BITSTRING_LENGTH,
  3 = ID4 rem ?BITSTRING_LENGTH - IDL1,   %% It is the low id which is incremnt for local pattern
  1 = IDH4 rem (1 bsl 8),      %% to find the id of DB, as we've added a new db it is incremented by 1
  1 = ( IDH4 bsr 8 ) rem (1 bsl 16 ),   %% to find the id of Node
  IDHIGH4 = IDH4 bsr 24,
  ct:pal("IDHIGH4: ~p", [IDHIGH4]),
ok.



create(Config)->
  ecomet_user:on_init_state(),
  put(test_log_branch,ecomet_object_create),

  % Handlers
  put({ecomet_test_behaviour1,on_create},fun(TheObject)->
    TheOID=ecomet_object:get_oid(TheObject),
    ?PROCESSLOG({ecomet_test_behaviour1,{TheOID,on_create}})
  end),
  put({ecomet_test_behaviour1,on_delete},fun(TheObject)->
    TheOID=ecomet_object:get_oid(TheObject),
    ?PROCESSLOG({ecomet_test_behaviour1,{TheOID,on_delete}})
  end),
  put({ecomet_test_behaviour2,on_create},fun(TheObject)->
    TheOID=ecomet_object:get_oid(TheObject),
    ?PROCESSLOG({ecomet_test_behaviour2,{TheOID,on_create}})
  end),
  put({ecomet_test_behaviour2,on_delete},fun(TheObject)->
    TheOID=ecomet_object:get_oid(TheObject),
    ?PROCESSLOG({ecomet_test_behaviour2,{TheOID,on_delete}})
  end),

  FolderOID=?GET(folder_id,Config),
  PatternOID=?GET(pattern_id,Config),
  Object=ecomet_object:create(#{
    <<".name">>=><<"test_object1">>,
    <<".folder">>=>FolderOID,
    <<".pattern">>=>PatternOID,
    <<"ram_field">>=><<"ram_value">>,
    <<"disc_field">>=><<"disc_value">>
  }),
  ct:pal("OBJECT:: ~p",[Object]),
  OID=ecomet_object:get_oid(Object),
  DB = ecomet_object:get_db_name(OID),
  ct:pal("OID:: ~p,DB:: ~p",[OID ,DB]),
  %---------------------------------------------
  % Fields
  %---------------------------------------------
  % Ramdisc
  #{fields:=RamDiscFields} =ecomet_backend:dirty_read(DB,?DATA,ramdisc,OID),
  3=maps:size(RamDiscFields),
  <<"test_object1">>=maps:get(<<".name">>,RamDiscFields),
  FolderOID=maps:get(<<".folder">>,RamDiscFields),
  PatternOID=maps:get(<<".pattern">>,RamDiscFields),
  % Ram
  #{fields:=RamFields}=ecomet_backend:dirty_read(DB,?DATA,ram,OID),
  1=maps:size(RamFields),
  <<"ram_value">>=maps:get(<<"ram_field">>,RamFields),
  % Disc
  #{fields:=DiscFields}=ecomet_backend:dirty_read(DB,?DATA,disc,OID),
  1=maps:size(DiscFields),
  <<"disc_value">>=maps:get(<<"disc_field">>,DiscFields),
  %---------------------------------------------
  % Backtags
  %---------------------------------------------
  % Ramdisc
  #{tags:=RamDiscBacktags}=ecomet_backend:dirty_read(DB,?DATA,ramdisc,OID),
  5=maps:size(RamDiscBacktags),
  RamBackTagsName = maps:get(<<".name">>,RamDiscBacktags),
  RamBackTagsFolder = maps:get(<<".folder">>,RamDiscBacktags),
  maps:get(<<".pattern">>,RamDiscBacktags),
  ct:pal("RamBackTagsName: ~p,RamBackTagsFolder: ~p", [RamBackTagsName,RamBackTagsFolder]),
  % Ram
  #{tags:=RamBacktags}=ecomet_backend:dirty_read(DB,?DATA,ram,OID),
  1=maps:size(RamBacktags),
  maps:get(<<"ram_field">>,RamBacktags),
  % Disc
  #{tags:=DiscBacktags}=ecomet_backend:dirty_read(DB,?DATA,disc,OID),
  1=maps:size(DiscBacktags),
  maps:get(<<"disc_field">>,DiscBacktags),
  %Cleaning
  ecomet_object:delete(Object),
  not_found=ecomet_backend:dirty_read(DB,?DATA,ramdisc,OID),
  not_found=ecomet_backend:dirty_read(DB,?DATA,ram,OID),
  not_found=ecomet_backend:dirty_read(DB,?DATA,disc,OID),

  ok.

construct(Config)->
  ecomet_user:on_init_state(),
  FolderOID=?GET(folder_id,Config),
  PatternOID=?GET(pattern_id,Config),
  CreatedObject=ecomet_object:create(#{
    <<".name">>=><<"test_object2">>,
    <<".folder">>=>FolderOID,
    <<".pattern">>=>PatternOID,
    <<"ram_field">>=><<"ram_value">>,
    <<"disc_field">>=><<"disc_value">>
  }),

  OID=ecomet_object:get_oid(CreatedObject),
  Object=ecomet_object:construct(OID),
  {ok,<<"disc_value">>}=ecomet_object:read_field(Object,<<"disc_field">>),
  % Next reading from storage with cache again
  {ok,<<"disc_value">>}=ecomet_object:read_field(Object,<<"disc_field">>),

  % Read all fields
  AllFields=ecomet_object:read_all(Object),
  <<"test_object2">> =maps:get(<<".name">>,AllFields),
  FolderOID =maps:get(<<".folder">>,AllFields),
  PatternOID =maps:get(<<".pattern">>,AllFields),
  <<"ram_value">> =maps:get(<<"ram_field">>,AllFields),
  <<"disc_value">> =maps:get(<<"disc_field">>,AllFields),
  ecomet_object:delete(CreatedObject).


dirty_open(Config)->
  ecomet_user:on_init_state(),
  FolderOID=?GET(folder_id,Config),
  PatternOID=?GET(pattern_id,Config),
  CreatedObject=ecomet_object:create(#{
    <<".name">>=><<"test_object3">>,
    <<".folder">>=>FolderOID,
    <<".pattern">>=>PatternOID,
    <<"ram_field">>=><<"ram_value">>,
    <<"disc_field">>=><<"disc_value">>
  }),
  ct:pal("CreatedObject ~p",[CreatedObject]),

  OID=ecomet_object:get_oid(CreatedObject),
  Object=ecomet_object:open(OID,none),

  {ok,<<"test_object3">>}=ecomet_object:read_field(Object,<<".name">>),

  % Edit object
  ecomet_object:edit(Object,#{<<"ram_field">>=><<"ram_value_new">>}),

  DB = ecomet_object:get_db_name(OID),

  % Check new value is saved
  #{fields:=RamFields}=ecomet_backend:dirty_read(DB,?DATA,ram,OID),
  1=maps:size(RamFields),
  <<"ram_value_new">>=maps:get(<<"ram_field">>,RamFields),

  ct:pal("Folder ~p",[ecomet_object:read_field(CreatedObject,<<".folder">>)]),
  ecomet_object:delete(CreatedObject),
  ok.

read_lock(Config)->
  ecomet_user:on_init_state(),
  FolderOID=?GET(folder_id,Config),
  PatternOID=?GET(pattern_id,Config),
  CreatedObject=ecomet_object:create(#{
    <<".name">>=><<"test_object4">>,
    <<".folder">>=>FolderOID,
    <<".pattern">>=>PatternOID,
    <<"ram_field">>=><<"ram_value">>,
    <<"disc_field">>=><<"disc_value">>
  }),

  OID=ecomet_object:get_oid(CreatedObject),
  DB = ecomet_object:get_db_name(OID),
  ecomet_transaction:start(),
  Object=ecomet_object:open(OID,read),
  Parent=self(),
  % Shared lock
  ReadChild=spawn_link(fun()->
    ecomet_user:on_init_state(),
    ecomet_transaction:internal(fun()->
      ecomet_object:open(OID,read),
      Parent!{lock_acquired,self()}
    end)
 end),
  % Check child
  receive
    {lock_acquired,ReadChild}->ok
  after
    2000->?ERROR(lock_timeout)
  end,
  WriteChild=spawn_link(fun()->
    ecomet_user:on_init_state(),
    ecomet_transaction:start(),
    Parent!{lock_timeout,self()},
    ecomet_transaction:rollback()
  end),
  % Check child
  receive
    {lock_timeout,WriteChild}->ok
  after
    3000->?ERROR(wait_child_timeour)
  end,

  % Read variants
  {ok,<<"test_object4">>}=ecomet_object:read_field(Object,<<".name">>),

  % Edit routine
  % Handler
  put({ecomet_test_behaviour1,on_edit},fun(TheObject)->
    TheOID=ecomet_object:get_oid(TheObject),
    ?PROCESSLOG({ecomet_test_behaviour1,{TheOID,on_edit}}),
    {ok,<<"ram_value_new">>}=ecomet_object:read_field(TheObject,<<"ram_field">>)
   end),
  ecomet_object:edit(Object,#{<<"ram_field">>=><<"ram_value_new">>}),
  ecomet_transaction:commit(),


  % Check new value is saved
  #{fields:=RamFields}=ecomet_backend:dirty_read(DB,?DATA,ram,OID),
  1=maps:size(RamFields),
  <<"ram_value_new">>=maps:get(<<"ram_field">>,RamFields),

  ecomet_object:delete(CreatedObject),
  ok.

write_lock(Config)->
  ecomet_user:on_init_state(),
  FolderOID=?GET(folder_id,Config),
  PatternOID=?GET(pattern_id,Config),
  CreatedObject=ecomet_object:create(#{
    <<".name">>=><<"test_object5">>,
    <<".folder">>=>FolderOID,
    <<".pattern">>=>PatternOID,
    <<"ram_field">>=><<"ram_value">>,
    <<"disc_field">>=><<"disc_value">>
  }),

  OID=ecomet_object:get_oid(CreatedObject),
  ecomet_transaction:start(),
  ecomet_object:open(OID,write),
  Parent=self(),
  ReadChild=spawn_link(fun()->
    ecomet_user:on_init_state(),
    ecomet_transaction:start(),
    Parent!{lock_timeout,self()},
    ecomet_transaction:rollback()
                       end),
  % Check child
  receive
    {lock_timeout,ReadChild}->ok
  after
    3000->?ERROR(wait_child_timeour)
  end,
  ecomet_transaction:rollback(),
  ecomet_object:delete(CreatedObject).


storage(_Config)->
  ecomet_user:on_init_state(),

  TestPattern={?PATTERN_PATTERN,3001},
  Map=#{
    <<".name">>=>#{ type=> string, index=>[simple] },
    <<".folder">>=>#{ type=> link, index=>[simple] },
    <<".pattern">>=>#{ type=> link, index=>[simple] },
    <<"ram_field">>=>#{ type=> string, index=>[simple], storage=>?RAM } ,
    <<"ramdisc_field">>=>#{ type=> string, index=>[simple], storage=>?RAMDISC }
  },
  ecomet_schema:register_type(TestPattern,Map),

  Folder = ?OID(ecomet:create_object(#{
    <<".name">>=><<"test_storage">>,
    <<".folder">>=>?OID(<<"/root">>),
    <<".pattern">>=>?OID(<<"/root/.patterns/.folder">>)
  })),

  %------------Create a new object-------------------------------
  Object1=ecomet:create_object(#{
    <<".name">>=><<"obj1">>,
    <<".folder">>=>Folder,
    <<".pattern">>=>TestPattern
  }),
  % DISC
  #{
    fields:=#{
      <<".name">>:= <<"obj1">>,
      <<".folder">>:= Folder,
      <<".pattern">>:= TestPattern
    },
    tags:=#{
      <<".name">>:=[{<<"obj1">>,simple}],
      <<".folder">>:=[{Folder,simple}],
      <<".pattern">>:=[{TestPattern,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?DISC,?OID(Object1)),
  % RAM
  not_found = ecomet_backend:dirty_read(?ROOT,?DATA,?RAM,?OID(Object1)),
  %RAMDISC
  not_found = ecomet_backend:dirty_read(?ROOT,?DATA,?RAMDISC,?OID(Object1)),

  %------------Edit RAM storage-------------------------------
  ok = ecomet:edit_object(Object1,#{
    <<"ram_field">>=><<"ram_value1">>
  }),
  % DISC
  #{
    fields:=#{
      <<".name">>:= <<"obj1">>,
      <<".folder">>:= Folder,
      <<".pattern">>:= TestPattern
    },
    tags:=#{
      <<".name">>:=[{<<"obj1">>,simple}],
      <<".folder">>:=[{Folder,simple}],
      <<".pattern">>:=[{TestPattern,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?DISC,?OID(Object1)),
  % RAM
  #{
    fields:=#{
      <<"ram_field">>:= <<"ram_value1">>
    },
    tags:=#{
      <<"ram_field">>:=[{<<"ram_value1">>,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?RAM,?OID(Object1)),
  % RAMDISC
  not_found = ecomet_backend:dirty_read(?ROOT,?DATA,?RAMDISC,?OID(Object1)),

  %------------Edit RAMDISC storage-------------------------------
  ok = ecomet:edit_object(Object1,#{
    <<"ramdisc_field">>=><<"ramdisc_value1">>
  }),

  % DISC
  #{
    fields:=#{
      <<".name">>:= <<"obj1">>,
      <<".folder">>:= Folder,
      <<".pattern">>:= TestPattern
    },
    tags:=#{
      <<".name">>:=[{<<"obj1">>,simple}],
      <<".folder">>:=[{Folder,simple}],
      <<".pattern">>:=[{TestPattern,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?DISC,?OID(Object1)),
  % RAM
  #{
    fields:=#{
      <<"ram_field">>:= <<"ram_value1">>
    },
    tags:=#{
      <<"ram_field">>:=[{<<"ram_value1">>,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?RAM,?OID(Object1)),
  % RAMDISC
  #{
    fields:=#{
      <<"ramdisc_field">>:= <<"ramdisc_value1">>
    },
    tags:=#{
      <<"ramdisc_field">>:=[{<<"ramdisc_value1">>,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?RAMDISC,?OID(Object1)),

  %-------------------No real changes--------------------------------
  ok = ecomet:edit_object(Object1,#{
    <<".name">>=><<"obj1">>,
    <<"ramdisc_field">>=><<"ramdisc_value1">>
  }),
  % DISC
  #{
    fields:=#{
      <<".name">>:= <<"obj1">>,
      <<".folder">>:= Folder,
      <<".pattern">>:= TestPattern
    },
    tags:=#{
      <<".name">>:=[{<<"obj1">>,simple}],
      <<".folder">>:=[{Folder,simple}],
      <<".pattern">>:=[{TestPattern,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?DISC,?OID(Object1)),
  % RAM
  #{
    fields:=#{
      <<"ram_field">>:= <<"ram_value1">>
    },
    tags:=#{
      <<"ram_field">>:=[{<<"ram_value1">>,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?RAM,?OID(Object1)),
  % RAMDISC
  #{
    fields:=#{
      <<"ramdisc_field">>:= <<"ramdisc_value1">>
    },
    tags:=#{
      <<"ramdisc_field">>:=[{<<"ramdisc_value1">>,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?RAMDISC,?OID(Object1)),

  %------------Empty RAMDISC storage-------------------------------
  ok = ecomet:edit_object(Object1,#{
    <<"ramdisc_field">>=>none
  }),
  % DISC
  #{
    fields:=#{
      <<".name">>:= <<"obj1">>,
      <<".folder">>:= Folder,
      <<".pattern">>:= TestPattern
    },
    tags:=#{
      <<".name">>:=[{<<"obj1">>,simple}],
      <<".folder">>:=[{Folder,simple}],
      <<".pattern">>:=[{TestPattern,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?DISC,?OID(Object1)),
  % RAM
  #{
    fields:=#{
      <<"ram_field">>:= <<"ram_value1">>
    },
    tags:=#{
      <<"ram_field">>:=[{<<"ram_value1">>,simple}]
    }
  } = ecomet_backend:dirty_read(?ROOT,?DATA,?RAM,?OID(Object1)),
  % RAMDISC
  not_found = ecomet_backend:dirty_read(?ROOT,?DATA,?RAMDISC,?OID(Object1)),

  %------------Delete the object-------------------------------
  ok=ecomet:delete_object(Object1),
  % DISC
  not_found = ecomet_backend:dirty_read(?ROOT,?DATA,?DISC,?OID(Object1)),
  % RAM
  not_found = ecomet_backend:dirty_read(?ROOT,?DATA,?RAM,?OID(Object1)),
  %RAMDISC
  not_found = ecomet_backend:dirty_read(?ROOT,?DATA,?RAMDISC,?OID(Object1)),

  ok.

admin_rights(Config)->
  ecomet_user:on_init_state(),
  AdminGroupOID=?GET(admingroup,Config),
  UserOID=?GET(user,Config),
  ObjectOID=?GET(object,Config),

  ecomet_user:on_init_state(),
  User=ecomet_object:open(UserOID,none),
  ecomet_object:edit(User,#{<<"usergroups">>=>[AdminGroupOID]}),
  % Login as test_user
%%  ecomet_user:set_state(UserOID,none,[AdminGroupOID]),
  Object=ecomet_object:open(ObjectOID,none),
  ecomet_object:edit(Object,#{<<".name">>=><<"test_object1">>}),
  % Cleaning
  ecomet_object:edit(User,#{<<"usergroups">>=>none}).

write_rights(Config)->
  ecomet_user:on_init_state(),
  WriteGroupOID=?GET(write_group,Config),
  UserOID=?GET(user,Config),
  ObjectOID=?GET(object,Config),

  ecomet_user:on_init_state(),
  User=ecomet_object:open(UserOID,none),
  ecomet_object:edit(User,#{<<"usergroups">>=>[WriteGroupOID]}),
  % Login as test_user
%%  ecomet_user:set_state(UserOID,none,[WriteGroupOID]),
  Object=ecomet_object:open(ObjectOID,none),
  ecomet_object:edit(Object,#{<<".writegroups">>=>[UserOID]}),

  ecomet_user:on_init_state(),
  ecomet_object:edit(User,#{<<"usergroups">>=>none}),
%%  ecomet_user:set_state(UserOID,none,none),

  Object1=ecomet_object:open(ObjectOID,none),
  ecomet_object:edit(Object1,#{<<".writegroups">>=>[WriteGroupOID]}).


read_rights(Config)->
  ecomet_user:on_init_state(),
  ReadGroupOID=?GET(read_group,Config),
  UserOID=?GET(user,Config),
  ObjectOID=?GET(object,Config),

  ecomet_user:on_init_state(),
  User=ecomet_object:open(UserOID,none),
  ecomet_object:edit(User,#{<<"usergroups">>=>[ReadGroupOID]}),
  % Login as test_user
%%  ecomet_user:set_state(UserOID,none,[ReadGroupOID]),
  Object=ecomet_object:open(ObjectOID,none),
  {ok,_}=ecomet_object:read_field(Object,<<".name">>),
  % No write rights

  ecomet_user:on_init_state(),
  Object1=ecomet_object:open(ObjectOID,none),
  ecomet_object:edit(Object1,#{<<".readgroups">>=>[UserOID]}),
  ecomet_object:edit(User,#{<<"usergroups">>=>none}),
%%  ecomet_user:set_state(UserOID,none,none),

  Object2=ecomet_object:open(ObjectOID,none),
  {ok,_}=ecomet_object:read_field(Object2,<<".name">>),
  % No write rights

  ecomet_user:on_init_state(),
  ecomet_object:edit(Object1,#{<<".readgroups">>=>[ReadGroupOID]}).

copy(Config)->
  ecomet_user:on_init_state(),
  FolderOID=?GET(folder_id,Config),
  PatternOID=?GET(pattern_id,Config),
  Object1=ecomet_object:create(#{
    <<".name">>=><<"object1">>,
    <<".folder">>=>FolderOID,
    <<".pattern">>=>PatternOID,
    <<"ram_field">>=><<"ram_value">>,
    <<"disc_field">>=><<"disc_value">>
  }),
  OID1=ecomet_object:get_oid(Object1),
  Object2=ecomet_object:copy(Object1,#{<<".name">>=><<"object2">>}),
  OID2=ecomet_object:get_oid(Object2),

  Opened1=ecomet_object:open(OID1,none),
  {ok,<<"object1">>}=ecomet_object:read_field(Opened1,<<".name">>),
  Opened2=ecomet_object:open(OID2,none),
  {ok,<<"object2">>}=ecomet_object:read_field(Opened2,<<".name">>),
  ecomet_object:delete(Opened1),
  ecomet_object:delete(Opened2).


helpers(Config)->
  ecomet_user:on_init_state(),
  FolderOID=?GET(folder_id,Config),
  PatternOID=?GET(pattern_id,Config),
  Object=ecomet_object:create(#{
    <<".name">>=><<"object5">>,
    <<".folder">>=>FolderOID,
    <<".pattern">>=>PatternOID,
    <<"ram_field">>=><<"ram_value">>,
    <<"disc_field">>=><<"disc_value">>
  }),
  OID=ecomet_object:get_oid(Object),
  ecomet_transaction:internal(fun()->
    Opened=ecomet_object:open(OID),
    ecomet_object:edit(Opened,#{<<"ram_field">>=><<"ram_value_new">>}),
    {<<"ram_value_new">>,<<"ram_value">>}=ecomet_object:field_changes(Opened,<<"ram_field">>),
    none=ecomet_object:field_changes(Opened,<<".name">>)
  end),
  root=ecomet_object:get_db_name(OID),
  ecomet_object:delete(Object).





















