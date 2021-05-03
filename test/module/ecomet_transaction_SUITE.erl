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

-module(ecomet_transaction_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").


-define(TKEY,eCoMeT_tRaNsAcTiOn).
-record(state,{locks,dict,log,droplog,parent,oncommit,type,dirty}).

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
  internal_simple_dict/1,
  internal_simple_log/1,
  internal_simple_oncommit/1,
  internal_simple_locks/1,
  internal_nested_dict/1,
  internal_nested_log/1,
  internal_nested_oncommit/1,
  internal_nested_locks/1,
  external_simple_operations/1,
  external_simple_locks/1,
  external_nested_operations/1,
  external_nested_locks/1
]).

all()->
  [
    {group,internal},
    {group,external}
  ].

groups()->[
  {internal,
    [sequence],
    [
      {simple,
        [sequence],
        [
          internal_simple_dict,
          internal_simple_log,
          internal_simple_oncommit,
          internal_simple_locks
        ]
      },
      {nested,
        [sequence],
        [
          internal_nested_dict,
          internal_nested_log,
          internal_nested_oncommit,
          internal_nested_locks
        ]
      }
    ]
  },
  {external,
    [sequence],
    [
      {simple,
        [sequence],
        [
          external_simple_operations,
          external_simple_locks
        ]
      },
      {nested,
        [sequence],
        [
          external_nested_operations,
          external_nested_locks
        ]
      }
    ]
  }
].

init_per_suite(Config)->
  ?BACKEND_INIT(),
  [{suite_pid,?SUITE_PROCESS_START()}|Config].

end_per_suite(Config)->
  ?SUITE_PROCESS_STOP(?GET(suite_pid,Config)),
  ?BACKEND_STOP(30000),
  ok.

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

%%--------------------------------------------------------------
%%  Dictionary routine
%%--------------------------------------------------------------
internal_simple_dict(_Config)->
  % No transaction
  ?assertError(no_transaction,ecomet_transaction:dict_put([{key1,value1}])),
  ?assertError(no_transaction,ecomet_transaction:dict_get(key1)),
  default_value1=ecomet_transaction:dict_get(key1,default_value1),
  % in transaction
  {ok,ok}=ecomet_transaction:internal(fun()->
    ecomet_transaction:dict_put([{key1,value1}]),
    value1=ecomet_transaction:dict_get(key1),
    ?assertError({badkey,key2},ecomet_transaction:dict_get(key2)),
    default_value2=ecomet_transaction:dict_get(key2,default_value2),
    ecomet_transaction:dict_remove([key1]),
    no_value1=ecomet_transaction:dict_get(key1,no_value1),
    ok
  end).

%%--------------------------------------------------------------
%%  Commit log routine
%%--------------------------------------------------------------
internal_simple_log(_Config)->
  OID={10000,1},
  ?assertError(no_transaction,ecomet_transaction:queue_commit(OID)),
  {ok,ok}=ecomet_transaction:internal(fun()->
    ecomet_transaction:queue_commit(OID),
    #state{log=[OID],droplog=[OID]}=get(?TKEY),
    ecomet_transaction:apply_commit(OID),
    #state{log=[OID],droplog=[]}=get(?TKEY),
    ecomet_transaction:queue_commit({10000,2}),
    #state{log=[{10000,2},OID]}=get(?TKEY),
    ecomet_transaction:queue_commit(OID),
    State=#state{log=[{10000,2},OID]}=get(?TKEY),
    % Clean log
    put(?TKEY,State#state{log=[]}),
    ok
  end).

%%--------------------------------------------------------------
%%  on_commits routine
%%--------------------------------------------------------------
internal_simple_oncommit(_Config)->
  ?assertError(no_transaction,ecomet_transaction:on_commit(fun()->ok end)),
  {ok,ok}=ecomet_transaction:internal(fun()->
    ?assertError(not_function,ecomet_transaction:on_commit(invalid_fun)),
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun1) end),
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun2) end),
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun3) end),
    ok
    end),
  [fun1,fun2,fun3]= ?GETLOG(),
  % Transaction rollback
  {error,_}=ecomet_transaction:internal(fun()->
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun1) end),
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun2) end),
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun3) end),
    ?ERROR(test)
  end).

%%--------------------------------------------------------------
%%  Locks routine
%%--------------------------------------------------------------
internal_simple_locks(_Config)->
  ecomet_backend:dirty_write(?ROOT,?DATA,ramdisc,lock_key,lock_value),
  Key={?ROOT,?DATA,ramdisc,lock_key},
  ?assertError(no_transaction,ecomet_transaction:lock(Key,read,none)),
  {ok,_}=ecomet_transaction:internal(fun()->
    {ok,lock_value}=ecomet_transaction:lock(Key,read,none),
    {lock_value,read}=ecomet_transaction:find_lock(Key),
    % shared lock
    Parent=self(),
    ReadChild=spawn_link(fun()->
      ecomet_transaction:internal(fun()->
        {ok,lock_value}=ecomet_transaction:lock(Key,read,none),
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
      ecomet_transaction:internal(fun()->
        ecomet_transaction:lock(Key,write,none),
        Parent!{lock_acquired,self()}
      end)
    end),
    % Write must fail while read active
    receive
      {lock_acquired,WriteChild}->?ERROR(write_acquired)
    after
      2000->ok
    end,

    % Upgrade lock
    {ok,lock_value}=ecomet_transaction:lock(Key,write,none),
    {lock_value,write}=ecomet_transaction:find_lock(Key),

    ReadChild1=spawn_link(fun()->
      ecomet_transaction:internal(fun()->
        ecomet_transaction:lock(Key,read,none),
        Parent!{lock_acquired,self()}
      end)
    end),
    % Read must fail while write active
    receive
      {lock_acquired,ReadChild1}->?ERROR(read_acquired)
    after
      2000->ok
    end
   end),
  ecomet_backend:dirty_delete(?ROOT,?DATA,ramdisc,lock_key).


%%--------------------------------------------------------------
%%  nested transaction
%%--------------------------------------------------------------
internal_nested_dict(_Config)->
  {ok,_}=ecomet_transaction:internal(fun()->
    ecomet_transaction:dict_put([
      {parent_key1,parent_value1},
      {parent_key2,parent_value2}
    ]),
    ecomet_transaction:internal(fun()->
      parent_value1=ecomet_transaction:dict_get(parent_key1),
      parent_value2=ecomet_transaction:dict_get(parent_key2),
      ecomet_transaction:dict_put([
        {parent_key2,parent_value2_new},
        {child_key1,child_value1}
      ]),
      parent_value2_new=ecomet_transaction:dict_get(parent_key2),
      child_value1=ecomet_transaction:dict_get(child_key1),
      ?ERROR(rollback)
    end),
    parent_value2=ecomet_transaction:dict_get(parent_key2),
    no_child_value1=ecomet_transaction:dict_get(child_key1,no_child_value1),
    {ok,_}=ecomet_transaction:internal(fun()->
      ecomet_transaction:dict_put([
        {parent_key2,parent_value2_new},
        {child_key1,child_value1}
      ]),
      parent_value2_new=ecomet_transaction:dict_get(parent_key2),
      child_value1=ecomet_transaction:dict_get(child_key1)
     end),
    parent_value2_new=ecomet_transaction:dict_get(parent_key2),
    child_value1=ecomet_transaction:dict_get(child_key1)
  end).

internal_nested_log(_Config)->
  OID1={10000,1},
  OID2={10000,2},
  OID3={10000,3},
  {ok,_}=ecomet_transaction:internal(fun()->
    ecomet_transaction:queue_commit(OID1),
    ecomet_transaction:queue_commit(OID2),
    #state{log=[OID2,OID1],droplog=[OID2,OID1]}=get(?TKEY),
    ecomet_transaction:internal(fun()->
      #state{log=[]}=get(?TKEY),
      ecomet_transaction:queue_commit(OID3),
      #state{log=[OID3],droplog=[OID3]}=get(?TKEY)
    end),
    #state{log=[OID2,OID1],droplog=[OID2,OID1]}=get(?TKEY),

    ecomet_transaction:internal(fun()->
      ecomet_transaction:queue_commit(OID3),
      ecomet_transaction:apply_commit(OID3),
      #state{log=[OID3],droplog=[]}=get(?TKEY),
      ?ERROR(rollback)
    end),
    #state{log=[OID2,OID1],droplog=[OID2,OID1]}=get(?TKEY),

    {ok,_}=ecomet_transaction:internal(fun()->
      ecomet_transaction:queue_commit(OID3),
      ecomet_transaction:queue_commit(OID1),
      ecomet_transaction:apply_commit(OID3),
      ecomet_transaction:apply_commit(OID1),
      #state{log=[OID1,OID3],droplog=[]}=get(?TKEY)
    end),
    State=#state{log=[OID3,OID2,OID1],droplog=[OID2]}=get(?TKEY),
    put(?TKEY,State#state{log=[]})
  end).

internal_nested_oncommit(_Config)->
  % Transaction rollback
  {ok,_}=ecomet_transaction:internal(fun()->
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun1) end),
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun2) end),
    ecomet_transaction:internal(fun()->
      ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun21) end),
      ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun22) end),
      ?ERROR(rollback)
    end),
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun3) end)
  end),
  [fun1,fun2,fun3]= ?GETLOG(),

  % Transaction commit
  {ok,_}=ecomet_transaction:internal(fun()->
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun1) end),
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun2) end),
    ecomet_transaction:internal(fun()->
      ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun21) end),
      ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun22) end)
    end),
    ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun3) end)
  end),
  [fun1,fun2,fun21,fun22,fun3]= ?GETLOG().


internal_nested_locks(_Config)->
  ecomet_backend:dirty_write(?ROOT,?DATA,ramdisc,lock_key1,lock_value1),
  Key1={?ROOT,?DATA,ramdisc,lock_key1},
  Key2={?ROOT,?DATA,ramdisc,lock_key2},
  Key3={?ROOT,?DATA,ramdisc,lock_key3},
  {ok,_}=ecomet_transaction:internal(fun()->
    {ok,lock_value1}=ecomet_transaction:lock(Key1,read,none),
    % Commit
    {ok,_}=ecomet_transaction:internal(fun()->
      {lock_value1,read}=ecomet_transaction:find_lock(Key1),
      {ok,not_found}=ecomet_transaction:lock(Key2,read,none),
      % Upgrade lock
      {ok,lock_value1}=ecomet_transaction:lock(Key1,write,none)
    end),
    {lock_value1,write}=ecomet_transaction:find_lock(Key1),
    {not_found,read}=ecomet_transaction:find_lock(Key2),
    % Rollback
    ecomet_transaction:internal(fun()->
      {ok,not_found}=ecomet_transaction:lock(Key3,read,none),
      ?ERROR(rollback)
    end),
    {lock_value1,write}=ecomet_transaction:find_lock(Key1),
    {not_found,read}=ecomet_transaction:find_lock(Key2),
    % We hold locks even if transaction failed until main transaction finish
    {not_found,read}=ecomet_transaction:find_lock(Key3)
  end),
  ecomet_backend:dirty_delete(?ROOT,?DATA,ramdisc,lock_key).


%%--------------------------------------------------------------
%%  External transaction
%%--------------------------------------------------------------
external_simple_operations(_Config)->
  ecomet_transaction:start(),
  external=ecomet_transaction:get_type(),
  ecomet_transaction:dict_put([{key1,value1}]),
  value1=ecomet_transaction:dict_get(key1),
  ecomet_transaction:queue_commit({10000,1}),
  #state{log=[{10000,1}],droplog=[{10000,1}]}=get(?TKEY),
  ecomet_transaction:apply_commit({10000,1}),
  State=#state{log=[{10000,1}],droplog=[]}=get(?TKEY),
  put(?TKEY,State#state{log=[]}),
  ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun1) end),
  ecomet_transaction:commit(),
  [fun1]= ?GETLOG().



external_simple_locks(_Config)->
  ecomet_backend:dirty_write(?ROOT,?DATA,ramdisc,lock_key1,lock_value1),
  Key1={?ROOT,?DATA,ramdisc,lock_key1},

  ecomet_transaction:start(),
  {ok,lock_value1}=ecomet_transaction:lock(Key1,read,2000),

  {lock_value1,read}=ecomet_transaction:find_lock(Key1),
  Parent=self(),
  % Shared lock
  ReadChild1=spawn_link(fun()->
    ecomet_transaction:internal(fun()->
      {ok,lock_value1}=ecomet_transaction:lock(Key1,read,none),
      Parent!{lock_acquired,self()}
    end)
  end),
  % Check child
  receive
    {lock_acquired,ReadChild1}->ok
  after
    2000->?ERROR(lock_timeout)
  end,
  % Write lock
  {ok,lock_value1}=ecomet_transaction:lock(Key1,write,2000),
  ReadChild2=spawn_link(fun()->
    ecomet_transaction:start(),
    case ecomet_transaction:lock(Key1,read,2000) of
      {error,timeout}->Parent!{lock_timeout,self()};
      {ok,lock_value1}->Parent!{lock_acquired,self()};
      Invalid->Parent!{invalid_result,Invalid,self()}
    end,
    ecomet_transaction:rollback()
  end),
  % Check child
  receive
    {lock_timeout,ReadChild2}->ok;
    {lock_acquired,ReadChild2}->?ERROR(read_acquired);
    Invalid->?ERROR(Invalid)
  after
    5000->?ERROR(wait_child_timeour)
  end,
  % obtain same lock
  {ok,lock_value1}=ecomet_transaction:lock(Key1,write,2000),
  ecomet_transaction:commit(),
  % Check locks are released
  ReadChild3=spawn_link(fun()->
    ecomet_transaction:internal(fun()->
      {ok,lock_value1}=ecomet_transaction:lock(Key1,read,none),
      Parent!{lock_acquired,self()}
    end)
  end),
  receive
    {lock_acquired,ReadChild3}->ok
  after
    2000->?ERROR(lock_timeout)
  end,
  %-------------------------------------------------------
  % Rollback
  %-------------------------------------------------------
  WriteChild1=spawn(fun()->
    ecomet_transaction:start(),
    {ok,lock_value1}=ecomet_transaction:lock(Key1,write,2000),
    Parent!{locked,self()},
    ecomet_transaction:rollback(),
    receive
      {stop,Parent}->ok
    end
  end),
  receive
    {locked,WriteChild1}->ok
  after
    2000->?ERROR(child_timeout)
  end,
  ecomet_transaction:start(),
  {ok,lock_value1}=ecomet_transaction:lock(Key1,write,1000),
  WriteChild1!{stop,self()},
  ecomet_transaction:rollback(),
  %-------------------------------------------------------
  % Process crash
  %-------------------------------------------------------
  WriteChild2=spawn(fun()->
    ecomet_transaction:start(),
    {ok,lock_value1}=ecomet_transaction:lock(Key1,write,2000),
    Parent!{locked,self()},
    ?ERROR(test_crash)
  end),
  receive
    {locked,WriteChild2}->ok
  after
    2000->?ERROR(child_timeout)
  end,
  ecomet_transaction:start(),
  {ok,lock_value1}=ecomet_transaction:lock(Key1,write,1000),
  ecomet_transaction:rollback(),
  %-------------------------------------------------------
  % Normal terminating
  %-------------------------------------------------------
  WriteChild3=spawn(fun()->
    ecomet_transaction:start(),
    {ok,lock_value1}=ecomet_transaction:lock(Key1,write,2000),
    Parent!{locked,self()}
  end),
  receive
    {locked,WriteChild3}->ok
  after
    2000->?ERROR(child_timeout)
  end,
  ecomet_transaction:start(),
  {ok,lock_value1}=ecomet_transaction:lock(Key1,write,1000),
  ecomet_transaction:rollback(),
  %-------------------------------------------------------
  % Release lock
  %-------------------------------------------------------
  WriteChild4=spawn(fun()->
    ecomet_transaction:start(),
    {ok,lock_value1}=ecomet_transaction:lock(Key1,read,2000),
    % test upgrade by the way
    {ok,lock_value1}=ecomet_transaction:lock(Key1,write,2000),
    Parent!{locked,self()},
    ecomet_transaction:release_lock(Key1),
    timer:sleep(5000)
  end),
  receive
    {locked,WriteChild4}->ok
  after
    2000->?ERROR(child_timeout)
  end,
  ecomet_transaction:start(),
  {ok,lock_value1}=ecomet_transaction:lock(Key1,write,1000),
  ecomet_transaction:rollback().



%%--------------------------------------------------------------
%%  Nested external transaction
%%--------------------------------------------------------------
external_nested_operations(_Config)->
  {ok,_}=ecomet_transaction:internal(fun()->
    ?assertError(external_transaction_within_internal,ecomet_transaction:start())
  end),
  %--------------------------------------------------
  % Rollback
  %--------------------------------------------------
  ecomet_transaction:start(),
  {ok,_}=ecomet_transaction:internal(fun()->
    % parent is external. Sub run as external, even if it is claimed as internal
    external=ecomet_transaction:get_type()
   end),
  ecomet_transaction:dict_put([{key1,value1}]),
  ecomet_transaction:queue_commit({10000,1}),
  ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun1) end),
  % start nested transaction
  ecomet_transaction:start(),
  % parent dict is available
  value1=ecomet_transaction:dict_get(key1),
  #state{log=[]}=get(?TKEY),
  ecomet_transaction:dict_put([{key2,value2}]),
  ecomet_transaction:queue_commit({10000,2}),
  ecomet_transaction:apply_commit({10000,2}),
  ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun2) end),
  ecomet_transaction:rollback(),
  value1=ecomet_transaction:dict_get(key1),
  no_value2=ecomet_transaction:dict_get(key2,no_value2),
  State1=#state{log=[{10000,1}],droplog=[{10000,1}]}=get(?TKEY),
  put(?TKEY,State1#state{log=[]}),
  ecomet_transaction:commit(),
  [fun1]= ?GETLOG(),
  %--------------------------------------------------
  % Commit
  %--------------------------------------------------
  ecomet_transaction:start(),
  ecomet_transaction:dict_put([{key1,value1}]),
  ecomet_transaction:queue_commit({10000,1}),
  ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun1) end),
  % start nested transaction
  ecomet_transaction:start(),
  ecomet_transaction:dict_put([{key2,value2}]),
  ecomet_transaction:queue_commit({10000,2}),
  ecomet_transaction:apply_commit({10000,2}),
  ecomet_transaction:on_commit(fun()->?PROCESSLOG(fun2) end),
  ecomet_transaction:commit(),
  value1=ecomet_transaction:dict_get(key1),
  value2=ecomet_transaction:dict_get(key2),
  State2=#state{log=[{10000,2},{10000,1}],droplog=[{10000,1}]}=get(?TKEY),
  put(?TKEY,State2#state{log=[]}),
  ecomet_transaction:commit(),
  [fun1,fun2]= ?GETLOG().



external_nested_locks(_Config)->
  ecomet_backend:dirty_write(?ROOT,?DATA,ramdisc,lock_key1,lock_value1),
  Key1={?ROOT,?DATA,ramdisc,lock_key1},
  Key2={?ROOT,?DATA,ramdisc,lock_key2},
  Key3={?ROOT,?DATA,ramdisc,lock_key3},
  ecomet_transaction:start(),
  {ok,lock_value1}=ecomet_transaction:lock(Key1,read,none),
  % Commit
  ecomet_transaction:start(),
  {lock_value1,read}=ecomet_transaction:find_lock(Key1),
  {ok,not_found}=ecomet_transaction:lock(Key2,read,none),
  % Upgrade lock
  {ok,lock_value1}=ecomet_transaction:lock(Key1,write,none),
  ecomet_transaction:commit(),
  {lock_value1,write}=ecomet_transaction:find_lock(Key1),
  {not_found,read}=ecomet_transaction:find_lock(Key2),
  % Rollback
  ecomet_transaction:start(),
  {ok,not_found}=ecomet_transaction:lock(Key3,read,none),
  ecomet_transaction:rollback(),
  {lock_value1,write}=ecomet_transaction:find_lock(Key1),
  {not_found,read}=ecomet_transaction:find_lock(Key2),
  % We hold locks even if transaction failed until main transaction finish
  {not_found,read}=ecomet_transaction:find_lock(Key3),
  ecomet_transaction:rollback(),
  ecomet_backend:dirty_delete(?ROOT,?DATA,ramdisc,lock_key).


