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
-module(ecomet_backend_SUITE).

-include("ecomet_test.hrl").

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
  test_root_db/1,
  test_add_remove_db/1,
  test_dirty_operations/1,
  test_transactions/1
]).

all()->
  [
    test_root_db
    ,test_add_remove_db
    ,test_dirty_operations
    ,test_transactions
  ].

groups()->
  [].

init_per_suite(Config)->
  ?BACKEND_INIT(),
  Config.
end_per_suite(_Config)->
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

%%========================================================================
%%    Test the root database availability and type
%%========================================================================
test_root_db(_Config)->
  %%----------Object storage--------------------------------------------
  StorageList = ecomet_backend:get_storages(),
  ct:pal("Storage list ~p",[StorageList]),
  [] = [
    % Object storage
    ecomet_root_data_ramlocal,
    ecomet_root_data_ram,
    ecomet_root_data_ramdisc,
    ecomet_root_data_disc,
    % Index storage
    ecomet_root_index_ramlocal,
    ecomet_root_index_ram,
    ecomet_root_index_ramdisc,
    ecomet_root_index_disc
  ] -- StorageList,

  % Storage types
  ramlocal = ecomet_backend:get_storage_type(ecomet_root_data_ramlocal),
  ram = ecomet_backend:get_storage_type(ecomet_root_data_ram),
  ramdisc = ecomet_backend:get_storage_type(ecomet_root_data_ramdisc),
  disc = ecomet_backend:get_storage_type(ecomet_root_data_disc),

  ramlocal = ecomet_backend:get_storage_type(ecomet_root_index_ramlocal),
  ram = ecomet_backend:get_storage_type(ecomet_root_index_ram),
  ramdisc = ecomet_backend:get_storage_type(ecomet_root_index_ramdisc),
  disc = ecomet_backend:get_storage_type(ecomet_root_index_disc),

  ok.

test_add_remove_db(_Config)->

  %--------------Add----------------------------------
  BeforeAdd = ecomet_backend:get_storages(),

  ok = ecomet_backend:create_db(test),

  [ ] = (ecomet_backend:get_storages() -- BeforeAdd) -- [
    % Object storage
    ecomet_test_data_ramlocal,
    ecomet_test_data_ram,
    ecomet_test_data_ramdisc,
    ecomet_test_data_disc,
    % Index storage
    ecomet_test_index_ramlocal,
    ecomet_test_index_ram,
    ecomet_test_index_ramdisc,
    ecomet_test_index_disc
  ],

  % Storage types
  ramlocal = ecomet_backend:get_storage_type(ecomet_test_data_ramlocal),
  ram = ecomet_backend:get_storage_type(ecomet_test_data_ram),
  ramdisc = ecomet_backend:get_storage_type(ecomet_test_data_ramdisc),
  disc = ecomet_backend:get_storage_type(ecomet_test_data_disc),

  ramlocal = ecomet_backend:get_storage_type(ecomet_test_index_ramlocal),
  ram = ecomet_backend:get_storage_type(ecomet_test_index_ram),
  ramdisc = ecomet_backend:get_storage_type(ecomet_test_index_ramdisc),
  disc = ecomet_backend:get_storage_type(ecomet_test_index_disc),

  %------------Remove-----------------------------------------
  ok = ecomet_backend:remove_db(test),
  BeforeAdd = ecomet_backend:get_storages(),

  ok.

%%========================================================================
%   Simple dirty operations
%%========================================================================
test_dirty_operations(_Config)->
  %----------------Object ramlocal storage-----------------------------------------
%%  RamLocalSize=ecomet_backend:fragment_size(ecomet_sys_object_ramlocal),
%%  ok=ecomet_backend:dirty_write(<<"sys">>,object,ramlocal,#ecomet_keyvalue{key= <<"key1">>, value= <<"value1">>}),
%%  % +1 record
%%  true = (RamLocalSize+1) == ecomet_backend:fragment_size(ecomet_sys_object_ramlocal),
%%  % Record is available now
%%  [#ecomet_keyvalue{key= <<"key1">>, value= <<"value1">>}]=ecomet_backend:dirty_read(<<"sys">>,object,ramlocal,<<"key1">>,false),
%%  ok=ecomet_backend:dirty_delete(<<"sys">>,object,ramlocal,<<"key1">>),
%%  % Record deleted, -1 record
%%  RamLocalSize=ecomet_backend:fragment_size(ecomet_sys_object_ramlocal),
%%  % No record for the key now
%%  []=ecomet_backend:dirty_read(<<"sys">>,object,ramlocal,<<"key1">>,false),
%%
%%  % Ramlocal storage is always in sys domain
%%  ok=ecomet_backend:dirty_write(<<"somedomain">>,object,ramlocal,#ecomet_keyvalue{key= <<"key1">>, value= <<"value1">>}),
%%  % +1 record in sys domain
%%  true = (RamLocalSize+1) == ecomet_backend:fragment_size(ecomet_sys_object_ramlocal),
%%  % Record is available in sys domain
%%  [#ecomet_keyvalue{key= <<"key1">>, value= <<"value1">>}]=ecomet_backend:dirty_read(<<"sys">>,object,ramlocal,<<"key1">>,false),
%%  ok=ecomet_backend:dirty_delete(<<"somedomain">>,object,ramlocal,<<"key1">>),
%%  % No record for the key now
%%  []=ecomet_backend:dirty_read(<<"somedomain">>,object,ramlocal,<<"key1">>,false),
%%
%%  %----------------Object ram storage-----------------------------------------
%%  RamSize=get_storage_size(ecomet_sys_object_ram),
%%  ok=ecomet_backend:dirty_write(<<"sys">>,object,ram,#ecomet_keyvalue{key= <<"key2">>, value= <<"value2">>}),
%%  % +1 record
%%  true = (RamSize+1) == get_storage_size(ecomet_sys_object_ram),
%%  % Record is available now
%%  [#ecomet_keyvalue{key= <<"key2">>, value= <<"value2">>}]=ecomet_backend:dirty_read(<<"sys">>,object,ram,<<"key2">>,false),
%%  ok=ecomet_backend:dirty_delete(<<"sys">>,object,ram,<<"key2">>),
%%  % Record deleted, -1 record
%%  RamSize=get_storage_size(ecomet_sys_object_ram),
%%  % No record for the key now
%%  []=ecomet_backend:dirty_read(<<"sys">>,object,ram,<<"key2">>,false),
%%
%%  %----------------Object ramdisc storage-----------------------------------------
%%  RamDiscSize=get_storage_size(ecomet_sys_object_ramdisc),
%%  ok=ecomet_backend:dirty_write(<<"sys">>,object,ramdisc,#ecomet_keyvalue{key= <<"key3">>, value= <<"value3">>}),
%%  % +1 record
%%  true = (RamDiscSize+1) == get_storage_size(ecomet_sys_object_ramdisc),
%%  % Record is available now
%%  [#ecomet_keyvalue{key= <<"key3">>, value= <<"value3">>}]=ecomet_backend:dirty_read(<<"sys">>,object,ramdisc,<<"key3">>,false),
%%  ok=ecomet_backend:dirty_delete(<<"sys">>,object,ramdisc,<<"key3">>),
%%  % Record deleted, -1 record
%%  RamDiscSize=get_storage_size(ecomet_sys_object_ramdisc),
%%  % No record for the key now
%%  []=ecomet_backend:dirty_read(<<"sys">>,object,ramdisc,<<"key3">>,false),
%%
%%  %----------------Object discbuf storage-----------------------------------------
%%  % Disc operations is performed in discbuf storage, and discbuf records
%%  % are moved to disc only within discbuf clean procedure
%%  DiscBufSize=get_storage_size(ecomet_sys_object_discbuf),
%%  ok=ecomet_backend:dirty_write(<<"sys">>,object,disc,#ecomet_keyvalue{key= <<"key4">>, value= <<"value4">>}),
%%  % +1 record
%%  true = (DiscBufSize+1) == get_storage_size(ecomet_sys_object_discbuf),
%%  % Record is available now
%%  [#ecomet_keyvalue{key= <<"key4">>, value= <<"value4">>}]=ecomet_backend:dirty_read(<<"sys">>,object,disc,<<"key4">>,false),
%%  ok=ecomet_backend:dirty_delete(<<"sys">>,object,disc,<<"key4">>),
%%  % Record is deleted, but it is still stored in discbuf storage as deleted,
%%  % and is to be deleted only within discbuf clean procedure
%%  true = (DiscBufSize+1) == get_storage_size(ecomet_sys_object_discbuf),
%%  % No record for the key now
%%  []=ecomet_backend:dirty_read(<<"sys">>,object,disc,<<"key4">>,false),
%%  % Clean deleted record
%%  ok=ecomet_backend:clean_discbuf_fragment(<<"ecomet_sys_object_discbuf">>,100.0),

  ok.

test_transactions(_Config)->
  ok.