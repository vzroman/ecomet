%%-----------------------------------------------------------------
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this file,
%% You can obtain one at http://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2015, Invent Technology
%% Author: Vozzhenikov Roman, vzroman@gmail.com.
%%
%% Alternatively, the contents of this file may be used under the terms
%% of the GNU General Public License Version 2.0, as described below:
%%
%% This file is free software: you may copy, redistribute and/or modify
%% it under the terms of the GNU General Public License as published by the
%% Free Software Foundation, either version 2.0 of the License, or (at your
%% option) any later version.
%%
%% This file is distributed in the hope that it will be useful, but
%% WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
%% Public License for more details.
%%
%% You should have received a copy of the GNU General Public License
%% along with this program. If not, see http://www.gnu.org/licenses/.
%%
%%-----------------------------------------------------------------
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
  test_sys_storages/1,
  test_dirty_operations/1
]).

all()->
  [
    test_sys_storages
    ,test_dirty_operations
  ].

groups()->
  [].

%% Init system storages
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
%%    Test system storages availability and type
%%========================================================================
test_sys_storages(_Config)->
%%  %%----------Object storage--------------------------------------------
%%  % ramlocal storage
%%  ram_copies=ecomet_backend:fragment_info(ecomet_sys_object_ramlocal,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_object_ramlocal,type),
%%  true=ecomet_backend:fragment_info(ecomet_sys_object_ramlocal,local_content),
%%  % ram storage
%%  ram_copies=ecomet_backend:fragment_info(ecomet_sys_object_ram,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_object_ram,type),
%%  false=ecomet_backend:fragment_info(ecomet_sys_object_ram,local_content),
%%  % ramdisc storage
%%  disc_copies=ecomet_backend:fragment_info(ecomet_sys_object_ramdisc,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_object_ramdisc,type),
%%  false=ecomet_backend:fragment_info(ecomet_sys_object_ramdisc,local_content),
%%  % discbuf storage
%%  disc_copies=ecomet_backend:fragment_info(ecomet_sys_object_discbuf,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_object_discbuf,type),
%%  false=ecomet_backend:fragment_info(ecomet_sys_object_discbuf,local_content),
%%  % disc storage
%%  disc_only_copies=ecomet_backend:fragment_info(ecomet_sys_object_disc,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_object_disc,type),
%%  false=ecomet_backend:fragment_info(ecomet_sys_object_disc,local_content),
%%
%%  %%----------Index storage--------------------------------------------
%%  % ramlocal storage
%%  ram_copies=ecomet_backend:fragment_info(ecomet_sys_index_ramlocal,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_index_ramlocal,type),
%%  true=ecomet_backend:fragment_info(ecomet_sys_index_ramlocal,local_content),
%%  % ram storage
%%  ram_copies=ecomet_backend:fragment_info(ecomet_sys_index_ram,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_index_ram,type),
%%  false=ecomet_backend:fragment_info(ecomet_sys_index_ram,local_content),
%%  % ramdisc storage
%%  disc_copies=ecomet_backend:fragment_info(ecomet_sys_index_ramdisc,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_index_ramdisc,type),
%%  false=ecomet_backend:fragment_info(ecomet_sys_index_ramdisc,local_content),
%%  % discbuf storage
%%  disc_copies=ecomet_backend:fragment_info(ecomet_sys_index_discbuf,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_index_discbuf,type),
%%  false=ecomet_backend:fragment_info(ecomet_sys_index_discbuf,local_content),
%%  % disc storage
%%  disc_only_copies=ecomet_backend:fragment_info(ecomet_sys_index_disc,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_index_disc,type),
%%  false=ecomet_backend:fragment_info(ecomet_sys_index_disc,local_content),
%%
%%  %%------------Node local params------------------------------------------
%%  disc_copies=ecomet_backend:fragment_info(ecomet_sys_node,storage_type),
%%  set=ecomet_backend:fragment_info(ecomet_sys_node,type),
%%  true=ecomet_backend:fragment_info(ecomet_sys_node,local_content),

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
