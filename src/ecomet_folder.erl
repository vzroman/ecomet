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
-module(ecomet_folder).

-include("ecomet_schema.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  oid2path/1,
  path2oid/1
]).

%%=================================================================
%%	API
%%=================================================================
oid2path({?FOLDER_PATTERN,?ROOT_FOLDER})->
  <<"/root">>.
%%oid2path(OID)->
%%  Object = ecomet_object:construct(OID),
%%  { ok, Name } = ecomet:read_field( Object, <<".name">> ),
%%  { ok, FolderID } = ecomet:read_field( Object, <<".folder">> ),
%%  Path = oid2path( FolderID ),
%%  <<Path/binary,"/",Name/binary>>.

path2oid(<<"/root">>)->
  {?FOLDER_PATTERN,?ROOT_FOLDER}.
%%path2oid(Path)->
%%  % TODO
%%  ok.