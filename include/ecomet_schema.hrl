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

-ifndef(ECOMET_SCHEMA).
-define(ECOMET_SCHEMA,1).

-define(SCHEMA_DIR,zaya:schema_dir()++"/ECOMET").

%%==============================================================
%%	Patterns
%%==============================================================
-define(OBJECT_PATTERN,1).
-define(FOLDER_PATTERN,2).
-define(PATTERN_PATTERN,3).
-define(FIELD_PATTERN,4).
%%==============================================================
%%	Folder objects
%%==============================================================
-define(ROOT_FOLDER,1).
-define(PATTERNS_FOLDER,2).

-endif.
