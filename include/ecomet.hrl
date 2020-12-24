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

-ifndef(ECOMET_STRUCT).
-define(ECOMET_STRUCT,1).

-define(ROOT,root).

%--------Database storage-----------------------
-define(DATA,data).
-define(INDEX,index).

%-------Supported storage types------------------
-define(RAMLOCAL,ramlocal).
-define(RAM,ram).
-define(RAMDISC,ramdisc).
-define(DISC,disc).


-define(BITSTRING_LENGTH,65536).
-define(MAX_NODES,65535).

-record(kv,{key,value}).
-record(ecomet_log,{
  object,
  db,
  ts,
  tags,
  rights,
  changes
}).

-define(ERROR(Error),erlang:error(Error)).

-define(PIPE(Items,Acc),ecomet_lib:pipe(Items,Acc)).

-define(OBJECT(ID),ecomet_lib:to_object_system(ID)).
-define(OID(ID),ecomet_lib:to_oid(ID)).
-define(PATH(ID),ecomet_lib:to_path(ID)).

-define(ENV(Key,Default),application:get_env(dlss,Key,Default)).
-define(ENV(OS,Config,Default),
  (fun()->
    case os:getenv(OS) of
      false->?ENV(Config,Default);
      Value->Value
    end
  end)()
).

-define(SUBSCRIPTION(ID,Action,OID,Fields),{ecomet, ID, Action, OID, Fields }).
-define(SUBSCRIPTION(ID,Action,OID),{ecomet, ID, Action, OID }).

-define(A2B(Atom),unicode:characters_to_binary(atom_to_list(Atom))).

-define(LOGERROR(Text),lager:error(Text)).
-define(LOGERROR(Text,Params),lager:error(Text,Params)).
-define(LOGWARNING(Text),lager:warning(Text)).
-define(LOGWARNING(Text,Params),lager:warning(Text,Params)).
-define(LOGINFO(Text),lager:info(Text)).
-define(LOGINFO(Text,Params),lager:info(Text,Params)).
-define(LOGDEBUG(Text),lager:debug(Text)).
-define(LOGDEBUG(Text,Params),lager:debug(Text,Params)).

-endif.
