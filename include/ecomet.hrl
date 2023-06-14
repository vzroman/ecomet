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
-define(RAM,ram).
-define(RAMDISC,ramdisc).
-define(DISC,disc).
-define(STORAGE_TYPES,[?DISC,?RAMDISC,?RAM]).

%-------CONSTANTS------------------
-define(BITSTRING_LENGTH,65536).
-define(MAX_NODES,65535).
-define(ROUTER_POOL_SIZE, 128).
-define(PROCESS_MEMORY_LIMIT, 100). %MB

-define(LOCKS,'$ecomet_locks$').
-define(INDEX_LOCKS,'$ecomet_index_locks$').

-define(ESUBSCRIPTIONS,'$ecomet_esubscriptions$').

-define(LOCKS,'$ecomet_locks$').
-define(INDEX_LOCKS,'$ecomet_index_locks$').

-define(ESUBSCRIPTIONS,'$ecomet_esubscriptions$').

-define(ERROR(Error),erlang:error(Error)).

-define(PIPE(Items,Acc),ecomet_lib:pipe(Items,Acc)).

-define(OBJECT(ID),ecomet_lib:to_object_system(ID)).
-define(OID(ID),ecomet_lib:to_oid(ID)).
-define(PATH(ID),ecomet_lib:to_path(ID)).

-define(ENV(Key,Default),application:get_env(ecomet,Key,Default)).
-define(ENV(OS,Config,Default),
  (fun()->
    case os:getenv(OS) of
      false->?ENV(Config,Default);
      Value->Value
    end
  end)()
).

-define(SUBSCRIPTION(ID,Action,OID,Fields),{ecomet, ID, Action, OID, Fields }).

-define(B2A(Value),if is_atom(Value)->Value; true->list_to_atom(unicode:characters_to_list(Value)) end).
-define(A2B(Atom),unicode:characters_to_binary(atom_to_list(Atom))).
-define(T2B(Term),if is_binary(Term)->Term; true->unicode:characters_to_binary(io_lib:format("~p",[Term])) end).

-ifndef(TEST).

-define(LOGERROR(Text),lager:error(Text)).
-define(LOGERROR(Text,Params),lager:error(Text,Params)).
-define(LOGWARNING(Text),lager:warning(Text)).
-define(LOGWARNING(Text,Params),lager:warning(Text,Params)).
-define(LOGINFO(Text),lager:info(Text)).
-define(LOGINFO(Text,Params),lager:info(Text,Params)).
-define(LOGDEBUG(Text),lager:debug(Text)).
-define(LOGDEBUG(Text,Params),lager:debug(Text,Params)).

-else.

-define(LOGERROR(Text),ct:pal("error: "++Text)).
-define(LOGERROR(Text,Params),ct:pal("error: "++Text,Params)).
-define(LOGWARNING(Text),ct:pal("warning: "++Text)).
-define(LOGWARNING(Text,Params),ct:pal("warning: "++Text,Params)).
-define(LOGINFO(Text),ct:pal("info: "++Text)).
-define(LOGINFO(Text,Params),ct:pal("info: "++Text,Params)).
-define(LOGDEBUG(Text),ct:pal("debug: "++Text)).
-define(LOGDEBUG(Text,Params),ct:pal("debug: "++Text,Params)).

-endif.


-endif.
