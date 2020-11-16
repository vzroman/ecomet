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

-behaviour(gen_server).

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

-define(DEFAULT_SCHEMA_CYCLE, 1000).
-define(WAIT_SCHEMA_TIMEOUT,5000).

-define(INCREMENT,list_to_atom("ecomet_"++atom_to_list(node()))).
-define(SCHEMA,ecomet_schema).

-record(state,{
  cycle
}).

%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

init([])->

  ?LOGINFO("starting ecomet schema server ~p",[self()]),

  ok = init_increment_table(),

  ok = init_schema(),

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

init_schema()->
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
      ?LOGINFO("schema does not exist yet, creating..."),
      create_schema(),
      ?LOGINFO("initialize ecomet environment"),
      init_environment()
  end.

create_schema()->
  ok.

init_environment()->
  % TODO
  ok.

table_exists(Table)->
  Known = mnesia:system_info(tables),
  lists:member(Table,Known).

table_has_copy(Table,Type)->
  Copies=mnesia:table_info(?SCHEMA,Type),
  lists:member(Table,Copies).
