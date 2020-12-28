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
-module(ecomet_node).

-include("ecomet.hrl").

-behaviour(ecomet_object).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  get_unique_id/0,
  get_configured_nodes/0,
  get_attached_nodes/0,
  get_ready_nodes/0,
  attach_node/1,
  detach_node/1,
  set_ready/2
]).

%%===========================================================================
%% Ecomet object behaviour
%%===========================================================================
-export([
  on_create/1,
  on_edit/1,
  on_delete/1
]).

%%===========================================================================
%% This functions we need to export ONLY for testing them
%%===========================================================================
-ifdef(TEST).
-export([
  check_id/1,
  check_name/1,
  register_node/1,
  unregister_node/1
]).

-endif.



%%=================================================================
%%	SERVICE API
%%=================================================================
% Unique id of the node
get_unique_id()->
  get_unique_id(node()).
get_unique_id(Node)->
  ecomet_schema:get_node_id(Node).

get_configured_nodes()->
  {_Header,Result}=ecomet_query:system([?ROOT],[{fun([N])->binary_to_atom(N,utf8) end,[<<".name">>]}],{'AND',[
    {<<".pattern">>,'=',?OID(<<"/root/.patterns/.node">>)},
    {<<".folder">>,'=',?OID(<<"/root/.nodes">>)}
  ]}),
  [N||[N]<-Result].

get_attached_nodes()->
  dlss:get_nodes().

get_ready_nodes()->
  dlss:get_ready_nodes().

attach_node(Node) when is_atom(Node)->
  dlss:add_node(Node);
attach_node(Node) when is_binary(Node)->
  attach_node(binary_to_atom(Node,utf8));
attach_node(Node)->
  {ok,Name}=ecomet:read_field(?OBJECT(Node),<<".name">>),
  attach_node(Name).

detach_node(Node) when is_atom(Node)->
  dlss:remove_node(Node);
detach_node(Node) when is_binary(Node)->
  detach_node(binary_to_atom(Node,utf8));
detach_node(Node)->
  {ok,Name}=ecomet:read_field(?OBJECT(Node),<<".name">>),
  detach_node(Name).

set_ready(Node,Value) when is_atom(Node)->
  set_ready(<<"/root/.nodes/",(atom_to_binary(Node,utf8))/binary>>,Value);
set_ready(<<"/root/.nodes",_>>=Path,Value)->
  set_ready(?OID(Path),Value);
set_ready(Node,Value) when is_binary(Node)->
  set_ready(<<"/root/.nodes/",Node/binary>>,Value);
set_ready(Node,Value)->
  ecomet:edit_object(ecomet:open(?OID(Node),none),#{<<"is_ready">>=>Value}).

%%=================================================================
%%	Ecomet object behaviour
%%=================================================================
on_create(Object)->
  check_name(Object),
  register_node(Object),
  ok.

on_edit(Object)->
  check_name(Object),
  check_id(Object),
  ok.

on_delete(Object)->
  case ecomet:read_field(Object,<<"is_ready">>) of
    {ok,true}->?ERROR(is_active);
    _->
      unregister_node(Object)
  end,
  ok.

check_name(Object)->
  case ecomet:field_changes(Object,<<".name">>) of
    none->ok;
    {NewName,none}->
      case re:run(NewName,"^([a-z])([a-z0-9])*@(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)+([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])$") of
        {match,_}->ok;
        _->?ERROR(invalid_node_name)
      end;
    _->
      ?ERROR(renaming_is_not_allowed)
  end.

check_id(Object)->
  case ecomet:field_changes(Object,<<"id">>) of
    none->ok;
    { _New, none }->
      % This is the creation
      ok;
    {_New, _Old}->
      ?ERROR(change_id_is_not_allowed)
  end.

register_node(Object)->
  {ok,Name}=ecomet:read_field(Object,<<".name">>),
  {ok,ID}=ecomet_schema:add_node(binary_to_atom(Name,utf8)),
  ok = ecomet:edit_object(Object,#{<<"id">>=>ID}).

unregister_node(Object)->
  {ok,Name}=ecomet:read_field(Object,<<".name">>),
  ecomet_schema:remove_node(binary_to_atom(Name,utf8)).