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
-module(ecomet_subscription).

%%=================================================================
%%	API
%%=================================================================
-export([
  notify/1
]).

%%=================================================================
%%	API
%%=================================================================
% Notify clients on change
notify(Log)->
%%	Query={'ANDNOT',
%%		{'AND',[
%%			{<<".pattern">>,get_pattern_id(),[<<"ramlocal">>]},
%%			{<<"domains">>,ecomet_object:get_domain(Log#ecomet_log.oid),[<<"ramlocal">>]},
%%			{'OR',lists:append([
%%				[{<<"reper_tags">>,AddTag,[<<"ramlocal">>]}||AddTag<-Log#ecomet_log.addtags],
%%				[{<<"reper_tags">>,DelTag,[<<"ramlocal">>]}||DelTag<-Log#ecomet_log.deltags],
%%				[{<<"reper_tags">>,Tag,[<<"ramlocal">>]}||Tag<-Log#ecomet_log.tags]
%%			])},
%%			{'OR',lists:append([
%%				%[{<<"rights">>,AddRight,[<<"ramlocal">>]}||AddRight<-Log#ecomet_log.addrights],
%%				%[{<<"rights">>,DelRight,[<<"ramlocal">>]}||DelRight<-Log#ecomet_log.delrights],
%%				%[{<<"rights">>,Right,[<<"ramlocal">>]}||Right<-Log#ecomet_log.rights],
%%				[{<<"is_admin">>,true,[<<"ramlocal">>]}]
%%			])},
%%			{'OR',lists:append([
%%				lists:append([
%%					[{<<"query_tags">>,AddTag,[<<"ramlocal">>]}||AddTag<-Log#ecomet_log.addtags],
%%					[{<<"query_tags">>,DelTag,[<<"ramlocal">>]}||DelTag<-Log#ecomet_log.deltags]
%%				]),
%%				[{<<"fields">>,FieldName,[<<"ramlocal">>]}||FieldName<-element(1,Log#ecomet_log.fields)],
%%				[{<<"fields">>,<<".all">>,[<<"ramlocal">>]},{<<"fields">>,<<".none">>,[<<"ramlocal">>]}]
%%			])}
%%		]},
%%		{<<".deleted">>,true,[<<"ramlocal">>]}
%%	},
%%	{ok,ClusterID}=ecomet_node:get_cluster(ecomet_node:get_local_oid()),
%%	% Send notification to other nodes
%%	spawn(fun()->
%%		notify_neighbors(ClusterID,Log,Query),
%%		notify_clusters(ClusterID,Log,Query)
%%	end).
  dummy.