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

-ifndef(ECOMET_SUBSCRIPTION).
-define(ECOMET_SUBSCRIPTION,1).

-define(EMPTY_SET, {0,nil} ).
-define(NEW_SET(Es), gb_sets:from_list( Es )).
-define(SET_ADD(E,S), gb_sets:add_element(E, S)).
-define(SET_DEL(E,S), gb_sets:del_element(E, S)).
-define(SET_AND(S1,S2), gb_sets:intersection( S1, S2 )).
-define(SET_OR(S1,S2), gb_sets:union( S1, S2 )).
-define(SET_ANDNOT(S1,S2), gb_sets:subtract( S1, S2 )).
-define(SET_IS_SUBSET(S1,S2), gb_sets:is_subset( S1, S2 )).
-define(SET_IS_DISJOINT(S1,S2), gb_sets:is_disjoint( S1, S2 )).
-define(SET_FOLD(F,Acc,S), gb_sets:fold( F, Acc, S )).

-record(subscription,{ id, owner, dbs, read, deps, conditions, params }).

-endif.
