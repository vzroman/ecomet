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

-ifndef(DLSS_TEST).
-define(DLSS_TEST,1).

-include_lib("common_test/include/ct.hrl").

-define(assertError(Term, Expr),
  begin
    ((fun () ->
      try (Expr) of
        __V -> erlang:error({assertException,
          [{module, ?MODULE},
            {line, ?LINE},
            {expression, (??Expr)},
            {unexpected_success, __V}]})
      catch
        error:Term -> ok;
        __C:__T:__Stack ->
          erlang:error({assertException,
            [{module, ?MODULE},
              {line, ?LINE},
              {expression, (??Expr)},
              {unexpected_exception,
                {__C, __T, __Stack}}]})
      end
      end)())
  end).

-define(GET(Key,Config),proplists:get_value(Key,Config)).
-define(GET(Key,Config,Default),proplists:get_value(Key,Config,Default)).

-endif.
