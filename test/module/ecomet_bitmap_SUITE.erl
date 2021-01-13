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

-module(ecomet_bitmap_SUITE).

-define(WORD_LENGTH,64).

-define(EMPTY,0).
-define(SPARSE,1).
-define(SPARSE_FULL,2).
-define(FULL,3).
-define(FULL_WORD,2#1111111111111111111111111111111111111111111111111111111111111111).
-define(BIT(X), 1 bsl X).

-define(W(Bytes),lists:foldl(fun({N,B},Acc)-> end,0,Bytes)).

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
  bit_test/1
]).


all()->
  [
    bit_test
  ].

groups()->
  [].

%% Init system storages
init_per_suite(Config)->
  Config.
end_per_suite(_Config)->
  ok.

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

%--------------------------------------------------------------
% Set bit
%--------------------------------------------------------------
bit_test(_Config) ->

  B1 = <<?SPARSE:2,2#1:?WORD_LENGTH,2#1:?WORD_LENGTH>> = ecomet_bitmap:set_bit(<<>>,0),
  true = ecomet_bitmap:get_bit(B1,0),
  false = ecomet_bitmap:get_bit(B1,1),
  false = ecomet_bitmap:get_bit(B1,128),
  1 = ecomet_bitmap:count(B1),
  {1,[0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B1,{none,none}),

  B2 = <<?SPARSE:2,2#1:?WORD_LENGTH,2#101:?WORD_LENGTH>> = ecomet_bitmap:set_bit(B1,2),
  true = ecomet_bitmap:get_bit(B2,0),
  false = ecomet_bitmap:get_bit(B2,1),
  true = ecomet_bitmap:get_bit(B2,2),
  2 = ecomet_bitmap:count(B2),
  {2,[2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B2,{none,none}),

  B3 = <<?SPARSE:2,2#1:?WORD_LENGTH,2#10:(?WORD_LENGTH-16),2#101:16>> = ecomet_bitmap:set_bit(B2,17),
  true = ecomet_bitmap:get_bit(B3,17),
  3 = ecomet_bitmap:count(B3),
  {3,[17,2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B3,{none,none}),
  {3,[0,2,17]} = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],B3,{none,none}),

  % The last bit in the word
  false = ecomet_bitmap:get_bit(B3,63),
  B4 = <<?SPARSE:2,2#1:?WORD_LENGTH,2#1:1,2#10:(?WORD_LENGTH-16-1),2#101:16>> = ecomet_bitmap:set_bit(B3,63),
  true = ecomet_bitmap:get_bit(B4,63),
  4 = ecomet_bitmap:count(B4),
  {4,[63,17,2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B4,{none,none}),
  {4,[0,2,17,63]} = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],B4,{none,none}),

  % The word after next word
  false = ecomet_bitmap:get_bit(B4,128),
  B5 =
    <<
      ?SPARSE:2,
      2#101:?WORD_LENGTH,               % Header
      % 0 word
      2#1:1,                            % 63 bit
      2#10:(?WORD_LENGTH-16-1),         % 17 bit
      2#101:16,                         % 0,2 bit
      % 2 word
      2#1:?WORD_LENGTH                 % 128 bit
    >> = ecomet_bitmap:set_bit(B4,128),

  true = ecomet_bitmap:get_bit(B5,128),
  5 = ecomet_bitmap:count(B5),
  {5,[128,63,17,2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B5,{none,none}),
  {5,[0,2,17,63,128]} = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],B5,{none,none}),

  % The bit in the last word (max bit in the bucket WORD_LENGTH * WORD_LENGTH = 4096)
  false = ecomet_bitmap:get_bit(B5,4093),
  B6 =
    <<
      ?SPARSE:2,
      % Header
      2#1:1,
      2#101:(?WORD_LENGTH-1),
      % 0 word
      2#1:1,                            % 63 bit
      2#10:(?WORD_LENGTH-16-1),         % 17 bit
      2#101:16,                         % 0,2 bit
      % 2 word
      2#1:?WORD_LENGTH,                 % 128 bit
      % 7 word
      2#001:3,                          % The 3 last bits
      2#0:(?WORD_LENGTH-3)
    >> = ecomet_bitmap:set_bit(B5,4093),

  true = ecomet_bitmap:get_bit(B6,4093),
  6 = ecomet_bitmap:count(B6),
  {6,[4093,128,63,17,2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B6,{none,none}),
  {6,[0,2,17,63,128,4093]} = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],B6,{none,none}),

  % The last bit in the last word
  false = ecomet_bitmap:get_bit(B6,4095),
  Bucket0 =
    <<
      ?SPARSE:2,
      % Header
      2#1:1,
      2#101:(?WORD_LENGTH-1),
      % 0 word
      2#1:1,                            % 63 bit
      2#10:(?WORD_LENGTH-16-1),         % 17 bit
      2#101:16,                         % 0,2 bit
      % 2 word
      2#1:?WORD_LENGTH,                 % 128 bit
      % 7 word
      2#101:3,                          % The 3 last bits
      2#0:(?WORD_LENGTH-3)
    >> = ecomet_bitmap:set_bit(B6,4095),

  true = ecomet_bitmap:get_bit(Bucket0,4095),
  7 = ecomet_bitmap:count(Bucket0),
  {7,[4095,4093,128,63,17,2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],Bucket0,{none,none}),
  {7,[0,2,17,63,128,4093,4095]} = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0,{none,none}),
  %-----------------2 bucket---------------------------------


  ok.


