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

-define(F,1).
-define(W,0).

-define(X,0).
-define(XX,1).

-define(FULL,2#1111111111111111111111111111111111111111111111111111111111111111).

-define(BIT(X), 1 bsl X).

-define(SHORT,5).
-define(LONG,29).

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
  bit_sparse/1,
  bit_sparse_full/1
]).


all()->
  [
    bit_sparse,
    bit_sparse_full
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
bit_sparse(_Config) ->

  B1 = <<?W:1,?X:1,2#1:?WORD_LENGTH,2#1:?WORD_LENGTH>> = ecomet_bitmap:set_bit(<<>>,0),
  true = ecomet_bitmap:get_bit(B1,0),
  false = ecomet_bitmap:get_bit(B1,1),
  false = ecomet_bitmap:get_bit(B1,128),
  1 = ecomet_bitmap:count(B1),
  {1,[0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B1,{none,none}),

  B2 = <<?W:1,?X:1,2#1:?WORD_LENGTH,2#101:?WORD_LENGTH>> = ecomet_bitmap:set_bit(B1,2),
  true = ecomet_bitmap:get_bit(B2,0),
  false = ecomet_bitmap:get_bit(B2,1),
  true = ecomet_bitmap:get_bit(B2,2),
  2 = ecomet_bitmap:count(B2),
  {2,[2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B2,{none,none}),

  B3 = <<?W:1,?X:1,2#1:?WORD_LENGTH,2#10:(?WORD_LENGTH-16),2#101:16>> = ecomet_bitmap:set_bit(B2,17),
  true = ecomet_bitmap:get_bit(B3,17),
  3 = ecomet_bitmap:count(B3),
  {3,[17,2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B3,{none,none}),
  {3,[0,2,17]} = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],B3,{none,none}),

  % The last bit in the word
  false = ecomet_bitmap:get_bit(B3,63),
  B4 = <<?W:1,?X:1,2#1:?WORD_LENGTH,2#1:1,2#10:(?WORD_LENGTH-16-1),2#101:16>> = ecomet_bitmap:set_bit(B3,63),
  true = ecomet_bitmap:get_bit(B4,63),
  4 = ecomet_bitmap:count(B4),
  {4,[63,17,2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B4,{none,none}),
  {4,[0,2,17,63]} = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],B4,{none,none}),

  % The word after next word
  false = ecomet_bitmap:get_bit(B4,128),
  B5 =
    <<
      ?W:1,?X:1,
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
      ?W:1,?X:1,
      % Header
      2#1:1,
      2#101:(?WORD_LENGTH-1),
      % 0 word
      2#1:1,                            % 63 bit
      2#10:(?WORD_LENGTH-16-1),         % 17 bit
      2#101:16,                         % 0,2 bit
      % 2 word
      2#1:?WORD_LENGTH,                 % 128 bit
      % 63 word
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
      ?W:1,?X:1,
      % Header
      2#1:1,
      2#101:(?WORD_LENGTH-1),
      % 0 word
      2#1:1,                            % 63 bit
      2#10:(?WORD_LENGTH-16-1),         % 17 bit
      2#101:16,                         % 0,2 bit
      % 2 word
      2#1:?WORD_LENGTH,                 % 128 bit
      % 63 word
      2#101:3,                          % The 3 last bits
      2#0:(?WORD_LENGTH-3)
    >> = ecomet_bitmap:set_bit(B6,4095),

  true = ecomet_bitmap:get_bit(Bucket0,4095),
  7 = ecomet_bitmap:count(Bucket0),
  {7,Bucket0BitsR = [4095,4093,128,63,17,2,0]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],Bucket0,{none,none}),
  {7,[0,2,17,63,128,4093,4095]} = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0,{none,none}),
  Bucket0Size = erlang:bit_size(Bucket0),

  ct:pal("Bucket0 ~p",[Bucket0]),
  %-----------------2 bucket---------------------------------
  false = ecomet_bitmap:get_bit(Bucket0,8192),
  B8 =
    <<
      %-------Bucket 0------------
      Bucket0:Bucket0Size/bitstring,
      %------Bucket 1 (empty)-----
      ?F:1,0:1,?X:1,1:?SHORT,
      %------Bucket 2-------------
      % Type
      ?W:1,?X:1,
      % Header
      2#1:?WORD_LENGTH,
      % Word 0
      2#1:?WORD_LENGTH
    >> = ecomet_bitmap:set_bit(Bucket0,8192),

  true = ecomet_bitmap:get_bit(B8,8192),
  8 = ecomet_bitmap:count(B8),
  ct:pal("B8 ~p",[B8]),
  {8,[8192|Bucket0BitsR]} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],B8,{none,none}),

  <<
    %-------Bucket 0------------
    Bucket0:Bucket0Size/bitstring,
    %------Bucket 1 (empty)-----
    ?F:1,0:1,?X:1,1:?SHORT,
    %------Bucket 2-------------
    Bucket2/bitstring
  >> = B8,
  Bucket2Size = erlang:bit_size(Bucket2),

  %-------------------------------------------------------------
  % Reset
  %-------------------------------------------------------------
  % reset bit 0
  R0 =
    <<
      %--------Bucket 0-----------------------
      ?W:1,?X:1,
      % Header
      2#1:1,
      2#101:(?WORD_LENGTH-1),
      % 0 word
      2#1:1,                            % 63 bit
      2#10:(?WORD_LENGTH-16-1),         % 17 bit
      2#100:16,                         % 2 bit
      % 2 word
      2#1:?WORD_LENGTH,                 % 128 bit
      % 63 word
      2#101:3,                          % The 3 last bits
      2#0:(?WORD_LENGTH-3),
      %-------Bucket 1 (empty)----------------
      ?F:1,0:1,?X:1,1:?SHORT,
      %-------Bucket 2------------------------
      Bucket2:Bucket2Size/bitstring
    >> = ecomet_bitmap:reset_bit(B8,0),

  false = ecomet_bitmap:get_bit(R0,0),
  7 = ecomet_bitmap:count(R0),

  % reset bit 17
  R1 =
    <<
      %--------Bucket 0-----------------------
      ?W:1,?X:1,
      % Header
      2#1:1,
      2#101:(?WORD_LENGTH-1),
      % 0 word
      2#1:1,                            % 63 bit
      2#100:(?WORD_LENGTH-1),           % 2 bit
      % 2 word
      2#1:?WORD_LENGTH,                 % 128 bit
      % 63 word
      2#101:3,                          % The 3 last bits
      2#0:(?WORD_LENGTH-3),
      %-------Bucket 1 (empty)----------------
      ?F:1,0:1,?X:1,1:?SHORT,
      %-------Bucket 2------------------------
      Bucket2:Bucket2Size/bitstring
    >> = ecomet_bitmap:reset_bit(R0,17),

  false = ecomet_bitmap:get_bit(R1,17),
  6 = ecomet_bitmap:count(R1),

  ok.

bit_sparse_full(_Config) ->

  Word0Bits = [0,2,17,63],
  Word2Bits = [128],
  Word63Bits= [4093,4095],
  Bucket0Bits = Word0Bits++Word2Bits++Word63Bits,

  Bucket0_0 = <<
    ?W:1,?X:1,
    % Header
    2#1:1,                           % 63 word
    2#101:(?WORD_LENGTH-1),          % 0,2 word
    % 0 word
    _Word0:?WORD_LENGTH,              % 0,2,17,63 bit
    % 2 word
    _Word2:?WORD_LENGTH,              % 128 bit
    % 63 word
    _Word63:?WORD_LENGTH              % 4093, 4095 bit
  >> = lists:foldl(fun(Bit,Acc)->ecomet_bitmap:set_bit(Acc,Bit) end,<<>>,Bucket0Bits),

  {7,Bucket0Bits} = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_0,{none,none}),
  Bucket0BitsR = lists:reverse(Bucket0Bits),
  {7,Bucket0BitsR} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],Bucket0_0,{none,none}),

  % Fill the word 1
  Word1Bits = lists:seq(?WORD_LENGTH,2*?WORD_LENGTH-1),
  % From scratch
  Bucket0_1 = <<
    ?W:1,?XX:1,
    % Header
    2#10:?WORD_LENGTH,
    % Full
    2#10:?WORD_LENGTH
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:set_bit(Acc,Bit) end,<<>>,Word1Bits),
  {64, Word1Bits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_1,{none,none}),
  Word1BitsR = lists:reverse(Word1Bits),
  {64, Word1BitsR } = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],Bucket0_1,{none,none}),

  % Add to Bucket0_0
  Bucket0_2 = <<
    ?W:1,?XX:1,
    % Header
    2#1:1,                           % 63 word
    2#111:(?WORD_LENGTH-1),          % 0,1,2 word
    % Full
    2#10:?WORD_LENGTH,               % 1 word
    % 0 word
    _Word0:?WORD_LENGTH,              % 0,2,17,63 bit
    % 2 word
    _Word2:?WORD_LENGTH,              % 128 bit
    % 63 word
    _Word63:?WORD_LENGTH              % 4093, 4095 bit
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:set_bit(Acc,Bit) end,Bucket0_0,Word1Bits),
  Bucket0_2Bits = Word0Bits++Word1Bits++Word2Bits++Word63Bits,

  % 7 + 64
  {71, Bucket0_2Bits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_2,{none,none}),

  % Fill word 5
  Word5Bits = lists:seq(5*?WORD_LENGTH,6*?WORD_LENGTH-1),
  Bucket0_3 = <<
    ?W:1,?XX:1,
    % Header
    2#1:1,                           % 63 word
    2#100111:(?WORD_LENGTH-1),       % 0,1,2,5 word
    % Full
    2#100010:?WORD_LENGTH,           % 1,5 word
    % 0 word
    _Word0:?WORD_LENGTH,              % 0,2,17,63 bit
    % 2 word
    _Word2:?WORD_LENGTH,              % 128 bit
    % 63 word
    _Word63:?WORD_LENGTH              % 4093, 4095 bit
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:set_bit(Acc,Bit) end,Bucket0_2,Word5Bits),
  Bucket0_3Bits = Word0Bits++Word1Bits++Word2Bits++Word5Bits++Word63Bits,
  {_, Bucket0_3Bits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_3,{none,none}),

  % Fill word 0
  Word0BitsF = lists:seq(0,?WORD_LENGTH-1),
  Bucket0_4 = <<
    ?W:1,?XX:1,
    % Header
    2#1:1,                           % 63 word
    2#100111:(?WORD_LENGTH-1),       % 0,1,2,5 word
    % Full
    2#100011:?WORD_LENGTH,           % 1,5 word
    % 2 word
    _Word2:?WORD_LENGTH,              % 128 bit
    % 63 word
    _Word63:?WORD_LENGTH              % 4093, 4095 bit
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:set_bit(Acc,Bit) end,Bucket0_3,Word0BitsF),
  Bucket0_4Bits = Word0BitsF++Word1Bits++Word2Bits++Word5Bits++Word63Bits,
  {_, Bucket0_4Bits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_4,{none,none}),

  % Fill word 63
  Word63BitsF = lists:seq(63*?WORD_LENGTH,64*?WORD_LENGTH-1),
  Bucket0_5 = <<
    ?W:1,?XX:1,
    % Header
    2#1:1,                           % 63 word
    2#100111:(?WORD_LENGTH-1),       % 0,1,2,5 word
    % Full
    2#1:1,                           % 63 word
    2#100011:(?WORD_LENGTH-1),           % 1,5 word
    % 2 word
    _Word2:?WORD_LENGTH              % 128 bit
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:set_bit(Acc,Bit) end,Bucket0_4,Word63BitsF),
  Bucket0_5Bits = Word0BitsF++Word1Bits++Word2Bits++Word5Bits++Word63BitsF,
  {_, Bucket0_5Bits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_5,{none,none}),

  % Fill bucket 1
  Bucket0Size = erlang:bit_size(Bucket0_5),
  Bucket1BitsF = lists:seq(?WORD_LENGTH*?WORD_LENGTH,2*?WORD_LENGTH*?WORD_LENGTH-1),
  Bucket0_Bucket1F = <<
    Bucket0_5:Bucket0Size/bitstring,
    ?F:1,1:1,?X:1,1:?SHORT
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:set_bit(Acc,Bit) end,Bucket0_5,Bucket1BitsF),
  Bucket0_Bucket1FBits = Bucket0_5Bits ++ Bucket1BitsF,
  {_, Bucket0_Bucket1FBits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_Bucket1F,{none,none}),
  true = ecomet_bitmap:get_bit(Bucket0_Bucket1F,4096),
  true = ecomet_bitmap:get_bit(Bucket0_Bucket1F,6333),
  true = ecomet_bitmap:get_bit(Bucket0_Bucket1F,8191),

  % Fill bucket 5
  Bucket5BitsF = lists:seq(5*?WORD_LENGTH*?WORD_LENGTH,6*?WORD_LENGTH*?WORD_LENGTH-1),
  Bucket0_Bucket1F_Bucket5F = <<
    Bucket0_5:Bucket0Size/bitstring,
    ?F:1,1:1,?X:1,1:?SHORT,    % Bucket 1
    ?F:1,0:1,?X:1,3:?SHORT,   % Bucket 2-4
    ?F:1,1:1,?X:1,1:?SHORT     % Bucket 5
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:set_bit(Acc,Bit) end,Bucket0_Bucket1F,Bucket5BitsF),
  Bucket0_Bucket1F_Bucket5Fbits = Bucket0_5Bits ++ Bucket1BitsF++Bucket5BitsF,
  {_, Bucket0_Bucket1F_Bucket5Fbits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_Bucket1F_Bucket5F,{none,none}),

  ct:pal("Bucket0_Bucket1F_Bucket5F = ~p",[Bucket0_Bucket1F_Bucket5F]),
  % Reset bit 4097 (bucket 1)
  Bucket0_Bucket1_Bucket5F = <<
    Bucket0_5:Bucket0Size/bitstring,

    % Bucket 1
    ?W:1,?XX:1,
    % Header
    ?FULL:?WORD_LENGTH,
    % Full
    2#1111111111111111111111111111111111111111111111111111111111111110:?WORD_LENGTH,
    % Data
    2#1111111111111111111111111111111111111111111111111111111111111101:?WORD_LENGTH,           % 4097 (1 bit) is absent

    ?F:1,0:1,?X:1,3:?SHORT,   % Bucket 2-4
    ?F:1,1:1,?X:1,1:?SHORT     % Bucket 5
  >> = ecomet_bitmap:reset_bit(Bucket0_Bucket1F_Bucket5F,4097),

  Bucket0_Bucket1_Bucket5Fbits = Bucket0_5Bits ++ (Bucket1BitsF--[4097])++Bucket5BitsF,
  {_, Bucket0_Bucket1_Bucket5Fbits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_Bucket1_Bucket5F,{none,none}),

  % Reset word 3 in bucket 1
  Word67bits = lists:seq(67*?WORD_LENGTH,68*?WORD_LENGTH-1),
  Bucket0_Bucket1_0_Bucket5F = <<
    Bucket0_5:Bucket0Size/bitstring,

    % Bucket 1
    ?W:1,?XX:1,
    % Header
    2#1111111111111111111111111111111111111111111111111111111111110111:?WORD_LENGTH,          % 63 word (3 in bucket) is absent
    % Full
    2#1111111111111111111111111111111111111111111111111111111111110110:?WORD_LENGTH,
    % Data
    2#1111111111111111111111111111111111111111111111111111111111111101:?WORD_LENGTH,           % 4097 (1 bit) is absent

    ?F:1,0:1,?X:1,3:?SHORT,   % Bucket 2-4
    ?F:1,1:1,?X:1,1:?SHORT     % Bucket 5
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:reset_bit(Acc,Bit) end,Bucket0_Bucket1_Bucket5F,Word67bits),

  Bucket0_Bucket1_0_Bucket5Fbits = Bucket0_Bucket1_Bucket5Fbits -- Word67bits,
  {_, Bucket0_Bucket1_0_Bucket5Fbits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_Bucket1_0_Bucket5F,{none,none}),

  % Reset entire bucket 1
  Bucket0_Bucket1_1_Bucket5F = <<
    Bucket0_5:Bucket0Size/bitstring,
    ?F:1,0:1,?X:1,1:?SHORT,   % Bucket 1 (reset)
    ?F:1,0:1,?X:1,3:?SHORT,   % Bucket 2-4
    ?F:1,1:1,?X:1,1:?SHORT     % Bucket 5
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:reset_bit(Acc,Bit) end,Bucket0_Bucket1_0_Bucket5F,Bucket1BitsF),

  Bucket0_Bucket1_1_Bucket5Fbits = Bucket0_Bucket1_Bucket5Fbits -- Bucket1BitsF,
  {_, Bucket0_Bucket1_1_Bucket5Fbits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_Bucket1_1_Bucket5F,{none,none}),

  % Zip the result
  Bucket0_Bucket1_1_Bucket5F_zip = <<
    Bucket0_5:Bucket0Size/bitstring,
    ?F:1,0:1,?X:1,4:?SHORT,   % Bucket 1 (reset)
    ?F:1,1:1,?X:1,1:?SHORT     % Bucket 5
  >> = ecomet_bitmap:zip(Bucket0_Bucket1_1_Bucket5F),

  {_, Bucket0_Bucket1_1_Bucket5Fbits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_Bucket1_1_Bucket5F_zip,{none,none}),

  % Reset bucket 5
  Bucket0_Bucket1_1_Bucket5_1 = <<
    Bucket0_5:Bucket0Size/bitstring,
    ?F:1,0:1,?X:1,4:?SHORT,   % Bucket 1 (reset)
    ?F:1,0:1,?X:1,1:?SHORT     % Bucket 5
  >> = lists:foldl( fun(Bit,Acc)->ecomet_bitmap:reset_bit(Acc,Bit) end,Bucket0_Bucket1_1_Bucket5F_zip,Bucket5BitsF),

  Bucket0_Bucket1_1_Bucket5_1bits = Bucket0_Bucket1_1_Bucket5Fbits -- Bucket5BitsF,
  {_, Bucket0_Bucket1_1_Bucket5_1bits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_Bucket1_1_Bucket5_1,{none,none}),

  % Zip the result
  % Reset bucket 5
  Bucket0_Bucket1_1_Bucket5_1_zip = <<
    Bucket0_5:Bucket0Size/bitstring
  >> = ecomet_bitmap:zip(Bucket0_Bucket1_1_Bucket5_1),

  {_, Bucket0_Bucket1_1_Bucket5_1bits } = ecomet_bitmap:foldr(fun(N,Acc)->[N|Acc] end,[],Bucket0_Bucket1_1_Bucket5_1_zip,{none,none}),

  ok.



