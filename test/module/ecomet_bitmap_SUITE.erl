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
  bit_hardly_sparse/1,
  bit_sparse_full/1,
  compress_decompress_test/1,
  tail_test/1,
  first_test/1,
  split_test/1,
  data_and_test/1,
  data_or_test/1,
  bit_and_test/1,
  bit_or_test/1,
  bit_andnot_test/1
]).


all()->
  [
    bit_sparse,
    bit_hardly_sparse,
    bit_sparse_full,
    compress_decompress_test,
    tail_test,
    first_test,
    split_test,
    data_and_test,
    data_or_test,
    bit_and_test,
    bit_or_test,
    bit_andnot_test
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

bit_hardly_sparse(_Config) ->

  Bits = [50331686,33554482,33554470],

  Bucket = lists:foldl(fun(Bit,Acc)->ecomet_bitmap:set_bit(Acc,Bit) end,<<>>,Bits),

  {3,Bits} = ecomet_bitmap:foldl(fun(N,Acc)->[N|Acc] end,[],Bucket,{none,none}),

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

compress_decompress_test(_Config) ->
  ct:pal("Checking random generator ~n~p~n", [payload_generator(5, 10)]),
  BucketNumber = 1000,
  Generator =  bucket_generator(BucketNumber, false),
  [begin
     Bucket = <<?W:1, ?X:1, Hword:64, Payload/bitstring>>,
     Bucket = ecomet_bitmap:compress(ecomet_bitmap:decompress(Bucket))
   end
    || {Hword, Payload} <-Generator
  ],
  ct:pal("RANDOM very cool thing"),
  ComplexGenerator = bucket_generator(BucketNumber, true),
  [
    begin
      Bucket = <<?W:1, ?XX:1, Hword:64, FullWord:64, Payload/bitstring>>,
      Bucket = ecomet_bitmap:compress(ecomet_bitmap:decompress(Bucket))
    end
    || {Hword, FullWord, Payload} <- ComplexGenerator
  ],
  ct:pal("RANDOM very cool thing, Part 2"),
  ok.

tail_test(_Config) ->
  TestNumber = 100,
  ok = t(TestNumber),
  ok.

first_test(_Config) ->
  BucketNumber = 1,
  Buckets = generate_buckets(BucketNumber),
  [Head | Tail] = Buckets,
  MergedTail = merge_buckets(Tail),
  {Head, MergedTail} = ecomet_bitmap:first(merge_buckets(Buckets)),

  TestNumber = 100,
  ok = f(TestNumber),
  ct:pal("I am fck genius"),
  ok.

split_test(_Config) ->
  {<<1:1, 0:1, 0:1, 30:5>> ,<<>>} = ecomet_bitmap:split(<<>>, 30),
  {<<1:1, 0:1, 1:1, 33:29>> ,<<>>} = ecomet_bitmap:split(<<>>, 33),
  {<<1:1, 1:1, 0:1, 16:5>>, <<1:1, 1:1, 0:1, 14:5, 29>>} = ecomet_bitmap:split(<<1:1, 1:1, 0:1, 30:5, 29>>, 16),
  {<<1:1, 0:1, 0:1, 30:5>>, <<29>>} = ecomet_bitmap:split(<<1:1, 0:1, 0:1, 30:5, 29>>, 30),
  {<<1:1, 0:1, 0:1, 30:5, 1:1, 1:1, 0:1, 1:5>>, <<>>} = ecomet_bitmap:split(<<1:1, 0:1, 0:1, 30:5, 193>>, 31),

  ok.

data_and_test(_Config) ->
  [] = ecomet_bitmap:data_and([], [1, 2, 3]),
  [] = ecomet_bitmap:data_and([1, 2, 3], []),
  [1, 2] = ecomet_bitmap:data_and([5, 2, 3], [3, 6]),
  [1, 2] = ecomet_bitmap:data_and([3, 6], [5, 2, 3]),
  ok.

data_or_test(_Config) ->
  [1, 2, 3] = ecomet_bitmap:data_or([1, 2, 3], []),
  [1, 2, 3] = ecomet_bitmap:data_or([], [1, 2, 3]),
  [3, 2, 3] = ecomet_bitmap:data_or([1, 2, 3], [2, 0]),
  [3, 2, 3] = ecomet_bitmap:data_or([2, 0], [1, 2, 3]),
  ok.

bit_and_test(_Config) ->
  A = rand:uniform(1 bsl 4096 - 1),
  B = rand:uniform(1 bsl 4096 - 1),
  Bucket1 = data_to_bucket(<<A:4096>>),
  Bucket2= data_to_bucket(<<B:4096>>),
  Result = data_to_bucket(<<(A band B):4096>>),
  Result = ecomet_bitmap:bit_and(Bucket1, Bucket2),

  Bucket1 = ecomet_bitmap:bit_and(Bucket1, <<?F:1, 1:1, ?X:1, 1:?SHORT>>),
  Bucket2 = ecomet_bitmap:bit_and(<<?F:1, 1:1, ?X:1, 1:?SHORT>>, Bucket2),
  <<>> = ecomet_bitmap:zip(ecomet_bitmap:bit_and(Bucket2, <<?F:1, 0:1, ?X:1, 1:?SHORT>>)),
  <<>> = ecomet_bitmap:zip(ecomet_bitmap:bit_and(<<?F:1, 0:1, ?X:1, 1:?SHORT>>, Bucket1)),


  C = generate_list_of_random_data(7),
  D = generate_list_of_random_data(10),
  Bitmap1 = data_to_bitmap(C),
  Bitmap2 = data_to_bitmap(D),
  Result1 = data_to_bitmap(list_and(C, D)),
  Result1 = ecomet_bitmap:bit_and(Bitmap1, Bitmap2),

  TestNumber = 10,
  BitAnd = fun(Bmap1, Bmap2) -> ecomet_bitmap:bit_and(Bmap1, Bmap2) end,
  ListAnd = fun(L1, L2) -> list_and(L1, L2) end,
  ok = check_bitwise_oper(TestNumber, BitAnd, ListAnd),
  ok.


bit_or_test(_Config) ->
  TestNumber = 10,
  BitOr = fun(Bmap1, Bmap2) -> ecomet_bitmap:bit_or(Bmap1, Bmap2) end,
  ListOr = fun(L1, L2) -> list_or(L1, L2) end,
  ok = check_bitwise_oper(TestNumber, BitOr, ListOr),
  ok.


bit_andnot_test(_Config) ->
  TestNumber = 10,
  BitAndNot = fun(Bmap1, Bmap2) -> ecomet_bitmap:bit_andnot(Bmap1, Bmap2) end,
  ListAndNot = fun(L1, L2) -> list_andnot(L1, L2) end,
  ok = check_bitwise_oper(TestNumber, BitAndNot, ListAndNot),
  ok.

%%=================================================================
%% Helpers
%%=================================================================
bit_count(Value)->
  bit_count(Value,0).
bit_count(0,Acc)->
  Acc;
bit_count(Value,Acc)->
  Bit = Value rem 2,
  bit_count(Value bsr 1,Acc+Bit).

% Generate N blocks. Block is a sequence of bits.
% Each block size is WORD_LENGTH(64 bit). Each block <= UpperBound
% Return N blocks as bitstring
payload_generator(0, _UpperBound) ->
  <<>>;
payload_generator(N, UpperBound) ->
  Payload = rand:uniform(UpperBound),
  Tail = payload_generator(N - 1, UpperBound),
  <<Payload:?WORD_LENGTH, Tail/bitstring>>.

% Generate TestNumber Buckets
% Actually it is BitMap%
bucket_generator(0, _) ->
  [];
bucket_generator(TestNumber, false) ->
  Hword = rand:uniform(1 bsl 64 - 1),
  Payload = payload_generator(bit_count(Hword), 1 bsl 64 - 1),
  [{Hword, Payload} | bucket_generator(TestNumber - 1, false)];
bucket_generator(TestNumber, true) ->
  if TestNumber rem 2 =:= 0 ->
    Hword = 1 bsl 64 - 1,
    FullWord = rand:uniform(1 bsl 64 - 1),
    BitN = 64 - bit_count(FullWord);
    true ->
      Hword = rand:uniform(1 bsl 64 - 1) bor 1,
      FullWord = 1,
      BitN = bit_count(Hword) - 1
  end,
  Payload = payload_generator(BitN, 1 bsl 64 - 1),
  [{Hword, FullWord, Payload} | bucket_generator(TestNumber - 1, true)].

% %
t(0) ->
  ok;
t(TestNumber) ->
  NumberOfBuckets = 10,
  Bucket = generate_buckets(NumberOfBuckets),
  BitMap = merge_buckets(Bucket),
  FirstBucketsNumber = rand:uniform(NumberOfBuckets),
  Tail = merge_buckets(lists:sublist(Bucket, FirstBucketsNumber + 1, 10000000)),
  Tail = ecomet_bitmap:tail(BitMap, FirstBucketsNumber),
  t(TestNumber - 1).

%This function generates list.
% Each element in list is bucket %
generate_buckets(0) ->
  [];
generate_buckets(BucketNumbers) ->
  Xbit = rand:uniform(10) rem 2,
  Bucket = if Xbit =:= 0 ->
    HWord = rand:uniform(1 bsl 64 - 1),
    Payload = payload_generator(bit_count(HWord), 1 bsl 64 - 1),
    <<?W:1, ?X:1, HWord:64, Payload/bitstring>>;
  true ->
    HWord = 1 bsl 64 - 1,
    FullWord = rand:uniform(1 bsl 64 - 1),
    Payload = payload_generator(64 - bit_count(FullWord), 1 bsl 64 - 1),
    <<?W:1, ?XX:1, HWord:64, FullWord:64, Payload/bitstring>>
  end,
  [Bucket | generate_buckets(BucketNumbers - 1)].

% Merging list of buckets into bitmap %
merge_buckets([]) ->
  <<>>;
merge_buckets([Bucket | []]) ->
  Bucket;
merge_buckets([Bucket | ListOfBuckets]) ->
  Tail = merge_buckets(ListOfBuckets),
  <<Bucket/bitstring, Tail/bitstring>>.

% Generate bitmap, then extract first bucket from it,
% and compare result with ecomet_bitmap:first%
f(0) ->
  ok;
f(TestNumber) ->
  BucketNumbers = rand:uniform(100),
  Buckets = generate_buckets(BucketNumbers),
  [Head | Tail] = Buckets,
  MergedTail = merge_buckets(Tail),
  {Head, MergedTail} = ecomet_bitmap:first(merge_buckets(Buckets)),
  f(TestNumber - 1).

% 4096 bit -> {HwordHeader, FullWordHeader, Payload}
% We divide 4096 bits into blocks. Each block contain 64 bits.
% if block[i] == 0, then HwordHeader i-th bit is 0, else 1
% if block[i] == 2^64 - 1, then FullWordHeader i-th bit is 1, else 0
% BlockNumber 63, ..., 0 %
transform(<<Head:?WORD_LENGTH, Rest/bitstring>>, BlockNumber) when Rest =:= <<>> ->
  ct:pal("BlockNumber ~p~n", [BlockNumber]),
  HWord = if Head =:= 0 -> 0; true -> 1 end,
  FullWord = if Head =:= (1 bsl 64 - 1) -> 1; true -> 0 end,
  Payload = if HWord =:= 0; FullWord =:= 1 -> <<>>; true -><<Head:?WORD_LENGTH>> end,

  {HWord, FullWord, Payload};

transform(<<Head:?WORD_LENGTH, Rest/bitstring>>, BlockNumber) ->
  ct:pal("BlockNumber ~p~n", [BlockNumber]),
  %Data = rand:uniform(1 bsl 4096 - 1) - 1,
  HWord = if Head =:= 0 -> 0; true -> 1 end,
  FullWord = if Head =:= (1 bsl 64 - 1) -> 1; true -> 0 end,
  Payload = if HWord =:= 0; FullWord =:= 1 -> <<>>; true -><<Head:?WORD_LENGTH>> end,

  {HW, FW, P} = transform(Rest, BlockNumber - 1),
  {(HWord bsl BlockNumber) bxor HW, (FullWord bsl BlockNumber) bxor FW, <<Payload/bitstring,P/bitstring>>}.

% Data is 4096 bit. We compress Data and obtain bucket %
data_to_bucket(Data) ->
  {Hword, FullWord, Payload} = transform(Data, 63),
  Bucket = if FullWord =:= 0 ->
    <<?W:1, ?X:1, Hword:?WORD_LENGTH,
      Payload/bitstring
    >>;
  true ->
    << ?W:1, ?X:1, Hword:?WORD_LENGTH,
      FullWord:?WORD_LENGTH, Payload/bitstring
    >>
  end,
  Bucket.

% Each element of 'ListOfData' is 4096 bits
% So len('ListOfData') == Number of Buckets
% Function merges 'ListOfData' into bitmap%
data_to_bitmap([]) ->
  <<>>;
data_to_bitmap(ListOfData) ->
  [Head | Tail] = ListOfData,
  Bucket = data_to_bucket(<<Head:4096>>),
  <<Bucket/bitstring, (data_to_bitmap(Tail))/bitstring>>.

% Function generate list of NumberOfBlock elements %
generate_list_of_random_data(0) ->
  [];
generate_list_of_random_data(NumberOfBlock) ->
  [rand:uniform(1 bsl 64 - 1) | generate_list_of_random_data(NumberOfBlock - 1)].

list_and(_L1, []) ->
  [];
list_and([], _L2) ->
  [];
list_and([Head1 | Tail1], [Head2 | Tail2]) ->
  [Head1 band Head2 | list_and(Tail1, Tail2)].

list_or(L1, []) ->
  L1;
list_or([], L2) ->
  L2;
list_or([Head1 | Tail1], [Head2 | Tail2]) ->
  [Head1 bor Head2 | list_or(Tail1, Tail2)].

list_andnot( [Head1 | Tail1], [Head2 | Tail2] )->
  [ Head1 band (Head1 bxor Head2) | list_andnot(Tail1, Tail2) ];
list_andnot( [], _T2 )->
  [];
list_andnot( T1, [] )->
  T1.

check_bitwise_oper(0, _BitWiseFunc, _ListWiseFunc) ->
  ok;
check_bitwise_oper(TestNumber, BitWiseFunc, ListWiseFunc) ->
  A = generate_list_of_random_data(rand:uniform(20)),
  B = generate_list_of_random_data(rand:uniform(20)),
  Bitmap1 = data_to_bitmap(A),
  Bitmap2 = data_to_bitmap(B),
  Result = data_to_bitmap(ListWiseFunc(A, B)),
  Result = BitWiseFunc(Bitmap1, Bitmap2),
  check_bitwise_oper(TestNumber - 1, BitWiseFunc, ListWiseFunc).
