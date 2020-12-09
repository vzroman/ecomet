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

-module(ecomet_bits_SUITE).

-include_lib("ecomet_schema.hrl").
-include_lib("ecomet.hrl").
-include_lib("ecomet_test.hrl").


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
  set_bit_test/1,
  reset_bit_test/1,
  get_bit_test/1,
  shrink_test/1,
  test_and_oper/1,
  test_or_oper/1,
  test_andnot_oper/1,
  test_xor_oper/1,
  lsb_test/1,
  msb_test/1,
  foldl_test/1,
  foldr_test/1,
  insert_test/1,
  bubles_add_test/1,
  bubbles_remove_test/1,
  bubbles_to_bits_test/1
]).

%%----Vectors-------
-export([
  fill_test/1,
  notx_test/1,
  tovector_test/1,
  splitr_test/1,splitl_test/1,
  tailr_test/1,taill_test/1,
  dropr_test/1,dropl_test/1,
  count_test/1
]).

all()->
  [
    set_bit_test,
    reset_bit_test,
    get_bit_test,
    shrink_test,
    {group,oper_test},
    lsb_test,
    msb_test,
    foldl_test,
    foldr_test,
    {group,bubbles_test},
    {group,vectors_test}
  ].

groups()->
  [
    {oper_test,
      [],  % Run strategy
      [                   % Cases:
        test_and_oper,     % AND operation
        test_or_oper,      % OR operation
        test_andnot_oper,  % ANDNOT operation
        test_xor_oper      % XOR operation
      ]
    },
    {bubbles_test,
      [],  % Run strategy
      [                   % Cases:
        insert_test,          % Bubles union
        bubles_add_test,      % Add bit
        bubbles_remove_test,  % Remove bit
        bubbles_to_bits_test  % Bubbles to bits
      ]
    },
    {vectors_test,
      [],  % Run strategy
      [
        fill_test,
        notx_test,
        tovector_test,
        splitr_test,splitl_test,
        tailr_test,taill_test,
        dropr_test,dropl_test,
        count_test
      ]
    }
  ].

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
set_bit_test(_Config) ->
  % Create bits
  {3,2#1}=ecomet_bits:set_bit(3,none),
  % Add bit to the left
  {3,2#101}=ecomet_bits:set_bit(5,{3,2#1}),
  % Add bit to the right
  {1,2#10101}=ecomet_bits:set_bit(1,{3,2#101}).

%--------------------------------------------------------------
% Reset bit
%--------------------------------------------------------------
reset_bit_test(_Config) ->
  % Reset bit in empty bits
  none=ecomet_bits:reset_bit(3,none),
  % Reset bit, that not set
  {1,2#10101}=ecomet_bits:reset_bit(4,{1,2#10101}),
  % Reset bit
  {1,2#10001}=ecomet_bits:reset_bit(3,{1,2#10101}),
  % Reset msb
  {1,2#101}=ecomet_bits:reset_bit(5,{1,2#10101}),
  % Reset lsb
  {1,2#10100}=ecomet_bits:reset_bit(1,{1,2#10101}),
  % Reset last bit
  none=ecomet_bits:reset_bit(3,{3,2#1}).

%--------------------------------------------------------------
% Get bit
%--------------------------------------------------------------
get_bit_test(_Config) ->
  % Get bit in empty bits
  false=ecomet_bits:get_bit(3,none),
  % Get bit, that not set
  false=ecomet_bits:get_bit(4,{1,2#100101}),
  % Get bit, that set
  true=ecomet_bits:get_bit(3,{1,2#10101}).

%--------------------------------------------------------------
% Shrink bits
%--------------------------------------------------------------
shrink_test(_Config) ->
  % Shrink empty bits
  none=ecomet_bits:shrink(none),
  % Shrink null
  none=ecomet_bits:shrink({5,0}),
  % No shrink possible
  {3,2#101}=ecomet_bits:shrink({3,2#101}),
  % No shrink possible
  {5,2#101}=ecomet_bits:shrink({3,2#10100}).

%--------------------------------------------------------------
% bits operations
%--------------------------------------------------------------
test_and_oper(_Config)->
  % Left is none
  none=ecomet_bits:oper('AND',none,{3,2#1010111}),
  % Left is null
  none=ecomet_bits:oper('AND',{2,2#0},{3,2#1010111}),
  % Left is start
  {3,2#1010111}=ecomet_bits:oper('AND',start,{3,2#1010111}),
  % Right is none
  none=ecomet_bits:oper('AND',{3,2#1010111},none),
  % Null result
  none=ecomet_bits:oper('AND',{3,2#10101},{3,2#10}),
  % Equal base
  {3,2#1000110}=ecomet_bits:oper('AND',{3,2#1010111},{3,2#1000110}),
  % Right base
  {3,2#100}=ecomet_bits:oper('AND',{5,2#101},{3,2#1101}),
  % Left base
  {3,2#100}=ecomet_bits:oper('AND',{3,2#1101},{5,2#101}),
  % No common base
  none=ecomet_bits:oper('AND',{3,2#1101},{50,2#101}).

test_or_oper(_Config)->
  % Left is none
  {3,2#1010111}=ecomet_bits:oper('OR',none,{3,2#1010111}),
  % Right is none
  {3,2#1010111}=ecomet_bits:oper('OR',{3,2#1010111},none),
  % Equal base
  {3,2#1010111}=ecomet_bits:oper('OR',{3,2#1010001},{3,2#110}),
  % Right base
  {3,2#11101}=ecomet_bits:oper('OR',{5,2#101},{3,2#1101}),
  % Left base
  {3,2#1010001101}=ecomet_bits:oper('OR',{3,2#1101},{10,2#101}).

test_andnot_oper(_Config)->
  % Left is none
  none=ecomet_bits:oper('ANDNOT',none,{3,2#1010111}),
  % Right is none
  {3,2#1010111}=ecomet_bits:oper('ANDNOT',{3,2#1010111},none),
  % Equal base
  {3,2#1010001}=ecomet_bits:oper('ANDNOT',{3,2#1010111},{3,2#110}),
  % Null result
  none=ecomet_bits:oper('ANDNOT',{3,2#1010111},{1,2#11111111111}),
  % Right base
  {1,2#101010000}=ecomet_bits:oper('ANDNOT',{3,2#1010111},{1,2#1100}),
  % Left base
  {1,2#1010011}=ecomet_bits:oper('ANDNOT',{1,2#1010111},{3,2#11}),
  % Nothing to cut off
  {1,2#1010111}=ecomet_bits:oper('ANDNOT',{1,2#1010111},{9,2#11}).

test_xor_oper(_Config)->
  % Left is none
  {3,2#1010111}=ecomet_bits:oper('XOR',none,{3,2#1010111}),
  % Right is none
  {3,2#1010111}=ecomet_bits:oper('XOR',{3,2#1010111},none),
  % Equal base
  {3,2#1010001}=ecomet_bits:oper('XOR',{3,2#1010111},{3,2#110}),
  % Null result (converted to none after shrink)
  {3,2#0}=ecomet_bits:oper('XOR',{3,2#1010111},{3,2#1010111}),
  % Right base
  {1,2#101010001}=ecomet_bits:oper('XOR',{3,2#1010111},{1,2#1101}),
  % Left base
  {1,2#1000011}=ecomet_bits:oper('XOR',{1,2#1010111},{3,2#101}).

% Least significant bit search
lsb_test(_Config) ->
  % +2
  5=ecomet_bits:lsb({3,2#100100}),
  % +5
  8=ecomet_bits:lsb({3,2#100100},5),
  % first
  3=ecomet_bits:lsb({3,2#100101}),
  % alone
  5=ecomet_bits:lsb({5,2#1}),
  % No next
  -1=ecomet_bits:lsb({5,2#1001},8),
  % None value
  -1=ecomet_bits:lsb(none),
  % Null value
  -1=ecomet_bits:lsb({3,2#0}),
  % Big value
  1025=ecomet_bits:lsb({3,2#10101 bsl 1022}),
  % Big value, big offset
  6022=ecomet_bits:lsb({5000,2#10101 bsl 1022}),
  % Very big value
  16025=ecomet_bits:lsb({3,2#10101 bsl 16022}),
  % Very big value, very big offset
  24022=ecomet_bits:lsb({8000,2#10101 bsl 16022}),
  % Very big value, very big offset, next
  24024=ecomet_bits:lsb({8000,2#10101 bsl 16022},24022).

% Most significant bit search
msb_test(_Config) ->
  % First
  8=ecomet_bits:msb({3,2#100100}),
  % Next
  5=ecomet_bits:msb({3,2#100100},8),
  % alone
  5=ecomet_bits:msb({5,2#1}),
  % No next
  -1=ecomet_bits:msb({5,2#1000},8),
  % None value
  -1=ecomet_bits:msb(none,8),
  % Null value
  -1=ecomet_bits:msb({4,2#0},8),
  % Big value
  -1=ecomet_bits:msb({10,2#0},8),
  % Very big value
  34012=ecomet_bits:msb({10,2#101 bsl 34000}),
  % Very big value, very big offset
  44002=ecomet_bits:msb({10000,2#101 bsl 34000}),
  % Very big value, very big offset, next
  44000=ecomet_bits:msb({10000,2#101 bsl 34000},44002).

% Iterator from lsb to msb
foldl_test(_Config) ->
  {0,[]}=ecomet_bits:foldl(fun(Bit,Res)->[Bit|Res] end,[],none,{none,none}),
  {3,[5,4,2]}=ecomet_bits:foldl(fun(Bit,Res)->[Bit|Res] end,[],{1,2#11010},{none,none}),
  {3,[2]}=ecomet_bits:foldl(fun(Bit,Res)->[Bit|Res] end,[],{1,2#11010},{0,1}),
  {3,[4,3]}=ecomet_bits:foldl(fun(Bit,Res)->[Bit|Res] end,[],{0,2#11010},{1,3}),
  {3,[9,8]}=ecomet_bits:foldl(fun(Bit,Res)->[Bit|Res] end,[],{5,2#11010},{1,3}),
  {9,[22,21,20]}=ecomet_bits:foldl(fun(Bit,Res)->[Bit|Res] end,[],{15,2#1011000011110011},{3,6}).

% Iterator from msb to lsb
foldr_test(_Config) ->
  {0,[]}=ecomet_bits:foldr(fun(Bit,Res)->[Bit|Res] end,[],none,{none,none}),
  {3,[2,4,5]}=ecomet_bits:foldr(fun(Bit,Res)->[Bit|Res] end,[],{1,2#11010},{none,none}),
  {3,[5]}=ecomet_bits:foldr(fun(Bit,Res)->[Bit|Res] end,[],{1,2#11010},{0,1}),
  {3,[1,3]}=ecomet_bits:foldr(fun(Bit,Res)->[Bit|Res] end,[],{0,2#11010},{1,3}),
  {3,[6,8]}=ecomet_bits:foldr(fun(Bit,Res)->[Bit|Res] end,[],{5,2#11010},{1,3}),
  {9,[20,21,22]}=ecomet_bits:foldr(fun(Bit,Res)->[Bit|Res] end,[],{15,2#1011000011110011},{3,6}).



%%-------------------------------------------------------------
%% Bubbles
%%-------------------------------------------------------------
%Bubles union
insert_test(_Config) ->
  %Exact inside
  {2,7,2#111011}=ecomet_bits:insert({2,7,2#101001},{3,6,2#1001}),
  %Right expand
  {1,7,2#1011011}=ecomet_bits:insert({2,7,2#101001},{1,4,2#1001}),
  %Left expand
  {2,12,2#10010101001}=ecomet_bits:insert({2,7,2#101001},{9,12,2#1001}),
  %Covered
  {0,9,2#1010100101}=ecomet_bits:insert({2,7,2#101001},{0,9,2#1000000001}).
%--------------------------------------------------------------
% Add bit
%--------------------------------------------------------------
bubles_add_test(_Config) ->
  % First bubble
  [{3,3,2#1}]=ecomet_bits:bubbles_add([],3),
  % Add bit to bubble
  [{3,5,2#101}]=ecomet_bits:bubbles_add([{3,3,2#1}],5),
  % New bubble
  [{3,5,2#101},{1000,1000,2#1}]=ecomet_bits:bubbles_add([{3,5,2#101}],1000),
  % Add bit to second
  [{3,5,2#101},{1000,1005,2#100001}]=ecomet_bits:bubbles_add([{3,5,2#101},{1000,1000,2#1}],1005),
  % Couple bubbles
  CoupleRes=ecomet_bits:bubbles_add([{3,5,2#101},{1000,1005,2#100001}],512),
  [{3,1005,_}]=CoupleRes,
  % Decouple bubbles
  [{3,5,2#101},{1000,1005,2#100001}]=ecomet_bits:bubbles_remove(CoupleRes,512).

%--------------------------------------------------------------
% Remove bit
%--------------------------------------------------------------
bubbles_remove_test(_Config) ->
  % Remove bit from second bubble
  [{3,5,2#101}]=ecomet_bits:bubbles_remove([{3,5,2#101},{1000,1000,2#1}],1000),
  % Remove bit, that not set
  [{3,5,2#101},{1000,1000,2#1}]=ecomet_bits:bubbles_remove([{3,5,2#101},{1000,1000,2#1}],1005),
  % Remove bit from first bubble
  [{4,5,2#11},{1000,1000,2#1}]=ecomet_bits:bubbles_remove([{3,5,2#111},{1000,1000,2#1}],3),
  % Remove bit from first bubble
  [{3,5,2#101},{1000,1000,2#1}]=ecomet_bits:bubbles_remove([{3,5,2#111},{1000,1000,2#1}],4),
  % Remove bit from first bubble
  [{5,5,2#1},{1000,1000,2#1}]=ecomet_bits:bubbles_remove([{3,5,2#101},{1000,1000,2#1}],3),
  % Remove first bubble
  [{1000,1000,2#1}]=ecomet_bits:bubbles_remove([{5,5,2#1},{1000,1000,2#1}],5),
  % Remove last bubble
  []=ecomet_bits:bubbles_remove([{1000,1000,2#1}],1000).
%--------------------------------------------------------------
% Bubbles to bits
%--------------------------------------------------------------
bubbles_to_bits_test(_Config)->
  X1=2#101,
  X2=2#100001,
  V=X1  bor (X2 bsl (1000-3)),
  {3,V}=ecomet_bits:bubbles_to_bits([{3,5,X1},{1000,1005,X2}]).


%--------------------------------------------------------------
% Build bitsring of defined length with 'true' bits
%--------------------------------------------------------------
fill_test(_Config) ->
  2#0=ecomet_bits:fill_bits(0),
  2#11=ecomet_bits:fill_bits(2),
  2#1111=ecomet_bits:fill_bits(4),
  2#1111111111=ecomet_bits:fill_bits(10).

%--------------------------------------------------------------
% Invert bit-string
%--------------------------------------------------------------
notx_test(_Config) ->
  2#0=ecomet_bits:notx(2#1),
  2#00110101=ecomet_bits:notx(2#11001010),
  2#11=ecomet_bits:notx(2#0,2),
  2#1110=ecomet_bits:notx(2#1,4).

%--------------------------------------------------------------
% Prepare bit-string for vector operations
%--------------------------------------------------------------
tovector_test(_Config)->
  {2#1,2#0,{0,0}}=ecomet_bits:to_vector({0,2#1}),
  {2#11001010,2#00110101,{9,2}}=ecomet_bits:to_vector({2,2#11001010}),
  {2#1011000011110011,2#0100111100001100,{30,15}}=ecomet_bits:to_vector({15,2#1011000011110011}).

%--------------------------------------------------------------
% Splitting continuous bits from right of the bit-string
%--------------------------------------------------------------
splitr_test(_Config) ->
  {1,none}=ecomet_bits:splitr(1,{2#1,2#0,{0,0}}),
  V00=ecomet_bits:to_vector({0,2#11001010}),
  {2,V01}=ecomet_bits:splitr(1,V00),
  {0,V00}=ecomet_bits:splitr(0,V00),
  % 2#001010
  {0,V01}=ecomet_bits:splitr(1,V01),
  {2,V02}=ecomet_bits:splitr(0,V01),
  % 2#1010
  {0,V02}=ecomet_bits:splitr(0,V02),
  {1,V03}=ecomet_bits:splitr(1,V02),
  % 2#010
  {0,V03}=ecomet_bits:splitr(1,V03),
  {1,V04}=ecomet_bits:splitr(0,V03),
  % 2#10
  {0,V04}=ecomet_bits:splitr(0,V04),
  {1,V05}=ecomet_bits:splitr(1,V04),
  % 2#0
  {0,V05}=ecomet_bits:splitr(1,V05),
  {1,none}=ecomet_bits:splitr(0,V05),
  % Tail test
  {4,none}=ecomet_bits:splitr(1,ecomet_bits:to_vector({0,2#1111})).

%--------------------------------------------------------------
% Splitting continuous bits from left of the bit-string
%--------------------------------------------------------------
splitl_test(_Config) ->
  {1,none}=ecomet_bits:splitl(1,{2#1,2#0,{0,0}}),
  V00=ecomet_bits:to_vector({0,2#11001010}),
  {0,V00}=ecomet_bits:splitl(1,V00),
  {1,V01}=ecomet_bits:splitl(0,V00),
  % 2#1100101
  {0,V01}=ecomet_bits:splitl(0,V01),
  {1,V02}=ecomet_bits:splitl(1,V01),
  % 2#110010
  {0,V02}=ecomet_bits:splitl(1,V02),
  {1,V03}=ecomet_bits:splitl(0,V02),
  % 2#11001
  {0,V03}=ecomet_bits:splitl(0,V03),
  {1,V04}=ecomet_bits:splitl(1,V03),
  % 2#1100
  {0,V04}=ecomet_bits:splitl(1,V04),
  {2,V05}=ecomet_bits:splitl(0,V04),
  % 2#11
  {0,V05}=ecomet_bits:splitl(0,V05),
  {2,none}=ecomet_bits:splitl(1,V05),
  % Tail test
  {4,none}=ecomet_bits:splitl(1,ecomet_bits:to_vector({0,2#1111})).

%--------------------------------------------------------------
% Drop bits from right of the vector
%--------------------------------------------------------------
tailr_test(_Config)->
  {2#11001010,2#00110101,{7,0}}=ecomet_bits:tailr(0,{2#11001010,2#00110101,{7,0}}),
  none=ecomet_bits:tailr(8,{2#11001010,2#00110101,{7,0}}),
  none=ecomet_bits:tailr(10,{2#11001010,2#00110101,{7,0}}),
  {2#001010,2#110101,{5,0}}=ecomet_bits:tailr(2,{2#11001010,2#00110101,{7,0}}),
  {2#0,2#1,{0,0}}=ecomet_bits:tailr(5,{2#001010,2#110101,{5,0}}).

%--------------------------------------------------------------
% Drop bits from left of the vector
%--------------------------------------------------------------
taill_test(_Config)->
  {2#11001010,2#00110101,{7,0}}=ecomet_bits:taill(0,{2#11001010,2#00110101,{7,0}}),
  none=ecomet_bits:taill(8,{2#11001010,2#00110101,{7,0}}),
  none=ecomet_bits:taill(10,{2#11001010,2#00110101,{7,0}}),
  {2#110010,2#001101,{7,2}}=ecomet_bits:taill(2,{2#11001010,2#00110101,{7,0}}),
  {2#1,2#0,{7,7}}=ecomet_bits:taill(5,{2#110010,2#001101,{7,2}}).

%--------------------------------------------------------------
% Drop defined count of the 'true' bits from right of the vector
%--------------------------------------------------------------
dropr_test(_Config)->
  {0,{2#11001010,2#00110101,{7,0}}}=ecomet_bits:dropr(0,{2#11001010,2#00110101,{7,0}}),

  {0,{2#1010,2#0101,{3,0}}}=ecomet_bits:dropr(2,{2#11001010,2#00110101,{7,0}}),
  {0,{2#10,2#01,{1,0}}}=ecomet_bits:dropr(3,{2#11001010,2#00110101,{7,0}}),
  {0,none}=ecomet_bits:dropr(4,{2#11001010,2#00110101,{7,0}}),
  {1,none}=ecomet_bits:dropr(5,{2#11001010,2#00110101,{7,0}}),

  {0,{2#110000,2#001111,{5,0}}}=ecomet_bits:dropr(2,{2#11110000,2#00001111,{7,0}}),
  {0,none}=ecomet_bits:dropr(4,{2#11110000,2#00001111,{7,0}}),
  {6,none}=ecomet_bits:dropr(10,{2#11110000,2#00001111,{7,0}}).

%--------------------------------------------------------------
% Drop defined count of the 'true' bits from right of the vector
%--------------------------------------------------------------
dropl_test(_Config)->
  {0,{2#11001010,2#00110101,{7,0}}}=ecomet_bits:dropl(0,{2#11001010,2#00110101,{7,0}}),

  {0,{2#11,2#00,{7,6}}}=ecomet_bits:dropl(2,{2#11001010,2#00110101,{7,0}}),
  {0,{2#1,2#0,{7,7}}}=ecomet_bits:dropl(3,{2#11001010,2#00110101,{7,0}}),
  {0,none}=ecomet_bits:dropl(4,{2#11001010,2#00110101,{7,0}}),
  {1,none}=ecomet_bits:dropl(5,{2#11001010,2#00110101,{7,0}}),

  {0,{2#11,2#00,{7,6}}}=ecomet_bits:dropl(2,{2#11110000,2#00001111,{7,0}}),
  {0,none}=ecomet_bits:dropl(4,{2#11110000,2#00001111,{7,0}}),
  {6,none}=ecomet_bits:dropl(10,{2#11110000,2#00001111,{7,0}}).

%--------------------------------------------------------------
% Get count of the 'true' bits in the bit-string
%--------------------------------------------------------------
count_test(_Config)->
  0=ecomet_bits:count(none,0),
  1=ecomet_bits:count(ecomet_bits:to_vector({5,2#10000000}),0),
  6=ecomet_bits:count(ecomet_bits:to_vector({0,2#10000000}),5),
  8=ecomet_bits:count(ecomet_bits:to_vector({0,2#1101111000001100}),0),
  4=ecomet_bits:count(ecomet_bits:to_vector({0,2#11001010}),0).
