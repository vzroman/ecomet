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
-module(ecomet_bits).

%% ====================================================================
%% API functions
%% ====================================================================
-export([
  bubbles_add/2,
  bubbles_remove/2,
  oper/3,
  bubbles_to_bits/1,
  lsb/2,
  lsb/1,
  msb/2,
  msb/1,
  shrink/1,
  set_bit/2,
  reset_bit/2,
  get_bit/2,
  foldr/4,foldl/4,
  insert/2,
  to_vector/1,from_vector/1,
  count/2
]).

%%-------Test API--------------
-export([
  splitr/2,splitl/2,
  notx/1,notx/2,
  fill_bits/1,
  tailr/2,taill/2,
  dropr/2,dropl/2
]).
-define(WEIGHT,512).

-define(HL,65535).
-define(HL2,32767).
-define(P1023,89884656743115795386465259539451236680898848947115328636715040578866337902750481566354238661203768010560056939935696678829394884407208311246423715319737062188883946712432742638151109800623047059726541476042502884419075341171231440736956555270413618581675255342293149119973622969239858152417678164812112068608).


%% ====================================================================
%% Bit logic
%% ====================================================================
%% Preparing
oper('AND',X1,X2)->
  if
    (X1==none) or (X2==none)->none;
    X1==start->X2;
    true->bit_and(base(X1,X2))
  end;
oper('OR',X1,X2)->
  if
    X1==none->X2;
    X2==none->X1;
    true->bit_or(base(X1,X2))
  end;
oper('ANDNOT',X1,X2)->
  if
    X1==none->none;
    X2==none->X1;
    true->bit_andnot(base(X1,X2))
  end;
oper('XOR',X1,X2)->
  if
    X1==none->X2;
    X2==none->X1;
    true->bit_xor(base(X1,X2))
  end.
%% Common base
base({Offset1,Value1},{Offset2,Value2})->
  if
    Offset1==Offset2->{Offset1,Value1,Value2};
    Offset1>=Offset2->{Offset2,Value1 bsl (Offset1-Offset2),Value2};
    true ->{Offset1,Value1,Value2 bsl (Offset2-Offset1)}
  end.
%% Shrink bits
shrink(BitsValue)->
  case BitsValue of
    none->none;
    {_Offset,0}->none;
    {Offset,Value}->
      case lsb(BitsValue) of
        Offset->{Offset,Value};
        NewOffset->{NewOffset,Value bsr (NewOffset-Offset)}
      end
  end.

%% Operations
bit_and({Offset,X1,X2})->
  case X1 band X2 of
    0->none;
    Res->{Offset,Res}
  end.
bit_or({Offset,X1,X2})->
  {Offset,X1 bor X2}.
bit_andnot({Offset,X1,X2})->
  bit_and({Offset,X1,X1 bxor X2}).
bit_xor({Offset,X1,X2})->
  {Offset,X1 bxor X2}.

% Set bit
set_bit(Bit,BitsValue)->
  case BitsValue of
    none->{Bit,1};
    {Offset,Value}->
      if
        Bit<Offset -> {Bit,(Value bsl (Offset-Bit)) bor 1};
        true -> {Offset,Value bor (1 bsl (Bit-Offset))}
      end
  end.
% Reset bit
reset_bit(Bit,BitsValue)->
  case BitsValue of
    none->none;
    {Offset,Value}->
      {NewOffset,NewValue}=
        if
          Bit<Offset -> {Bit,(Value bsl (Offset-Bit)) bor 1};
          true -> {Offset,Value band (Value bxor (1 bsl (Bit-Offset)))}
        end,
      if
        NewValue==0->none;
        true->{NewOffset,NewValue}
      end
  end.
% Get bit value
get_bit(Bit,BitsValue)->
  case BitsValue of
    none->false;
    {Offset,Value}->
      if
        Bit<Offset -> false;
        true ->
          Value band (1 bsl (Bit-Offset))=/=0
      end
  end.
%%=====================================================================
%% Convert bubbles to bits
%%=====================================================================
bubbles_to_bits([{LSB,_MSB,Value}|Rest])->
  concat_bubbles(Rest,{LSB,Value}).
concat_bubbles([{LSB,_MSB,Value}|Rest],Result)->
  concat_bubbles(Rest,bit_or(base(Result,{LSB,Value})));
concat_bubbles([],Result)->
  Result.
%%=====================================================================
%%	Add bit to bubbles
%%=====================================================================
% Let's scan which bubble to insert to
bubbles_add(Bubbles,Bit)->
  bubbles_add(Bubbles,Bit,[]).
%Bit is already here
bubbles_add([{LSB,MSB,Value}|Tail],Bit,Head) when (Bit==LSB) or (Bit==MSB)->
  build(Head,[{LSB,MSB,Value}|Tail]);
%Analyze bubble
bubbles_add([{LSB,MSB,Value}|Tail],Bit,Head)->
  case {(Bit>=LSB-?WEIGHT),(Bit=<MSB+?WEIGHT)} of
    %It's our bubble
    {true,true}->check_add(insert({Bit,Bit,1},{LSB,MSB,Value}),Head,Tail);
    %Next bubble too far
    {false,_}->build(Head,[{Bit,Bit,1}|Tail]);
    %It's not our bubble
    {_,_}->bubbles_add(Tail,Bit,[{LSB,MSB,Value}|Head])
  end;
% No suitable bubbles, create new one
bubbles_add([],Bit,Head)->
  build(Head,[{Bit,Bit,1}]).

%Union bubbles
insert({LSBx,MSBx,ValueX},{LSB,MSB,Value})->
  case {LSB<LSBx,MSBx<MSB} of
    %Exact inside
    {true,true}->{LSB,MSB,Value bor (ValueX bsl (LSBx-LSB))};
    %Right expand
    {false,true}->{LSBx,MSB,(Value bsl (LSB-LSBx)) bor ValueX};
    %Left expand
    {true,false}->{LSB,MSBx,Value bor (ValueX bsl (LSBx-LSB))};
    %Covered
    {false,false}->{LSBx,MSBx,(Value bsl (LSB-LSBx)) bor ValueX}
  end.

%Check if bubble now can consume neighbor
check_add({LSBx,MSBx,ValueX},Head,Tail)->
  case {Head,Tail} of
    % Right bubble close enough
    {[{LSB1,MSB1,Value1}|Prev],_} when (LSBx-?WEIGHT=<MSB1)->
      check_add(insert({LSBx,MSBx,ValueX},{LSB1,MSB1,Value1}),Prev,Tail);
    % Left bubble close enough
    {_,[{LSB2,MSB2,Value2}|Next]} when (MSBx+?WEIGHT>=LSB2)->
      check_add(insert({LSBx,MSBx,ValueX},{LSB2,MSB2,Value2}),Head,Next);
    _->
      build(Head,[{LSBx,MSBx,ValueX}|Tail])
  end.

%%=====================================================================
%%	Delete bit from bubbles
%%=====================================================================
% Let's scan which bubble it's inside
bubbles_remove(Bubbles,Bit)->
  bubbles_remove(Bubbles,Bit,[]).
% Analyze bubble
bubbles_remove([{LSB,MSB,Value}|Tail],Bit,Head)->
  case {LSB=<Bit,Bit=<MSB} of
    %It's our bubble
    {true,true}->build(Head,lists:append(delete(Bit,{LSB,MSB,Value}),Tail));
    %It's not, let's look next
    {true,false}->bubbles_remove(Tail,Bit,[{LSB,MSB,Value}|Head]);
    % It's not the one, and no sense to search more
    {false,_}->build(Head,[{LSB,MSB,Value}|Tail])
  end;
% Bubble not found, nothing to delete
bubbles_remove([],_Bit,Head)->
  build(Head,[]).

%Delete bit from bubble
delete(Bit,{LSB,MSB,Value})->
  case {Bit==LSB,Bit==MSB} of
    %Bit is exact inside
    {false,false}->
      check_del({LSB,MSB,Value band (Value bxor (1 bsl (Bit-LSB)))},Bit);
    %No bits left
    {true,true}->[];
    %Bit is right most
    {true,false}->
      NewLSB=lsb({LSB,Value},LSB+1),
      [{NewLSB,MSB,Value bsr (NewLSB-LSB)}];
    %Bit is left most
    {false,true}->
      [{LSB,msb({LSB,Value},MSB-1),Value band  (Value bxor (1 bsl (Bit-LSB)))}]
  end.
% Check if bubble must be splitted
check_del({LSB,MSB,Value},Bit)->
  MSB1=msb({LSB,Value},Bit),
  LSB2=lsb({LSB,Value},Bit),
  case (LSB2-MSB1>?WEIGHT) of
    % Bubble is strong enough
    false->[{LSB,MSB,Value}];
    % Split
    true->[{LSB,MSB1,Value rem (1 bsl MSB1)},{LSB2,MSB,Value div (1 bsl (LSB2-LSB))}]
  end.

%Construct sorted list of bubbles
build(Head,Tail)->
  lists:append(lists:reverse(Head),Tail).

% LSB search
lsb(none)->-1;
lsb(Bits)->lsb(Bits,-1).

lsb(none,_Offset)->-1;
lsb({LSB,Value},Offset)->
  {Bit,Val}=
    if
      (Offset+1)>LSB->
        {Offset+1,Value bsr (Offset+1-LSB)};
      true->{LSB,Value}
    end,
  if
    Val==0->-1;
    true->log2(Val band -Val,0)+Bit
  end.

% MSB search
msb(none)->-1;
msb({LSB,Value})->
  if
    Value==0->-1;
    true->log2(Value,0)+LSB
  end.

msb(none,_Offset)->-1;
msb({_LSB,0},_Offset)->-1;
msb({LSB,Value},Offset)->
  msb({LSB,Value rem (1 bsl (Offset-LSB))}).

% folding from lsb to msb
foldl(_F,Acc,none,_Page)->{0,Acc};
foldl(F,Acc,Bits,{From,To})->
  {X,Xnot,{MSB,LSB}}=to_vector(Bits),
  Start=if From==none->0; true->From end,
  Stop=if To==none->MSB+1; true->To end,
  foldlv({X,Xnot,{MSB,LSB}},F,Acc,{Start,Stop},LSB,0).

foldlv(none,_F,Acc,_Page,_Pos,Count)->{Count,Acc};
foldlv(Vector,F,Acc,{From,To},Pos,Count)->
  {L1,Tail0}=splitl(1,Vector),
  {L0,Tail1}=splitl(0,Tail0),
  NewCount=Count+L1,
  NewAcc=
    if
      (NewCount<From) or (Count>To)->Acc;
      true->
        {Shift,Start}=
          if From>Count->
            {From-Count,From};
            true->{0,Count}
          end,
        End=if NewCount<To->NewCount; true->To end,
        runl(End-Start,F,Acc,Pos+Shift)
    end,
  foldlv(Tail1,F,NewAcc,{From,To},Pos+L1+L0,NewCount).
runl(0,_F,Acc,_Pos)->Acc;
runl(C,F,Acc,Pos)->
  runl(C-1,F,F(Pos,Acc),Pos+1).

foldr(_F,Acc,none,_Page)->{0,Acc};
foldr(F,Acc,Bits,{From,To})->
  {X,Xnot,{MSB,LSB}}=to_vector(Bits),
  Start=if From==none->0; true->From end,
  Stop=if To==none->MSB+1; true->To end,
  foldrv({X,Xnot,{MSB,LSB}},F,Acc,{Start,Stop},MSB,0).

foldrv(none,_F,Acc,_Page,_Pos,Count)->{Count,Acc};
foldrv(Vector,F,Acc,{From,To},Pos,Count)->
  {L1,Tail0}=splitr(1,Vector),
  {L0,Tail1}=splitr(0,Tail0),
  NewCount=Count+L1,
  NewAcc=
    if
      (NewCount<From) or (Count>To)->Acc;
      true->
        {Shift,Start}=if From>Count->{From-Count,From}; true->{0,Count} end,
        End=if NewCount<To->NewCount; true->To end,
        runr(End-Start,F,Acc,Pos-Shift)
    end,
  foldrv(Tail1,F,NewAcc,{From,To},Pos-L1-L0,NewCount).
runr(0,_F,Acc,_Pos)->Acc;
runr(C,F,Acc,Pos)->
  runr(C-1,F,F(Pos,Acc),Pos-1).



% Builds inverted bit-string.
% 2#1001 1100 1010 0110
% is transformed to
% 2#0110 0011 0101 1001
notx(X)->notx(X,log2(X,0)+1).
notx(X,L)->fill_bits(L) bxor X.

% Creates the bit-string of the MSB length that includes only 1
fill_bits(MSB)->(1 bsl MSB)-1.

% Prepare bit-string for vector operations
to_vector({LSB,X})->
  MSB=log2(X,0),
  {X,notx(X,MSB+1),{MSB+LSB,LSB}}.
from_vector(none)->none;
from_vector({X,_,{_,LSB}})->{LSB,X}.

% Split solid continuous bits from the bit string
% arg1 = 1 - continuous 'true' bits
% arg1 = 0 - continuous 'false' bits
% Returns {Count,RestVector}
% splitr - split from the right side of the bit-string
% splitl - from the left side
splitr(_,none)->{0,none};
splitr(1,{_X,0,{MSB,LSB}})->{MSB-LSB+1,none};
splitr(1,{X,Xnot,{MSB,LSB}})->
  Next=log2(Xnot,0),
  Shift=1 bsl (Next+1),
  NextMSB=Next+LSB,
  {MSB-NextMSB,{X rem Shift,Xnot rem Shift,{NextMSB,LSB}}};
splitr(0,{0,_Xnot,{MSB,LSB}})->{MSB-LSB+1,none};
splitr(0,{X,Xnot,{MSB,LSB}})->
  Next=log2(X,0),
  Shift=1 bsl (Next+1),
  NextMSB=Next+LSB,
  {MSB-NextMSB,{X rem Shift,Xnot rem Shift,{NextMSB,LSB}}}.

splitl(_,none)->{0,none};
splitl(1,{_X,0,{MSB,LSB}})->{MSB-LSB+1,none};
splitl(1,{X,Xnot,{MSB,LSB}})->
  Next=log2(Xnot band -Xnot,0),
  {Next,{X bsr Next,Xnot bsr Next,{MSB,LSB+Next}}};
splitl(0,{0,_Xnot,{MSB,LSB}})->{MSB-LSB+1,none};
splitl(0,{X,Xnot,{MSB,LSB}})->
  Next=log2(X band -X,0),
  {Next,{X bsr Next,Xnot bsr Next,{MSB,LSB+Next}}}.

% Returns vector without L bit:
% tailr - from the right side of the bit-string
% taill - from the left side
tailr(L,{_X,_Xnot,{MSB,LSB}}) when L>(MSB-LSB)->none;
tailr(L,{X,Xnot,{MSB,LSB}})->
  Shift=1 bsl (MSB-L+1),
  {X rem Shift,Xnot rem Shift,{MSB-L,LSB}}.
taill(L,{_X,_Xnot,{MSB,LSB}}) when L>(MSB-LSB)->none;
taill(L,{X,Xnot,{MSB,LSB}})->
  {X bsr L,Xnot bsr L,{MSB,LSB+L}}.

% Drops defined number of 'true' bits from the the bit-string.
% dropr - from the right side of the string
% dropl - from the left side
dropr(0,Vector)->{0,Vector};
dropr(Count,none)->{Count,none};
dropr(Count,Vector)->
  case splitr(1,Vector) of
    {Head,_} when Head>Count->{0,tailr(Count,Vector)};
    {Head,none}->{Count-Head,none};
    {Head,Tail}->
      {_,Tail_1}=splitr(0,Tail),
      dropr(Count-Head,Tail_1)
  end.
dropl(0,Vector)->{0,Vector};
dropl(Count,none)->{Count,none};
dropl(Count,Vector)->
  case splitl(1,Vector) of
    {Head,_} when Head>Count->{0,taill(Count,Vector)};
    {Head,none}->{Count-Head,none};
    {Head,Tail}->
      {_,Tail_1}=splitl(0,Tail),
      dropl(Count-Head,Tail_1)
  end.

count(none,Sum)->Sum;
count(Vector,Sum)->
  {Count,Tail}=splitr(1,Vector),
  if
    Tail==none ->Sum+Count;
    true->
      {_,Tail_1}=splitr(0,Tail),
      count(Tail_1,Sum+Count)
  end.

% Logarithm base 2. OS max input=2^1023
log2(X,Offset)->
  case (X bsr ?HL) of
    0->log2_2(X,?HL2,Offset);
    Div->log2(Div,Offset+?HL)
  end.
log2_2(X,_Step,Offset) when X=< ?P1023->
  case trunc(math:log(X bor (X bsr 1))/math:log(2)) of
    Log2 when (1 bsl Log2)>X->Log2+Offset-1;
    Log2->Log2+Offset
  end;
log2_2(X,Step,Offset)->
  case X bsr Step of
    0->log2_2(X,Step bsr 1,Offset);
    Div->log2_2(Div,Step bsr 1,Offset+Step)
  end.

