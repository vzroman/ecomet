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
-module(ecomet_bitmap).

-define(WORD_LENGTH,64).

-define(FULL,2#1111111111111111111111111111111111111111111111111111111111111111).
-define(BIT(X), 1 bsl X).

%%------------------------------------------------------------------------------------
%%  BITS API
%%------------------------------------------------------------------------------------
-export([
  set_bit/2,
  reset_bit/2,
  get_bit/2,
  is_empty/1
]).

%%------------------------------------------------------------------------------------
%%  BITWISE OPERATIONS API
%%------------------------------------------------------------------------------------
-export([
  oper/3
]).

%%------------------------------------------------------------------------------------
%%  Iterators
%%------------------------------------------------------------------------------------
-export([
  count/1,
  foldl/4,
  foldr/4
]).


%%------------------------------------------------------------------------------------
%%  SET
%%------------------------------------------------------------------------------------
set_bit( none, Bit )->
  set_bit([], Bit);
set_bit( Bitmap, Bit )->

  BucketNum = Bit div ?WORD_LENGTH div ?WORD_LENGTH,
  HighBit = Bit div ?WORD_LENGTH rem ?WORD_LENGTH,
  LowBit = Bit rem ?WORD_LENGTH,

  Bucket = {BucketNum, [{HighBit, ?BIT(LowBit)}] },

  oper('OR',Bitmap,[Bucket]).

%%------------------------------------------------------------------------------------
%%  RESET
%%------------------------------------------------------------------------------------
reset_bit(none, Bit)->
  reset_bit([],Bit);
reset_bit(Bitmap, Bit)->
  BucketNum = Bit div ?WORD_LENGTH div ?WORD_LENGTH,
  HighBit = Bit div ?WORD_LENGTH rem ?WORD_LENGTH,
  LowBit = Bit rem ?WORD_LENGTH,

  Bucket = {BucketNum, [{HighBit, ?BIT(LowBit)}]},

  oper('ANDNOT',Bitmap,[Bucket]).


get_bit(Bitmap,Bit)->
  case is_empty(Bitmap) of
    true->false;
    _->
      BucketNum = Bit div ?WORD_LENGTH div ?WORD_LENGTH,
      case find_item(Bitmap,BucketNum) of
        undefined->
          false;
        ?FULL->
          true;
        Bucket->
          HighBit = Bit div ?WORD_LENGTH rem ?WORD_LENGTH,
          case find_item(Bucket,HighBit) of
            undefined->
              false;
            ?FULL->
              true;
            Word->
              LowBit = Bit rem ?WORD_LENGTH,
              (?BIT(LowBit)) band Word>0
          end
      end
  end.

is_empty(none)->
  true;
is_empty([])->
  true;
is_empty(_Other)->
  false.

count(none)->
  0;
count([])->
  0;
count([{_,Bucket} | Tail ])->
  count(Bucket) + count(Tail);
count(Word) when is_integer(Word)->
  bit_count(Word).


%%------------------------------------------------------------------------------------
%%  BITWISE OPERATIONS
%%------------------------------------------------------------------------------------
oper('AND',X1,X2)->
  if
    (X1==none) or (X2==none)->none;
    X1==start->X2;
    true->compress(item_and(X1,X2))
  end;
oper('OR',X1,X2)->
  if
    X1==none->X2;
    X2==none->X1;
    true->compress(item_or(X1,X2))
  end;
oper('ANDNOT',X1,X2)->
  if
    X1==none->none;
    X2==none->X1;
    true->compress(item_andnot(X1,X2))
  end.

%%------------------------------------------------------------------------------------
%%  AND
%%------------------------------------------------------------------------------------
item_and([],_V2)->
  [];
item_and(_V1,[])->
  [];
item_and([{N1,_}|T1], [{N2,_}|_] = V2) when N1 < N2->
  item_and(T1,V2);
item_and([{N1,_}|_]=V1, [{N2,_}|T2]) when N1 > N2->
  item_and(V1,T2);
item_and([{N,?FULL}|T1],[{N,V2}|T2])->
  [{N,V2}|item_and(T1,T2)];
item_and([{N,V1}|T1],[{N,?FULL}|T2])->
  [{N,V1}|item_and(T1,T2)];
item_and([{N,V1}|T1],[{N,V2}|T2])->
  [{N,item_and(V1,V2)}|item_and(T1,T2)];
item_and(V1,V2)->
  V1 band V2.

%%------------------------------------------------------------------------------------
%%  OR
%%------------------------------------------------------------------------------------
item_or([],V2)->
  V2;
item_or( V1, [] )->
  V1;
item_or([{N1,_}=I1|T1], [{N2,_}|_] = V2) when N1 < N2->
  [I1|item_or(T1,V2)];
item_or([{N1,_}|_]=V1, [{N2,_}=I2|T2]) when N1 > N2->
  [I2|item_or(V1,T2)];
item_or([{N,?FULL}|T1],[{N,_V2}|T2])->
  [{N,?FULL}|item_or(T1,T2)];
item_or([{N,_V1}|T1],[{N,?FULL}|T2])->
  [{N,?FULL}|item_or(T1,T2)];
item_or([{N,V1}|T1],[{N,V2}|T2])->
  [{N,item_or(V1,V2)}| item_or(T1,T2) ];
item_or(V1,V2)->
  V1 bor V2.

%%------------------------------------------------------------------------------------
%%  ANDNOT
%%------------------------------------------------------------------------------------
item_andnot( [], _V2 )->
  [];
item_andnot( V1, [] )->
  V1;
item_andnot([{N1,_}=I1|T1], [{N2,_}|_] = V2) when N1 < N2->
  [I1|item_andnot(T1,V2)];
item_andnot([{N1,_}|_]=V1, [{N2,_}|_]) when N1 > N2->
  V1;
item_andnot([{N,?FULL}|T1],[{N,V2}|T2]) when is_list(V2)->
  [{N,[item_andnot(?FULL,I2)||I2<-V2]}|item_andnot(T1,T2)];
item_andnot([{N,_V1}|T1],[{N,?FULL}|T2])->
  item_andnot(T1,T2);
item_andnot([{N,V1}|T1],[{N,V2}|T2])->
  [{N,item_andnot(V1,V2)}|item_andnot(T1,T2)];
item_andnot(V1,V2)->
  V1 band (V1 bxor V2).

%%------------------------------------------------------------------------------------
%%  Iterators
%%------------------------------------------------------------------------------------
% From the least significant bit to the most significant
foldl(_Fun,Acc,none,_Page)->
  {0,Acc};
foldl(_Fun,Acc,[],_Page)->
  {0,Acc};
foldl(Fun,InAcc,Bitmap,{From,To})->

  Iterator = get_iterator(From,To,Fun),

  lists:foldl(fun
    ({N,?FULL},TotalAcc)->
      BucketOffset = N * ?WORD_LENGTH*?WORD_LENGTH,
      full_foldl(BucketOffset,BucketOffset+?WORD_LENGTH*?WORD_LENGTH-1,TotalAcc,Iterator);
    ({N,Bucket},TotalAcc)->
    BucketOffset = N * ?WORD_LENGTH*?WORD_LENGTH,
    lists:foldl(fun({High,Word},BucketAcc)->
      WordOffset = High * ?WORD_LENGTH + BucketOffset,
      word_foldl(Word,WordOffset,BucketAcc,Iterator)
    end,TotalAcc,Bucket)
  end, {0,InAcc}, Bitmap ).


full_foldl(I,Stop,Acc,Fun) when I=<Stop->
  full_foldl(I+1,Stop,Fun(I,Acc),Fun);
full_foldl(_I,_Stop,Acc,_Fun)->
  Acc.

word_foldl(0,_I,Acc,_Fun)->
  Acc;
word_foldl(W,I,Acc,Fun) when W rem 2=:=1->
  word_foldl(W bsr 1,I+1,Fun(I,Acc),Fun);
word_foldl(W,I,Acc,Fun)->
  word_foldl(W bsr 1,I+1,Acc,Fun).

% From the most significant bit to the least significant
foldr(_F,Acc,none,_Page)->
  {0,Acc};
foldr(_F,Acc,[],_Page)->
  {0,Acc};
foldr(Fun,InAcc,Bitmap,{From,To})->
  Iterator = get_iterator(From,To,Fun),

  lists:foldr(fun
    ({N,?FULL},TotalAcc)->
      BucketOffset = N * ?WORD_LENGTH*?WORD_LENGTH,
      full_foldr(BucketOffset+?WORD_LENGTH*?WORD_LENGTH-1,BucketOffset,TotalAcc,Iterator);
    ({N,Bucket},TotalAcc)->
      BucketOffset = N * ?WORD_LENGTH*?WORD_LENGTH,
      lists:foldr(fun({High,Word},BucketAcc)->
        WordOffset = High * ?WORD_LENGTH + BucketOffset,
        word_foldr(Word,WordOffset,BucketAcc,Iterator)
      end,TotalAcc,Bucket)
  end, {0,InAcc}, Bitmap ).

word_foldr(0,_I,Acc,_Fun)->
  Acc;
word_foldr(W,I,Acc,Fun) when W rem 2=:=1->
  Fun(I,word_foldr( W bsr 1, I+1, Acc, Fun ));
word_foldr(W,I,Acc,Fun)->
  word_foldr(W bsr 1,I+1,Acc,Fun).

full_foldr(I,Stop,Acc,Fun) when I>=Stop->
  full_foldr(I-1,Stop,Fun(I,Acc),Fun);
full_foldr(_I,_Stop,Acc,_Fun)->
  Acc.

get_iterator(From,To,Fun)->
  Start=if is_integer(From)->From; true->0 end,
  Stop=if is_integer(To)->To; true->-1 end,
  fun(Bit,{Count,Acc})->
    Count1 = Count+1,
    Acc1=
      if
        Count1 =< Start->
          Acc;
        Count1 > Stop, Stop=/=-1->
          Acc;
        true ->
          Fun(Bit, Acc)
      end,
    {Count1, Acc1}
  end.

%%------------------------------------------------------------------------------------
%%  Bucket utilities
%%------------------------------------------------------------------------------------
find_item([],_I)->
  undefined;
find_item([{N,_Value}|Rest],I) when I>N->
  find_item(Rest,I);
find_item([{N,_Value}|_Rest],I) when I<N->
  undefined;
find_item([{N,Value}|_Rest],N)  ->
  Value.

compress([{N,Bucket}|Tail])->
  case zip_bucket(Bucket) of
    []->compress(Tail);
    Bucket1->
      [{N,Bucket1}|zip_bucket(Tail)]
  end;
compress([])->
  [].

zip_bucket(Bucket)->
  case [ W || W={_,V}<-Bucket, V=/=0 ] of
    Words when length(Words)=:=?WORD_LENGTH->
      % Potentially full bucket
      case [W || W={_,V}<-Words, V=/=?FULL] of
        []->?FULL;
        _->Words
      end;
    Words->
      Words
  end.

bit_count(0)->
  0;
bit_count(Word)->
  (Word rem 2) + bit_count( Word bsr 1 ).






