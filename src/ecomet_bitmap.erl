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

-define(F,1).
-define(W,0).

-define(X,0).
-define(XX,1).

-define(FULL,2#1111111111111111111111111111111111111111111111111111111111111111).

-define(BIT(X), 1 bsl X).

-define(SHORT,5).
-define(LONG,29).

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
  oper/3,
  bit_and/2,
  bit_or/2,
  bit_andnot/2
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
  set_bit(<<>>, Bit);
set_bit( Bitmap, Bit )->

  BucketNum = Bit div ?WORD_LENGTH div ?WORD_LENGTH,
  HighBit = Bit div ?WORD_LENGTH rem ?WORD_LENGTH,
  LowBit = Bit rem ?WORD_LENGTH,

  Bucket = << ?W:1,?X:1, (?BIT(HighBit)):?WORD_LENGTH, (?BIT(LowBit)):?WORD_LENGTH>>,
  Prefix = fill(0,BucketNum),

  bit_or(Bitmap,<<Prefix/bitstring,Bucket/bitstring>>).

%%------------------------------------------------------------------------------------
%%  RESET
%%------------------------------------------------------------------------------------
reset_bit(none, Bit)->
  reset_bit(<<>>,Bit);
reset_bit(Bitmap, Bit)->
  BucketNum = Bit div ?WORD_LENGTH div ?WORD_LENGTH,
  HighBit = Bit div ?WORD_LENGTH rem ?WORD_LENGTH,
  LowBit = Bit rem ?WORD_LENGTH,

  Bucket = << ?W:1,?X:1, (?BIT(HighBit)):?WORD_LENGTH, (?BIT(LowBit)):?WORD_LENGTH>>,
  Prefix = fill(0,BucketNum),

  bit_andnot(Bitmap,<<Prefix/bitstring,Bucket/bitstring>>).

get_bit(Bitmap,Bit)->
  case is_empty(Bitmap) of
    true->false;
    _->
      BucketNum = Bit div ?WORD_LENGTH div ?WORD_LENGTH,
      case find_bucket(Bitmap,BucketNum) of
        <<?F:1,V:1,_/bitstring>>->
          V=:=1;
        <<?W:1,?X:1,Head:?WORD_LENGTH,Data/bitstring>>->
          HighBit = Bit div ?WORD_LENGTH rem ?WORD_LENGTH,
          if
            (?BIT(HighBit)) band Head =:= 0 -> false ;
            true ->
              Word = find_word( HighBit, Head, 0, Data ),
              LowBit = Bit rem ?WORD_LENGTH,
              (?BIT(LowBit)) band Word>0
          end;
        <<?W:1,?XX:1,Head:?WORD_LENGTH,Full:?WORD_LENGTH,Data/bitstring>>->
          HighBit = Bit div ?WORD_LENGTH rem ?WORD_LENGTH,
          if
            (?BIT(HighBit)) band Head =:= 0 -> false ;
            true ->
              if
                (?BIT(HighBit)) band Full >0 -> true ;
                true ->
                  Word = find_word( HighBit, Head, Full, Data ),
                  LowBit = Bit rem ?WORD_LENGTH,
                  (?BIT(LowBit)) band Word>0
              end
          end
      end
  end.

is_empty(none)->
  true;
is_empty(<<>>)->
  true;
is_empty(<<?F:1,0:1,?X:1,_:?SHORT,Tail/bitstring>>)->
  is_empty(Tail);
is_empty(<<?F:1,0:1,?XX:1,_:?LONG,Tail/bitstring>>)->
  is_empty(Tail);
is_empty(_Other)->
  false.

count(none)->
  0;
count(<<>>)->
  0;
count(<<?F:1,V:1,?X:1,L:?SHORT,Tail/bitstring>>)->
  V * L * ?WORD_LENGTH * ?WORD_LENGTH + count(Tail);
count(<<?F:1,V:1,?XX:1,L:?LONG,Tail/bitstring>>)->
  V * L * ?WORD_LENGTH * ?WORD_LENGTH + count(Tail);
count(Bitmap)->
  { Bucket, Tail } = first(Bitmap),
  bucket_count(Bucket) + count(Tail).

bucket_count(<<?W:1,?X:1,Head:?WORD_LENGTH,Data/bitstring>>)->
  data_count(Head,_Full=0,Data,0);
bucket_count(<<?W:1,?XX:1,Head:?WORD_LENGTH,Full:?WORD_LENGTH,Data/bitstring>>)->
  data_count(Head,Full,Data,0).

data_count(0,_F,_D,Acc)->
  Acc;
data_count(H,F,D,Acc) when H rem 2=:=0->
  data_count(H bsr 1,F bsr 1, D, Acc);
data_count(H,F,D,Acc) when F rem 2=:=1->
  data_count(H bsr 1,F bsr 1, D, ?WORD_LENGTH+Acc);
data_count(H,F,<<Word:?WORD_LENGTH,Tail/bitstring>>,Acc)->
  data_count( H bsr 1, F bsr 1, Tail, bit_count(Word) + Acc ).


%%------------------------------------------------------------------------------------
%%  BITWISE OPERATIONS
%%------------------------------------------------------------------------------------
oper('AND',X1,X2)->
  if
    (X1==none) or (X2==none)->none;
    X1==start->X2;
    true->bit_and(X1,X2)
  end;
oper('OR',X1,X2)->
  if
    X1==none->X2;
    X2==none->X1;
    true->bit_or(X1,X2)
  end;
oper('ANDNOT',X1,X2)->
  if
    X1==none->none;
    X2==none->X1;
    true->bit_andnot(X1,X2)
  end.

%%------------------------------------------------------------------------------------
%%  AND
%%------------------------------------------------------------------------------------
bit_and(<<>>,_Bitmap2)->
  <<>>;
bit_and(_Bitmap1,<<>>)->
  <<>>;
% Short empty
bit_and(<<?F:1,0:1,?X:1,L:?SHORT,Tail1/bitstring>>,Bitamp2)->
  bit_and(Tail1,tail(Bitamp2,L));
bit_and(Bitamp1,<<?F:1,0:1,?X:1,L:?SHORT,Tail2/bitstring>>)->
  bit_and(tail(Bitamp1,L),Tail2);
% Short full
bit_and(<<?F:1,1:1,?X:1,L:?SHORT,Tail1/bitstring>>,Bitamp2)->
  { Head2, Tail2 } = split( Bitamp2, L),
  <<Head2/bitstring,(bit_and(Tail1,Tail2))/bitstring>>;
bit_and(Bitamp1,<<?F:1,1:1,?X:1,L:?SHORT,Tail2/bitstring>>)->
  { Head1, Tail1 } = split( Bitamp1, L),
  <<Head1/bitstring,(bit_and(Tail1,Tail2))/bitstring>>;
% Long empty
bit_and(<<?F:1,0:1,?XX:1,L:?LONG,Tail1/bitstring>>,Bitamp2)->
  bit_and(Tail1,tail(Bitamp2,L));
bit_and(Bitamp1,<<?F:1,0:1,?XX:1,L:?LONG,Tail2/bitstring>>)->
  bit_and(tail(Bitamp1,L),Tail2);
% Long full
bit_and(<<?F:1,1:1,?XX:1,L:?LONG,Tail1/bitstring>>,Bitamp2)->
  { Head2, Tail2 } = split( Bitamp2, L),
  <<Head2/bitstring,(bit_and(Tail1,Tail2))/bitstring>>;
bit_and(Bitamp1,<<?F:1,1:1,?XX:1,L:?LONG,Tail2/bitstring>>)->
  { Head1, Tail1 } = split( Bitamp1, L),
  <<Head1/bitstring,(bit_and(Tail1,Tail2))/bitstring>>;

bit_and(Bitmap1,Bitmap2)->
  { Bucket1, Tail1 } = first(Bitmap1),
  { Bucket2, Tail2 } = first(Bitmap2),
  << (bucket_and(Bucket1,Bucket2))/bitstring, (bit_and( Tail1, Tail2 ))/bitstring >>.

bucket_and(Bucket1,Bucket2)->
  Data1 = decompress(Bucket1),
  Data2 = decompress(Bucket2),
  Data = data_and(Data1,Data2),
  compress(Data).

data_and( [W1|T1], [W2|T2] )->
  [ W1 band W2 | data_and(T1,T2) ];
data_and( [], _T2 )->
  [];
data_and( _T1, [] )->
  [].

%%------------------------------------------------------------------------------------
%%  OR
%%------------------------------------------------------------------------------------
bit_or( <<>>, Bitmap2 )->
  Bitmap2;
bit_or( Bitmap1, <<>> )->
  Bitmap1;
% Short empty
bit_or(<<?F:1,0:1,?X:1,L:?SHORT,Tail1/bitstring>>,Bitamp2)->
  { Head2, Tail2 } = split(Bitamp2, L),
  <<Head2/bitstring,(bit_or(Tail1,Tail2))/bitstring>>;
bit_or(Bitamp1,<<?F:1,0:1,?X:1,L:?SHORT,Tail2/bitstring>>)->
  { Head1, Tail1 } = split(Bitamp1, L),
  <<Head1/bitstring,(bit_or(Tail1,Tail2))/bitstring>>;
% Short full
bit_or(<<?F:1,1:1,?X:1,L:?SHORT,Tail1/bitstring>>,Bitamp2)->
  <<?F:1,1:1,?X:1,L:?SHORT,(bit_or(Tail1,tail(Bitamp2,L)))/bitstring>>;
bit_or(Bitamp1,<<?F:1,1:1,?X:1,L:?SHORT,Tail2/bitstring>>)->
  <<?F:1,1:1,?X:1,L:?SHORT,(bit_or(tail(Bitamp1,L),Tail2))/bitstring>>;
% Long empty
bit_or(<<?F:1,0:1,?XX:1,L:?LONG,Tail1/bitstring>>,Bitamp2)->
  { Head2, Tail2 } = split(Bitamp2, L),
  <<Head2/bitstring,(bit_or(Tail1,Tail2))/bitstring>>;
bit_or(Bitamp1,<<?F:1,0:1,?XX:1,L:?LONG,Tail2/bitstring>>)->
  { Head1, Tail1 } = split(Bitamp1, L),
  <<Head1/bitstring,(bit_or(Tail1,Tail2))/bitstring>>;
% Long full
bit_or(<<?F:1,1:1,?XX:1,L:?LONG,Tail1/bitstring>>,Bitamp2)->
  <<?F:1,1:1,?XX:1,L:?LONG,(bit_or(Tail1,tail(Bitamp2,L)))/bitstring>>;
bit_or(Bitamp1,<<?F:1,1:1,?XX:1,L:?LONG,Tail2/bitstring>>)->
  <<?F:1,1:1,?XX:1,L:?LONG,(bit_or(tail(Bitamp1,L),Tail2))/bitstring>>;

bit_or( Bitmap1, Bitmap2 )->
  { Bucket1, Tail1 } = first(Bitmap1),
  { Bucket2, Tail2 } = first(Bitmap2),
  << (bucket_or(Bucket1,Bucket2))/bitstring, (bit_or( Tail1, Tail2 ))/bitstring >>.
bucket_or(Bucket1,Bucket2)->
  Data1 = decompress(Bucket1),
  Data2 = decompress(Bucket2),
  Data = data_or(Data1,Data2),
  compress(Data).

data_or( [W1|T1], [W2|T2] )->
  [ W1 bor W2 | data_or(T1,T2) ];
data_or( [], T2 )->
  T2;
data_or( T1, [] )->
  T1.

%%------------------------------------------------------------------------------------
%%  ANDNOT
%%------------------------------------------------------------------------------------
bit_andnot( <<>>, _Bitmap2 )->
  <<>>;
bit_andnot( Bitmap1, <<>> )->
  Bitmap1;
% Short empty
bit_andnot(<<?F:1,0:1,?X:1,L:?SHORT,Tail1/bitstring>>,Bitamp2)->
  bit_andnot(Tail1,tail(Bitamp2,L));
bit_andnot(Bitamp1,<<?F:1,0:1,?X:1,L:?SHORT,Tail2/bitstring>>)->
  { Head1, Tail1 } = split(Bitamp1,L),
  <<Head1/bitstring,(bit_andnot(Tail1,Tail2))/bitstring>>;
% Short full
bit_andnot(<<?F:1,1:1,?X:1,L:?SHORT,Tail1/bitstring>>,Bitamp2)->
  { Head2, Tail2 } = split( Bitamp2, L),
  <<(bit_not(Head2))/bitstring,(bit_andnot(Tail1,Tail2))/bitstring>>;
bit_andnot(Bitamp1,<<?F:1,1:1,?X:1,L:?SHORT,Tail2/bitstring>>)->
  <<?F:1,0:1,?X:1,L:?SHORT,(bit_andnot(tail(Bitamp1,L),Tail2))/bitstring>>;
% Long empty
bit_andnot(<<?F:1,0:1,?XX:1,L:?LONG,Tail1/bitstring>>,Bitamp2)->
  bit_andnot(Tail1,tail(Bitamp2,L));
bit_andnot(Bitamp1,<<?F:1,0:1,?XX:1,L:?LONG,Tail2/bitstring>>)->
  { Head1, Tail1 } = split(Bitamp1,L),
  <<Head1/bitstring,(bit_andnot(Tail1,Tail2))/bitstring>>;
% Long full
bit_andnot(<<?F:1,1:1,?XX:1,L:?LONG,Tail1/bitstring>>,Bitamp2)->
  { Head2, Tail2 } = split( Bitamp2, L),
  <<(bit_not(Head2))/bitstring,(bit_andnot(Tail1,Tail2))/bitstring>>;
bit_andnot(Bitamp1,<<?F:1,1:1,?XX:1,L:?LONG,Tail2/bitstring>>)->
  <<?F:1,0:1,?XX:1,L:?LONG,(bit_andnot(tail(Bitamp1,L),Tail2))/bitstring>>;

bit_andnot( Bitmap1, Bitmap2 )->
  { Bucket1, Tail1 } = first(Bitmap1),
  { Bucket2, Tail2 } = first(Bitmap2),
  << (bucket_andnot(Bucket1,Bucket2))/bitstring, (bit_andnot( Tail1, Tail2 ))/bitstring >>.

bucket_andnot(Bucket1,Bucket2)->
  Data1 = decompress(Bucket1),
  Data2 = decompress(Bucket2),
  Data = data_andnot(Data1,Data2),
  compress(Data ).

data_andnot( [W1|T1], [W2|T2] )->
  [ W1 band (W1 bxor W2) | data_andnot(T1,T2) ];
data_andnot( [], _T2 )->
  [];
data_andnot( T1, [] )->
  T1.

%%------------------------------------------------------------------------------------
%%  NOT
%%------------------------------------------------------------------------------------
bit_not(<<>>)->
  <<>>;
% Short
bit_not(<<?F:1,0:1,?X:1,L:?SHORT,Tail/bitstring>>)->
  <<?F:1,1:1,?X:1,L:?SHORT,(bit_not(Tail))/bitstring>>;
bit_not(<<?F:1,1:1,?X:1,L:?SHORT,Tail/bitstring>>)->
  <<?F:1,0:1,?X:1,L:?SHORT,(bit_not(Tail))/bitstring>>;
% Long
bit_not(<<?F:1,0:1,?XX:1,L:?LONG,Tail/bitstring>>)->
  <<?F:1,1:1,?XX:1,L:?LONG,(bit_not(Tail))/bitstring>>;
bit_not(<<?F:1,1:1,?XX:1,L:?LONG,Tail/bitstring>>)->
  <<?F:1,0:1,?XX:1,L:?LONG,(bit_not(Tail))/bitstring>>;

bit_not(Bitmap)->
  { Bucket, Tail } = first(Bitmap),
  <<(bucket_not(Bucket))/bitstring,(bit_not(Tail))/bitstring>>.

bucket_not(Bucket1)->
  Data1 = decompress(Bucket1),
  Data = data_not(Data1),
  compress(Data).

data_not( [W1|T] )->
  [ W1  bxor ?FULL | data_not(T) ];
data_not( [])->
  [].

%%------------------------------------------------------------------------------------
%%  Iterators
%%------------------------------------------------------------------------------------
% From the least significant bit to the most significant
foldl(_F,Acc,none,_Page)->
  {0,Acc};
foldl(_F,Acc,<<>>,_Page)->
  {0,Acc};
foldl(F,InitAcc,Bitmap,{From,To})->
  Start=if is_integer(From)->From; true->0 end,
  Stop=if is_integer(To)->To; true->-1 end,

  Buckets=split_buckets(Bitmap),
  NumBuckets=lists:zip(lists:seq(0,length(Buckets)-1),Buckets),

  lists:foldl(fun({N,Bucket},TotalAcc)->
    Offset = N * ?WORD_LENGTH*?WORD_LENGTH,
    bucket_foldl(Bucket,TotalAcc,fun(Bit, {Count,Acc} )->
      Count1 = Count+1,
      Acc1=
        if
          Count1 =< Start->
            Acc;
         Count1 > Stop, Stop=/=-1->
            Acc;
          true ->
            F(Offset + Bit, Acc)
        end,
      {Count1, Acc1}
     end)
  end,{0,InitAcc}, NumBuckets ).

bucket_foldl(<<?EMPTY:2>>,Acc,_Fun)->
  Acc;
bucket_foldl(<<?FULL:2>>,Acc,Fun)->
  full_foldl(0,?WORD_LENGTH*?WORD_LENGTH-1,Acc,Fun);
bucket_foldl(Bucket,Acc,Fun)->
  Data = decompress(Bucket),
  data_foldl( 0, length(Data)-1, Data, Acc, Fun ).

data_foldl(I,Stop,_Data,Acc,_Fun) when I>Stop->
  Acc;
data_foldl(I,Stop,[0|Tail],Acc,Fun)->
  data_foldl(I+1,Stop,Tail,Acc,Fun);
data_foldl(I,Stop,[?FULL_WORD|Tail],Acc,Fun)->
  Offset = I * ?WORD_LENGTH,
  data_foldl(I+1,Stop,Tail,full_foldl(Offset, Offset+?WORD_LENGTH-1, Acc, Fun),Fun);
data_foldl(I,Stop,[Word|Tail],Acc,Fun)->
  data_foldl(I+1,Stop,Tail,word_foldl(Word,I*?WORD_LENGTH,Acc,Fun),Fun).


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
foldr(_F,Acc,<<>>,_Page)->
  {0,Acc};
foldr(F,InitAcc,Bitmap,{From,To})->
  Start=if is_integer(From)->From; true->0 end,
  Stop=if is_integer(To)->To; true->-1 end,

  Buckets=split_buckets(Bitmap),
  NumBuckets=lists:zip(lists:seq(0,length(Buckets)-1),Buckets),

  lists:foldr(fun({N,Bucket},TotalAcc)->
    Offset = N * ?WORD_LENGTH*?WORD_LENGTH,
    bucket_foldr(Bucket,TotalAcc,fun(Bit, {Count,Acc} )->
      Count1 = Count+1,
      Acc1=
        if
          Count1 =< Start->
            Acc;
          Count1 > Stop, Stop=/=-1->
            Acc;
          true ->
            F(Offset + Bit, Acc)
        end,
      {Count1, Acc1}
    end)
  end,{0,InitAcc}, NumBuckets ).

bucket_foldr(<<?EMPTY:2>>,Acc,_Fun)->
  Acc;
bucket_foldr(<<?FULL:2>>,Acc,Fun)->
  full_foldr(?WORD_LENGTH*?WORD_LENGTH-1,0,Acc,Fun);
bucket_foldr(Bucket,Acc,Fun)->
  Data = decompress(Bucket),
  data_foldr( length(Data)-1, 0 ,lists:reverse(Data), Acc, Fun ).

data_foldr(I,Stop,_Data,Acc,_Fun) when I<Stop->
  Acc;
data_foldr(I,Stop,[0|Tail],Acc,Fun)->
  data_foldr(I-1,Stop,Tail,Acc,Fun);
data_foldr(I,Stop,[?FULL_WORD|Tail],Acc,Fun)->
  Offset = I * ?WORD_LENGTH,
  data_foldr(I-1,Stop,Tail,full_foldr(Offset+?WORD_LENGTH-1, Offset, Acc, Fun),Fun);
data_foldr(I,Stop,[Word|Tail],Acc,Fun)->
  data_foldr(I-1,Stop,Tail,word_foldr(Word,I*?WORD_LENGTH,Acc,Fun),Fun).

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
%%------------------------------------------------------------------------------------
%%  Bucket utilities
%%------------------------------------------------------------------------------------
find_bucket(Bitmap,N)->
  case tail(Bitmap,N) of
    <<>>->
      <<?F:1,0:1,?X:1,1:?SHORT>>;
    <<?F:1,V:1,_/bitstring>>->
      <<?F:1,V:1,?X:1,1:?SHORT>>;
    Left->
      {Bucket,_Tail} = first(Left),
      Bucket
  end.

tail(<<>>,_N)->
  <<>>;
tail(Bitmap,N) when N =<0 ->
  Bitmap;
% Short
tail(<<?F:1,V:1,?X:1,L:?SHORT,Tail/bitstring>>,N) when N < L ->
  <<?F:1,V:1,?X:1,(L-N):?SHORT,Tail/bitstring>>;
tail(<<?F:1,_V:1,?X:1,L:?SHORT,Tail/bitstring>>,N) when N =:= L ->
  Tail;
tail(<<?F:1,_V:1,?X:1,L:?SHORT,Tail/bitstring>>,N) when N > L ->
  tail(Tail,N-L);
% Long
tail(<<?F:1,V:1,?XX:1,L:?LONG,Tail/bitstring>>,N) when N < L ->
  <<(fill(V,L-N))/bitstring,Tail/bitstring>>;
tail(<<?F:1,_V:1,?XX:1,L:?LONG,Tail/bitstring>>,N) when N =:= L ->
  Tail;
tail(<<?F:1,_V:1,?XX:1,L:?LONG,Tail/bitstring>>,N) when N > L ->
  tail(Tail,N-L);
tail(Bitmap,N)->
  {_Bucket, Tail } = first(Bitmap),
  tail( Tail, N-1 ).

split(Bitmap,N)->
  split(Bitmap,N,<<>>).
split(<<>>,_N, Head)->
  { Head, <<>> };
split(Bitmap, N, Head) when N=<0->
  { Head, Bitmap };
% Short
split(<<?F:1,V:1,?X:1,L:?SHORT,Tail/bitstring>>,N,Head) when N < L->
  { <<Head/bitstring,(fill(V,N))/bitstring>>, <<(fill(V,L-N))/bitstring,Tail/bitstring>> };
split(<<?F:1,V:1,?X:1,L:?SHORT,Tail/bitstring>>,N,Head) when N =:= L->
  { <<Head/bitstring,?F:1,V:1,?X:1,L:?SHORT>>, Tail };
split(<<?F:1,V:1,?X:1,L:?SHORT,Tail/bitstring>>,N, Head) when N > L->
  split(Tail,N-L,<<Head/bitstring,?F:1,V:1,?X:1,L:?SHORT>>);
% Long
split(<<?F:1,V:1,?XX:1,L:?LONG,Tail/bitstring>>,N,Head) when N < L->
  { <<Head/bitstring,(fill(V,N))/bitstring>>, <<(fill(V,L-N))/bitstring,Tail/bitstring>> };
split(<<?F:1,V:1,?XX:1,L:?LONG,Tail/bitstring>>,N,Head) when N =:= L->
  { <<Head/bitstring,?F:1,V:1,?XX:1,L:?LONG>>, Tail };
split(<<?F:1,V:1,?XX:1,L:?LONG,Tail/bitstring>>,N, Head) when N > L->
  split(Tail,N-L,<<Head/bitstring,?F:1,V:1,?X:1,L:?SHORT>>);
% Sparse
split(Bitmap, N, Head)->
  { Bucket, Tail } = first(Bitmap),
  split( Tail, N-1, <<Head/bitstring,Bucket/bitstring>> ).

split_buckets(<<>>)->
  [];
split_buckets(Bitmap)->
  {Bucket,Tail} = first(Bitmap),
  [Bucket|split_buckets(Tail)].

first(<<?W:1,?X:1,HWord:?WORD_LENGTH,Rest/bitstring>>)->
  Length = bit_count(HWord) * ?WORD_LENGTH,
  <<Data:Length,Tail/bitstring>> = Rest,
  {<<?W:1,?X:1,HWord:?WORD_LENGTH,Data:Length>>, Tail };
first(<<?W:1,?XX:1,HWord:?WORD_LENGTH,FullWords:?WORD_LENGTH,Rest/bitstring>>)->
  Length = (bit_count(HWord) - bit_count(FullWords)) * ?WORD_LENGTH,
  <<Data:Length,Tail/bitstring>> = Rest,
  {<<?W:1,?XX:1,HWord:?WORD_LENGTH,FullWords:?WORD_LENGTH,Data:Length>>, Tail }.

decompress(<<?W:1,?X:1,High:?WORD_LENGTH,Data/bitstring>>)->
  decompress(High,0,Data);
decompress(<<?W:1,?XX:1,High:?WORD_LENGTH,Full:?WORD_LENGTH,Data/bitstring>>)->
  decompress(High,Full,Data).
decompress(0,_F,_Data)->
  [];
decompress(H,F,Data) when F rem 2 =:= 1->
  [?FULL|decompress(H bsr 1,F bsr 1,Data)];
decompress(H,F,Data) when H rem 2=:=0->
  [0|decompress(H bsr 1,F bsr 1,Data)];
decompress(H,F,<<Word:?WORD_LENGTH,Tail/bitstring>>)->
  [Word|decompress(H bsr 1,F bsr 1,Tail)].

compress(Data)->
  compress(Data, _High=0, _Full=0, _Bit=1, <<>>).
compress([0|Tail],High,Full,Bit,Acc)->
  compress(Tail,High,Full,Bit bsl 1,Acc);
compress([?FULL|Tail],High,Full,Bit,Acc)->
  compress(Tail,High bor Bit,Full bor Bit, Bit bsl 1 ,Acc);
compress([Word|Tail],High,Full,Bit,Acc)->
  compress(Tail, High bor Bit, Full, Bit bsl 1, <<Acc/bitstring,Word:?WORD_LENGTH>> );
compress([],High,Full,_Bit,Data)->
  if
    High =:=0 ->
      <<?F:1,0:1,?X:1,1:?SHORT>>;
    Full =:= ?FULL->
      <<?F:1,1:1,?X:1,1:?SHORT>>;
    Full =:=0->
      <<?W:1,0:1,?X:1,High:?WORD_LENGTH, Data/bitstring >>;
    true ->
      <<?W:1,1:1,?XX:1,High:?WORD_LENGTH, Full:?WORD_LENGTH, Data/bitstring >>
  end.

find_word(0,_H, _F, <<Word:?WORD_LENGTH,_/bitstring>>)->
  Word;
find_word(N,H,F,Data) when F rem 2=:=1->
  find_word(N-1,H bsr 1,F bsr 1,Data);
find_word(N,H,F,Data) when H rem 2=:=0->
  find_word(N-1,H bsr 1,F bsr 1,Data);
find_word(N,H,F,<<_:?WORD_LENGTH,Tail/bitstring>>)->
  find_word(N-1,H bsr 1,F bsr 1,Tail).

fill(_,0)->
  <<>>;
fill(V,Length) when Length<(1 bsl 5)->
  <<?F:1,V:1,?X:1,Length:?SHORT>>;
fill(V,Length)->
  % Max length 1073741824
  <<?F:1,V:1,?XX:1,Length:?LONG>>.

bit_count(Value)->
  bit_count(Value,0).
bit_count(0,Acc)->
  Acc;
bit_count(Value,Acc)->
  Bit = Value rem 2,
  bit_count(Value bsr 1,Acc+Bit).






