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
  zip/1
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
%%  Compressing
%%------------------------------------------------------------------------------------
zip(<<>>)->
  <<>>;
% Compress
zip(<<?F:1,V:1,?X:1,L1:?SHORT,?F:1,V:1,?X:1,L2:?SHORT,Tail/bitstring>>)->
  zip(<<(fill(V,L1+L2))/bitstring,Tail/bitstring>>);
zip(<<?F:1,V:1,?X:1,L1:?SHORT,?F:1,V:1,?XX:1,L2:?LONG,Tail/bitstring>>)->
  zip(<<(fill(V,L1+L2))/bitstring,Tail/bitstring>>);
zip(<<?F:1,V:1,?XX:1,L1:?LONG,?F:1,V:1,?X:1,L2:?SHORT,Tail/bitstring>>)->
  zip(<<(fill(V,L1+L2))/bitstring,Tail/bitstring>>);
zip(<<?F:1,V:1,?XX:1,L1:?LONG,?F:1,V:1,?XX:1,L2:?LONG,Tail/bitstring>>)->
  zip(<<(fill(V,L1+L2))/bitstring,Tail/bitstring>>);
% Empty end
zip(<<?F:1,0:1,?X:1,_:?SHORT>>)->
  <<>>;
zip(<<?F:1,0:1,?XX:1,_L:?LONG>>)->
  <<>>;
% Run fill
zip(<<?F:1,V:1,?X:1,L:?SHORT,Tail/bitstring>>)->
  <<?F:1,V:1,?X:1,L:?SHORT,(zip(Tail))/bitstring>>;
zip(<<?F:1,V:1,?XX:1,L:?LONG,Tail/bitstring>>)->
  <<?F:1,V:1,?XX:1,L:?LONG,(zip(Tail))/bitstring>>;
% Sparse
zip(Bitmap)->
  { Bucket, Tail } = first(Bitmap),
  <<Bucket/bitstring,(zip(Tail))/bitstring>>.

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

bucket_not(<<?W:1,_:1,Head:?WORD_LENGTH,_/bitstring>> = Bucket)->
  Data1 = decompress(Bucket),
  Data = data_not(Data1),
  Full = Head bxor ?FULL,
  compress(Data, Full, Full, _Bit=1, <<>>).

data_not( [W1|T] )->
  [ W1  bxor ?FULL | data_not(T) ];
data_not( [])->
  [].

%%------------------------------------------------------------------------------------
%%  From the less significant to the most significant iterator
%%------------------------------------------------------------------------------------
foldl(_Fun,Acc,none,_Page)->
  {0,Acc};
foldl(_Fun,Acc,[],_Page)->
  {0,Acc};
foldl(Fun,InAcc,Bitmap,{From,To})->
  Iterator = get_iterator(From,To,Fun),
  Buckets=split_buckets(Bitmap),
  buckets_foldl(Buckets,Iterator,{ _Count=0, InAcc }).

% Short
buckets_foldl([ { _N, <<?F:1,0:1,?X:1,_L:?SHORT>> } | Tail ], Fun, Acc )->
  buckets_foldl( Tail, Fun, Acc );
buckets_foldl([ { N, <<?F:1,1:1,?X:1,L:?SHORT>> } | Tail ], Fun, Acc )->
  From = N * ?WORD_LENGTH*?WORD_LENGTH,
  To = L * ?WORD_LENGTH * ?WORD_LENGTH + From - 1,
  buckets_foldl( Tail, Fun, full_foldl(From,To,Fun,Acc) );
% Long
buckets_foldl([ {_N, <<?F:1,0:1,?XX:1,_L:?LONG>>} | Tail ], Fun, Acc )->
  buckets_foldl( Tail, Fun, Acc );
buckets_foldl([ {N, <<?F:1,1:1,?XX:1,L:?LONG>>} | Tail ], Fun, Acc )->
  From = N * ?WORD_LENGTH*?WORD_LENGTH,
  To = L * ?WORD_LENGTH * ?WORD_LENGTH + From - 1,
  buckets_foldl( Tail, Fun, full_foldl(From,To,Acc,Fun) );

buckets_foldl([ {N, Bucket} | Tail ], Fun, Acc )->
  Data = decompress(Bucket),
  Offset = N * ?WORD_LENGTH,
  buckets_foldl(Tail, Fun, data_foldl( Data, Offset, Fun, Acc ) );

buckets_foldl([], _Fun, Acc )->
  Acc.

data_foldl([0|Tail], N, Fun, Acc)->
  data_foldl( Tail, N+1, Fun, Acc );
data_foldl([?FULL|Tail], N, Fun, Acc)->
  From = N * ?WORD_LENGTH,
  To = From + ?WORD_LENGTH - 1,
  data_foldl(Tail, N+1, Fun, full_foldl(From, To, Fun, Acc) );
data_foldl([Word|Tail],N,Fun,Acc)->
  Offset = N*?WORD_LENGTH,
  data_foldl(Tail, N+1, Fun, word_foldl(Word,Offset,Fun,Acc) );
data_foldl([],_N,_Fun,Acc)->
  Acc.

full_foldl(I,Stop,Fun,Acc) when I=<Stop->
  full_foldl(I+1, Stop, Fun, Fun(I,Acc) );
full_foldl(_I,_Stop,_Fun,Acc)->
  Acc.

word_foldl(0,_I,_Fun,Acc)->
  Acc;
word_foldl(W,I,Fun,Acc) when W rem 2=:=1->
  word_foldl(W bsr 1,I+1,Fun,Fun(I,Acc));
word_foldl(W,I,Fun,Acc)->
  word_foldl(W bsr 1,I+1,Fun,Acc).

%%------------------------------------------------------------------------------------
%%  From the less significant to the most significant iterator
%%------------------------------------------------------------------------------------
foldr(_F,Acc,none,_Page)->
  {0,Acc};
foldr(_F,Acc,[],_Page)->
  {0,Acc};
foldr(Fun,InAcc,Bitmap,{From,To})->
  Iterator = get_iterator(From,To,Fun),
  Buckets=split_buckets(Bitmap),
  buckets_foldr( Buckets, Iterator, { _Count=0, InAcc } ).
% Short
buckets_foldr( [ { _N, <<?F:1,0:1,?X:1,_L:?SHORT>> } | Tail ], Fun, Acc )->
  buckets_foldr( Tail, Fun, Acc );
buckets_foldr( [ { N, <<?F:1,1:1,?X:1,L:?SHORT>> } | Tail ], Fun, Acc )->
  From = N * ?WORD_LENGTH*?WORD_LENGTH,
  To = L * ?WORD_LENGTH * ?WORD_LENGTH + From - 1,
  full_foldr( From, To, Fun, buckets_foldr( Tail, Fun, Acc ) );
% Long
buckets_foldr( [ { _N, <<?F:1,0:1,?XX:1,_L:?LONG>> } | Tail ], Fun, Acc )->
  buckets_foldr( Tail, Fun, Acc );
buckets_foldr( [ { N, <<?F:1,1:1,?XX:1,L:?LONG>> } | Tail ], Fun, Acc )->
  To = N * ?WORD_LENGTH*?WORD_LENGTH,
  From = L * ?WORD_LENGTH * ?WORD_LENGTH + To - 1,
  full_foldr( From, To, Fun, buckets_foldr( Tail, Fun, Acc ) );

buckets_foldr([ {N, Bucket} | Tail ], Fun, Acc )->
  Data = decompress(Bucket),
  Offset = N * ?WORD_LENGTH,
  data_foldr( Data, Offset, Fun, buckets_foldr( Tail, Fun, Acc ) );

buckets_foldr([], _Fun, Acc )->
  Acc.

data_foldr([0|Tail],N,Fun,Acc)->
  data_foldr(Tail,N+1,Fun,Acc);
data_foldr([?FULL|Tail], N, Fun, Acc)->
  From = N * ?WORD_LENGTH,
  To = From + ?WORD_LENGTH - 1,
  full_foldr(From, To, Fun, data_foldr(Tail,N+1,Fun,Acc) );

data_foldr([Word|Tail],N,Fun,Acc)->
  Offset = N*?WORD_LENGTH,
  word_foldr(Word,Offset,Fun, data_foldr(Tail,N+1,Fun,Acc));
data_foldr([],_N,_Fun,Acc)->
  Acc.

full_foldr(I,Stop,Fun,Acc) when I=<Stop->
  Fun(I, full_foldr(I+1,Stop,Fun,Acc) );
full_foldr(_I,_Stop,_Fun,Acc)->
  Acc.

word_foldr(0,_I,_Fun,Acc)->
  Acc;
word_foldr(W,I,Fun,Acc) when W rem 2=:=1->
  Fun(I,word_foldr( W bsr 1, I+1, Fun, Acc ));
word_foldr(W,I,Fun,Acc)->
  word_foldr(W bsr 1,I+1,Fun,Acc).

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
split(Bitmap, N, Head) when N=<0->
  { Head, Bitmap };
split(<<>>, N, Head)->
  { <<Head/bitstring,(fill(0,N))/bitstring>>, <<>> };
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

split_buckets(Bitmap)->
  split_buckets(Bitmap,_N=0).
split_buckets(<<>>,_N)->
  [];
split_buckets(<<?F:1,V:1,?X:1,L:?SHORT,Tail/bitstring>>,N)->
  [{N,<<?F:1,V:1,?X:1,L:?SHORT>>}|split_buckets(Tail,N+L)];
split_buckets(<<?F:1,V:1,?XX:1,L:?LONG,Tail/bitstring>>,N)->
  [{N,<<?F:1,V:1,?XX:1,L:?LONG>>}|split_buckets(Tail,N+L)];
split_buckets(Bitmap,N)->
  {Bucket, Tail} = first(Bitmap),
  [{N,Bucket}|split_buckets(Tail,N+1)].

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
      <<?W:1,?X:1,High:?WORD_LENGTH, Data/bitstring >>;
    true ->
      <<?W:1,?XX:1,High:?WORD_LENGTH, Full:?WORD_LENGTH, Data/bitstring >>
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






