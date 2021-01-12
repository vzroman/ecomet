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

-record(bitmap,{h,l}).

-define(WORD_LENGTH,64).

-define(EMPTY,0).
-define(SPARSE,1).
-define(SPARSE_FULL,2).
-define(FULL,3).
-define(FULL_WORD,2#1111111111111111111111111111111111111111111111111111111111111111).
-define(BIT(X), 1 bsl X).

%% API
-export([
  set_bit/2,
  reset_bit/2,
  get_bit/2,
  is_empty/1,
  count/1,count/2,

  oper/3,
  bit_and/2,
  bit_or/2,
  bit_andnot/2
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

  Bucket = << ?SPARSE:2, (?BIT(HighBit)):?WORD_LENGTH, (?BIT(LowBit)):?WORD_LENGTH>>,
  Prefix = << <<?EMPTY:2>> || _<-lists:seq(1,BucketNum) >>,

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

  Bucket = << ?SPARSE:2, (?BIT(HighBit)):?WORD_LENGTH, (?BIT(LowBit)):?WORD_LENGTH>>,
  Prefix = << <<?EMPTY:2>> || _<-lists:seq(1,BucketNum) >>,

  bit_andnot(Bitmap,<<Prefix/bitstring,Bucket/bitstring>>).

get_bit(Bit,Bitmap)->
  case is_empty(Bitmap) of
    true->false;
    _->
      BucketNum = Bit div ?WORD_LENGTH div ?WORD_LENGTH,
      case find_bucket(BucketNum,Bitmap) of
        <<?EMPTY:2>>->
          false;
        <<?FULL:2>>->
          true;
        <<?SPARSE_FULL:2,Head:?WORD_LENGTH,Full:?WORD_LENGTH,Data/bitstring>>->
          HighBit = Bit div ?WORD_LENGTH rem ?WORD_LENGTH,
          if
            ?BIT(HighBit) band Head =:= 0 -> false ;
            true ->
              if
                ?BIT(HighBit) band Full >0 -> true ;
                true ->
                  LowBit = Bit rem ?WORD_LENGTH,
                  Word = find_word( LowBit, Head, Full, Data ),
                  ?BIT(LowBit) band Word>0
              end
          end;
        <<?SPARSE:2,Head:?WORD_LENGTH,Data/bitstring>>->
          HighBit = Bit div ?WORD_LENGTH rem ?WORD_LENGTH,
          if
            ?BIT(HighBit) band Head =:= 0 -> false ;
            true ->
              LowBit = Bit rem ?WORD_LENGTH,
              Word = find_word( LowBit, Head, 0, Data ),
              ?BIT(LowBit) band Word>0
          end
      end
  end.

is_empty(none)->
  true;
is_empty(<<>>)->
  true;
is_empty(<<?EMPTY:2,Tail/bitstring>>)->
  is_empty(Tail);
is_empty(_Other)->
  false.

count(Bitmap)->
  count(Bitmap,0).
count(none,Acc)->
  Acc;
count(<<>>,Acc)->
  Acc;
count(Bitmap, Acc)->
  { Bucket, Tail } = get_bucket(Bitmap),
  count(Tail, Acc + bucket_count(Bucket) ).

bucket_count(<<?EMPTY:2>>)->
  0;
bucket_count(<<?FULL:2>>)->
  ?WORD_LENGTH * ?WORD_LENGTH;
bucket_count(<<?SPARSE:2,Head:?WORD_LENGTH,Data/bitstring>>)->
  data_count(Head,_Full=0,Data,0);
bucket_count(<<?SPARSE_FULL:2,Head:?WORD_LENGTH,Full:?WORD_LENGTH,Data/bitstring>>)->
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
bit_and(Bitmap1,Bitmap2)->
  { Bucket1, Tail1 } = get_bucket(Bitmap1),
  { Bucket2, Tail2 } = get_bucket(Bitmap2),
  << (bucket_and(Bucket1,Bucket2))/bitstring, (bit_and( Tail1, Tail2 ))/bitstring >>.

bucket_and(<<?EMPTY:2>>,_Bucket2)->
  <<?EMPTY:2>>;
bucket_and(_Bucket1,<<?EMPTY:2>>)->
  <<?EMPTY:2>>;
bucket_and(<<?FULL:2>>,Bucket2)->
  Bucket2;
bucket_and(Bucket1,<<?FULL:2>>)->
  Bucket1;
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
bit_or( Bitmap1, Bitmap2 )->
  { Bucket1, Tail1 } = get_bucket(Bitmap1),
  { Bucket2, Tail2 } = get_bucket(Bitmap2),
  << (bucket_or(Bucket1,Bucket2))/bitstring, (bit_or( Tail1, Tail2 ))/bitstring >>.
bucket_or(<<?FULL:2>>,_Bucket2)->
  <<?FULL:2>>;
bucket_or(_Bucket1,<<?FULL:2>>)->
  <<?FULL:2>>;
bucket_or(<<?EMPTY:2>>,Bucket2)->
  Bucket2;
bucket_or(Bucket1,<<?EMPTY:2>>)->
  Bucket1;
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
bit_andnot( Bitmap1, Bitmap2 )->
  { Bucket1, Tail1 } = get_bucket(Bitmap1),
  { Bucket2, Tail2 } = get_bucket(Bitmap2),
  << (bucket_andnot(Bucket1,Bucket2))/bitstring, (bit_andnot( Tail1, Tail2 ))/bitstring >>.
bucket_andnot(<<?EMPTY:2>>,_Bucket2)->
  <<?EMPTY:2>>;
bucket_andnot(Bucket1,<<?EMPTY:2>>)->
  Bucket1;
bucket_andnot(_Bucket1,<<?FULL:2>>)->
  <<?EMPTY:2>>;
bucket_andnot(<<?FULL:2>>,Bucket)->
  Data1 = [?FULL_WORD || _<- lists:seq(1,?WORD_LENGTH)],
  Data2 = decompress(Bucket),
  Data = data_andnot(Data1, Data2 ),
  compress(Data);
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
%%  Bucket utilities
%%------------------------------------------------------------------------------------
find_bucket(N,<<>>)->
  <<?EMPTY:2>>;
find_bucket(N,Bitmap)->
  { Bucket, Tail } = get_bucket(Bitmap),
  if
    N=:=0 -> Bucket;
    true ->
      find_bucket(N-1,Tail)
  end.

get_bucket(<<?EMPTY:2,Tail/bitstring>>)->
  { <<?EMPTY:2>>, Tail };
get_bucket(<<?FULL:2,Tail/bitstring>>)->
  { <<?FULL:2>>, Tail };
get_bucket(<<?SPARSE:2,HWord:?WORD_LENGTH,Rest/bitstring>>)->
  Length = bit_count(HWord) * ?WORD_LENGTH,
  <<Data:Length,Tail/bitstring>> = Rest,
  {<<?SPARSE:2,HWord:?WORD_LENGTH,Data:Length>>, Tail };
get_bucket(<<?SPARSE_FULL:2,HWord:?WORD_LENGTH,FullWords:?WORD_LENGTH,Rest/bitstring>>)->
  Length = (bit_count(HWord) - bit_count(FullWords)) * ?WORD_LENGTH,
  <<Data:Length,Tail/bitstring>> = Rest,
  {<<?SPARSE_FULL:2,HWord:?WORD_LENGTH,FullWords:?WORD_LENGTH,Data:Length>>, Tail }.

decompress(<<?SPARSE:2,High:?WORD_LENGTH,Data/bitstring>>)->
  decompress(High,0,Data);
decompress(<<?SPARSE_FULL:2,High:?WORD_LENGTH,Full:?WORD_LENGTH,Data/bitstring>>)->
  decompress(High,Full,Data).
decompress(0,_F,_Data)->
  [];
decompress(H,F,Data) when F rem 2 =:= 1->
  [?FULL_WORD|decompress(H bsr 1,F bsr 1,Data)];
decompress(H,F,Data) when H rem 2=:=0->
  [0|decompress(H bsr 1,F bsr 1,Data)];
decompress(H,F,<<Word:?WORD_LENGTH,Tail/bitstring>>)->
  [Word|decompress(H bsr 1,F bsr 1,Tail)].

compress(Data)->
  compress(Data, _High=0, _Full=0, _Bit=1, <<>>).
compress([0|Tail],High,Full,Bit,Acc)->
  compress(Tail,High,Full,Bit bsl 1,Acc);
compress([?FULL_WORD|Tail],High,Full,Bit,Acc)->
  compress(Tail,High,Full bor Bit, Bit bsl 1 ,Acc);
compress([Word|Tail],High,Full,Bit,Acc)->
  compress(Tail, High bor Bit, Full, Bit bsl 1, <<Acc/binary,Word:?WORD_LENGTH>> );
compress([],High,Full,_Bit,Data)->
  if
    High =:=0 ->
      <<?EMPTY:2>>;
    Full =:= ?FULL_WORD->
      <<?FULL:2>>;
    Full =:=0->
      <<?SPARSE:2, High:?WORD_LENGTH, Data/bitstring >>;
    true ->
      <<?SPARSE_FULL:2, High:?WORD_LENGTH, Full:?WORD_LENGTH, Data/bitstring >>
  end.

find_word(0,_H, _F, <<Word:?WORD_LENGTH,_/bitstring>>)->
  Word;
find_word(N,H,F,Data) when F rem 2=:=1->
  find_word(N-1,H bsr 1,F bsr 1,Data);
find_word(N,H,F,Data) when H rem 2=:=0->
  find_word(N-1,H bsr 1,F bsr 1,Data);
find_word(N,H,F,<<_:?WORD_LENGTH,Tail/bitstring>>)->
  find_word(N-1,H bsr 1,F bsr 1,Tail).


bit_count(Value)->
  bit_count(Value,0).
bit_count(0,Acc)->
  Acc;
bit_count(Value,Acc)->
  Bit = Value rem 2,
  bit_count(Value bsr 1,Acc+Bit).










