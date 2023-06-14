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

%%------------------------------------------------------------------------------------
%%  API
%%------------------------------------------------------------------------------------
-export([
  create/0,
  is_empty/1,
  count/1
]).
%%------------------------------------------------------------------------------------
%%  BITS API
%%------------------------------------------------------------------------------------
-export([
  set_bit/2,
  reset_bit/2,
  get_bit/2
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
  foldl/4,
  foldr/4
]).

%%------------------------------------------------------------------------------------
%%  API
%%------------------------------------------------------------------------------------
create()->
  eroaring:create().

is_empty(none)->
  true;
is_empty( Set )->
  eroaring:is_empty( Set ).

count(none)->
  0;
count( Set )->
  eroaring:count( Set ).

%%------------------------------------------------------------------------------------
%%  SET
%%------------------------------------------------------------------------------------
set_bit(none, Bits) when is_list( Bits )->
  eroaring:from_list( Bits );
set_bit(none, Bit)->
  eroaring:from_list( [Bit] );
set_bit( Set, Bits ) when is_list( Bits )->
  eroaring:add_elements( Set, Bits );
set_bit( Set, Bit )->
  eroaring:add_elements( Set, [Bit] ).

%%------------------------------------------------------------------------------------
%%  RESET
%%------------------------------------------------------------------------------------
reset_bit(none, _Bit)->
  eroaring:create();
reset_bit(Set, Bits) when is_list( Bits )->
  eroaring:remove_elements( Set, Bits );
reset_bit(Set, Bit)->
  eroaring:remove_elements( Set, [Bit] ).

get_bit(none,_Bit)->
  false;
get_bit(Set,Bit)->
  eroaring:contains(Set, Bit).

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
bit_and(Set1, Set2)->
  eroaring:intersection( Set1, Set2 ).

%%------------------------------------------------------------------------------------
%%  OR
%%------------------------------------------------------------------------------------
bit_or( Set1, Set2 )->
  eroaring:union( Set1, Set2 ).

%%------------------------------------------------------------------------------------
%%  ANDNOT
%%------------------------------------------------------------------------------------
bit_andnot( Set1, Set2 )->
  eroaring:subtract( Set1, Set2 ).


%%------------------------------------------------------------------------------------
%%  From the less significant to the most significant iterator
%%------------------------------------------------------------------------------------
foldl(_Fun,Acc,none,_Page)->
  {0,Acc};
foldl(_Fun,Acc,[],_Page)->
  {0,Acc};
foldl(Fun,InAcc,Set,{From,To})->
  Bits= eroaring:to_list( Set ),
  lists:foldl(fun(Bit,{Count,Acc})->
    Count1 = Count+1,
    Acc1=
      if
        is_integer(From), Count1 =< From->
          Acc;
        is_integer(To), Count1 > To->
          Acc;
        true ->
          Fun(Bit, Acc)
      end,
    {Count1, Acc1}
  end, { _Count=0, InAcc }, Bits).

%%------------------------------------------------------------------------------------
%%  From the less significant to the most significant iterator
%%------------------------------------------------------------------------------------
foldr(_F,Acc,none,_Page)->
  {0,Acc};
foldr(_F,Acc,[],_Page)->
  {0,Acc};
foldr(Fun,InAcc,Set,{From,To})->
  Bits= eroaring:to_list( Set ),
  lists:foldr(fun(Bit,{Count,Acc})->
    Count1 = Count+1,
    Acc1=
      if
        is_integer(From), Count1 =< From->
          Acc;
        is_integer(To), Count1 > To->
          Acc;
        true ->
          Fun(Bit, Acc)
      end,
    {Count1, Acc1}
  end, { _Count=0, InAcc }, Bits).







