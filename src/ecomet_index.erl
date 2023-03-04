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
-module(ecomet_index).

-include("ecomet.hrl").

%% ====================================================================
%% Indexing API
%% ====================================================================
-export([
  build_index/2,
  commit/1,
  write/3
]).

%% ====================================================================
%% Search API
%% ====================================================================
-export([
  read_tag/4
]).

%% ====================================================================
%% Utilities API
%% ====================================================================
-export([
  get_supported_types/0,

  build_3gram/2,
  build_dt/2
]).
%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([

]).
-endif.

%% ====================================================================
%% Indexing
%% ====================================================================
%------------------PHASE 1---------------------------------------------
%               Prepare object tags
%----------------------------------------------------------------------
build_index(Changes, Map)->
  maps:fold(fun(Field,{PrevValue, NewValue},Acc)->
    PrevTags =
      if
        PrevValue =/= none->
          field_tags( Field, Map, PrevValue );
        true->
          []
      end,
    NewTags =
      if
        NewValue =/= none->
          field_tags( Field, Map, NewValue );
        true->
          []
      end,
    case { NewTags -- PrevTags, PrevTags -- NewTags } of
      {[],[]}-> Acc;
      {AddTags,DelTags}->
        {ok,Type} = ecomet_field:get_storage(Map, Field),
        {TypeAddAcc, TypeDelAcc} = maps:get(Type, Acc, {[],[]}),
        Acc#{ Type => { AddTags ++ TypeAddAcc, DelTags ++ TypeDelAcc } }
    end
  end, #{}, Changes).

field_tags( Field, Map, Value )->
  case ecomet_field:get_index(Map, Field) of
    {ok, none}->
      [];
    {ok, IndexTypes}->
      ListValue =
        case ecomet_field:get_type(Map, Field) of
          {ok,{list,_}}-> Value;
          _-> [Value]
        end,
      lists:usort(lists:append(lists:foldl(fun(Type,Acc)->
        [[{Field,TagValue,Type}||TagValue<-build_values(Type,ListValue)]|Acc]
      end,[],IndexTypes)))
  end.

%----------------------------------------
%   Indexing a value
%----------------------------------------
build_values(simple,Values)->
  % Simple index by exact value
  [Value||Value<-Values,Value=/=none];
build_values('3gram',Values)->
  % ngram index for string values. Fuzzy search
  build_3gram(Values,[]);
build_values(datetime,Values)->
  % Datetime index for integer values. Trapezoid search
  build_dt(Values,[]).

%------ngram index----------------------
build_3gram([none|Rest],Acc)->
  build_3gram(Rest,Acc);
build_3gram([Value|Rest],Acc)->
  build_3gram(Rest,[split_3grams(Value)|Acc]);
build_3gram([],Acc)->
  lists:append(Acc).

% split string to 3grams
split_3grams(Value)->
  Value1=unicode:characters_to_list(<<"^",Value/binary,"$">>),
  split_3grams(Value1,[]).
split_3grams([C1,C2,C3|Rest],NgramList)->
  split_3grams([C2,C3|Rest],[unicode:characters_to_binary(string:uppercase([C1,C2,C3]))|NgramList]);
split_3grams([_C1,_C2],NgramList)->
  if
    length(NgramList)>2->NgramList;
    true->[]
  end.

%------datetime index----------------------
build_dt([none|Rest],Acc)->
  build_dt(Rest,Acc);
build_dt([Value|Rest],Acc)->
  Value1=
    try binary_to_integer(Value)
    catch _:_->ecomet_lib:parse_dt(Value)
    end,
  Value2=ecomet_lib:dt_to_string(Value1),
  build_dt(Rest,[dt_index(Value2)|Acc]);
build_dt([],Acc)->
  lists:append(Acc).

% Split the datetime into components
dt_index(DT)->
  <<
    YYYY:4/binary,"-",
    MM:2/binary,"-",
    DD:2/binary,"T",
    HH:2/binary,":",
    Min:2/binary,":",
    SS:2/binary,_/binary
  >>=DT,
  [{I,binary_to_integer(V)}||{I,V}<-[{y,YYYY},{m,MM},{d,DD},{h,HH},{mi,Min},{s,SS}]].


%------------------PHASE 2---------------------------------------------
%               Prepare changes for each tag
%----------------------------------------------------------------------
commit(LogList)->
  {ByDBsTypes, LogList1 } = group_by_dbs_types_tags( LogList ),
  maps:fold(fun(DB,Types,_)->
    maps:fold(fun(Type,Tags,_)->
      maps:fold(fun(Tag,OIDs,_)->
        Commit = prepare_commit(Tag, OIDs),
        Rollback = prepare_rollback(Commit),
        ok = ecomet_db:bulk_write(DB,?INDEX,Type,Commit,_Lock = none),
        ok = ecomet_db:bulk_on_abort(DB,?INDEX,Type,Rollback)
      end, undefined, Tags)
    end, undefined, Types)
  end, undefined, ByDBsTypes),
  lists:reverse(LogList1).


group_by_dbs_types_tags( LogList )->
  % #{
  %   DB => #{
  %     Type => #{
  %       Tag => #{
  %         OID => true | false
  %       }
  %     }
  %   }
  % }
  lists:foldl(fun(#{db := DB, object:=#{ <<".oid">>:=OID }, index_log := IndexLog}=Log, {Acc,LogAcc})->
    DBAcc0 = maps:get(DB, Acc, #{}),
    DBAcc = maps:fold(fun(Type,{Add, Del}, InDBAcc ) ->
        TypeAcc0 = maps:get(Type, InDBAcc, #{}),
        TypeAcc1 = lists:foldl(fun(Tag,InTagAcc) ->
          TagAcc = maps:get(Tag, InTagAcc, #{}),
          InTagAcc#{Tag => TagAcc#{OID => true}}
        end, TypeAcc0, Add),
        TypeAcc = lists:foldl(fun(Tag,InTagAcc) ->
          TagAcc = maps:get(Tag, InTagAcc, #{}),
          InTagAcc#{Tag => TagAcc#{OID => false}}
         end, TypeAcc1, Del),
        InDBAcc#{Type => TypeAcc }
        end, DBAcc0,IndexLog),
      {Acc#{ DB => DBAcc }, [maps:remove(index_log, Log)|LogAcc]}
  end,{#{},[]}, LogList).

prepare_commit(Tag, OIDs)->
  maps:to_list(maps:fold(fun(OID,Value,Acc)->
    PatternID=ecomet_object:get_pattern(OID),
    ObjectID=ecomet_object:get_id(OID),
    IDHN=ObjectID div ?BITSTRING_LENGTH,
    IDLN=ObjectID rem ?BITSTRING_LENGTH,
    IDLAcc =  maps:get({Tag,{idl,PatternID,IDHN}},Acc,#{}),
    Acc#{
      {Tag,patterns} =>#{},
      {Tag,{idh,PatternID}} => #{},
      {Tag,{idl,PatternID,IDHN}} => IDLAcc#{ IDLN => Value }
    }
  end,#{},OIDs)).

prepare_rollback([{Tag, Commit}| Rest])->
  [{Tag, maps:map(fun(_ID,Value)-> not Value end, Commit )} | prepare_rollback(Rest)];
prepare_rollback([])->
  [].

%------------------PHASE 3---------------------------------------------
%               Write changes to the real database
%----------------------------------------------------------------------
write(Module, Ref, Updates)->
  ByTags = group_by_tags( Updates ),
  maps:fold(fun(Tag,Patterns,_)->
    {ok, Unlock} = elock:lock(?INDEX_LOCKS, {Ref,Tag}, _IsShared = false, _Timeout=infinity,[node()]),
    try update_tag(Module, Ref, Tag, Patterns)
    after
      Unlock()
    end,
    ignore
  end,ignore,ByTags),
  ok.

group_by_tags( Updates )->
  % #{
  %   Tag => #{
  %     Pattern => #{
  %       IDH => #{
  %         IDL => true | false
  %       }
  %     }
  %   }
  % }
  lists:foldl(fun
    ({{Tag,{idl,PatternID,IDH}}, IDLs},Acc)->
      TAcc = maps:get(Tag,Acc,#{}),
      PAcc = maps:get(PatternID,TAcc,#{}),
      Acc#{ Tag => TAcc#{ PatternID=> PAcc#{ IDH =>IDLs } } };
    (_,Acc)->
      Acc
  end, #{} ,Updates).

update_tag( Module, Ref, Tag, Patterns )->
  Ps =
    maps:fold(fun(P,IDHs,PAcc)->
      Hs = maps:fold(fun(H,Ls,HAcc)->
        case build_bitmap(Module, Ref, {Tag,{idl,P,H}},Ls) of
          stop-> HAcc;
          Value -> HAcc#{ H => Value }
        end
      end,#{},IDHs),
      case build_bitmap(Module, Ref, {Tag,{idh,P}}, Hs) of
        stop-> PAcc;
        Value -> PAcc#{ P => Value }
      end
    end,#{}, Patterns),
  build_bitmap(Module, Ref, {Tag,patterns}, Ps).

build_bitmap(_Module, _Ref, _Tag, Update) when map_size(Update) =:= 0->
  stop;
build_bitmap(Module, Ref, Tag, Update)->
  Empty = ecomet_bitmap:create(),
  {Add, Del}=
    maps:fold(fun(ID,Value,{AddAcc,DelAcc})->
      if
        Value -> { ecomet_bitmap:set_bit(AddAcc,ID), DelAcc };
        true-> { AddAcc, ecomet_bitmap:set_bit(DelAcc,ID) }
      end
    end,{Empty,Empty}, Update),

  case Module:read(Ref,[{?INDEX,[Tag]}]) of
    []->
      case ecomet_bitmap:bit_andnot(Add,Del) of
        Empty->
          stop;
        LevelValue->
          ok = Module:write( Ref, [{{?INDEX,[Tag]}, LevelValue}]),
          true
      end;
    [{_,LevelValue0}]->
      LevelValue1 = ecomet_bitmap:bit_or(LevelValue0, Add ),
      case ecomet_bitmap:bit_andnot(LevelValue1,Del) of
        Empty->
          ok = Module:delete( Ref, [{?INDEX,[Tag]}]),
          false;
        LevelValue->
          ok = Module:write( Ref, [{{?INDEX,[Tag]}, LevelValue}]),
          stop
      end
  end.


%%==============================================================================================
%%	Search
%%==============================================================================================
%% Searching by tag
read_tag(DB,Storage,Vector,Tag)->
  Key=
    case Vector of
      [PatternID,IDH]->{Tag,{idl,PatternID,IDH}};
      [PatternID]->{Tag,{idh,PatternID}};
      []->{Tag,patterns}
    end,
  case ecomet_db:read(DB,?INDEX,Storage,Key) of
    not_found->none;
    IndexValue-> IndexValue
  end.

get_supported_types()->
  [
    simple,
    '3gram',
    datetime
  ].