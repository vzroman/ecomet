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
  build_index/3,
  destroy_index/2,
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

-define(AS_LIST( V ), if is_list(V) -> V; true -> [V] end).

%% ====================================================================
%% Indexing
%% ====================================================================
%------------------PHASE 1---------------------------------------------
%               Prepare object tags and commit
%----------------------------------------------------------------------
-record(index,{ field, type, storage, value }).
-record(acc,{ index, log }).
% indexes:
% #{
%   Storage => #{
%     Field => #{
%       Type => [ ...values ]
%     }
%   }
% }
% log:
% #{
%   Storage => { [ ...AddTags ], [...OtherTags] ,[ ...DelTags ] }
% }
%---------------------Create-----------------------------------
build_index(OID, Changes, Indexes) when map_size( Indexes ) =:= 0->

  #acc{
    index = Indexes,
    log = Log
  } = maps:fold(fun(Field, {Value, Index, Storage}, Acc)->

    create_index(#index{
      field = Field,
      type = ?AS_LIST( Index ),
      value = ?AS_LIST( Value ),
      storage = Storage
    }, Acc)
  end, #acc{
    index = #{},
    log = #{}
  }, Changes ),

  %--------------------Commit index-------------------------------
  DB = ecomet_object:get_db_name( OID ),
  maps:foreach(fun( S, Tags )->

    Commit = prepare_commit( Tags, OID , _Value = true ),
    ok = ecomet_db:bulk_write(DB, ?INDEX, S, Commit, _Lock = none)

  end, Log ),


  { Indexes, Log };

%---------------------Update-----------------------------------
build_index(OID, Changes, Indexes)->

  #acc{
    index = Indexes,
    log = Log
  } = maps:fold(fun(Field, {Value, Index, Storage}, Acc)->

    update_index(Acc#index{
      field = Field,
      type = ?AS_LIST( Index ),
      value = ?AS_LIST( Value ),
      storage = Storage
    }, Acc)

  end, #acc{
    index = Indexes,
    log = #{}
  }, Changes ),

  %--------------------Commit index-------------------------------
  DB = ecomet_object:get_db_name( OID ),
  maps:foreach(fun( S, {AddTags, _Other, DelTags} )->

    CommitAdd = prepare_commit( AddTags, OID, true ),
    CommitDel = prepare_commit( DelTags, OID , false ),
    ok = ecomet_db:bulk_write(DB, ?INDEX, S, CommitAdd ++ CommitDel, _Lock = none)

  end, Log ),

  { Indexes, Log }.


create_index(#index{
  field = Field,
  type = Types,
  value = Values,
  storage = Storage
}, #acc{
  log = LogAcc,
  index = IndexAcc
} = Acc)->

  SLogAcc = maps:get( Storage, LogAcc, []),
  SIndexAcc = maps:get( Storage, IndexAcc, #{} ),

  {Index, Log} =
    lists:foldl(fun(T, {TAcc, LAcc})->
      case build_values(T, Values) of
        [] -> { TAcc, LAcc };
        TValues0 ->
          TValues = ordsets:from_list( TValues0 ),
          Tags = [{Field,V,T} || V <- TValues],
          { TAcc#{ T => TValues }, Tags ++ LAcc }
      end
    end, {#{}, SLogAcc}, Types),

  Acc#acc{
    index = IndexAcc#{ Storage => SIndexAcc#{ Field => Index } },
    log = LogAcc#{ Storage => Log }
  }.

update_index(#index{
  field = Field,
  type = Types,
  value = Values,
  storage = Storage
}, #acc{
  log = LogAcc,
  index = IndexAcc
} = Acc)->

  SLogAcc = maps:get( Storage, LogAcc, {[],[],[]}),
  SIndexAcc = maps:get( Storage, IndexAcc, #{} ),
  Index0 = maps:get( Field, SIndexAcc, #{} ),

  {Index, Log} =
    lists:foldl(fun(T, FieldAcc)->
      TValues = ordsets:from_list( build_values(T, Values) ),
      update_field_index(TValues, T, Field, FieldAcc )
    end, {Index0, SLogAcc}, Types),

  Acc#acc{
    index = IndexAcc#{ Storage => SIndexAcc#{ Field => Index } },
    log = LogAcc#{ Storage => Log }
  }.

update_field_index([], Type, Field, {IndexAcc, {LogAddAcc, LogOtherAcc, LogDelAcc}} )->
  DelAcc1 =
    case IndexAcc of
      #{ Type := Values } -> [{Field,V,Type} || V <- Values] ++ LogDelAcc;
      _-> LogDelAcc
    end,
  { maps:remove( Type, IndexAcc ), {LogAddAcc, LogOtherAcc, DelAcc1} };

update_field_index( Values, Type, Field , {IndexAcc, {LogAddAcc, LogOtherAcc, LogDelAcc}} )->

  Tags = [{Field,V,Type} || V <- Values],

  LogAcc =
    case IndexAcc of
      #{ Type := Values0 } ->
        Tags0 = [{Field,V,Type} || V <- Values0],
        LogAdd = ordsets:subtract( Tags, Tags0 ),
        LogDel = ordsets:subtract( Tags0, Tags ),
        LogOther = ordsets:subtract( Tags0, LogDel ),
        { LogAdd ++ LogAddAcc, LogOther ++ LogOtherAcc, LogDel ++ LogDelAcc };
      _->
        { Tags ++ LogAddAcc,   LogDelAcc}
    end,
  { IndexAcc#{ Type => Values }, LogAcc }.

destroy_index(OID, Index)->
  DB = ecomet_object:get_db_name( OID ),
  maps:foreach(fun( S, Fields )->
    Tags =
      maps:fold(fun(F, Types, Acc)->
        maps:fold(fun( T, Values, FAcc ) ->
          [{F,V,T} || V <- Values] ++ FAcc
        end, Acc, Types )
      end, [], Fields ),

    Commit = prepare_commit( Tags, OID , false ),
    ok = ecomet_db:bulk_write(DB, ?INDEX, S,  Commit, _Lock = none)

  end, Index ).

prepare_commit(Tags, OID ,Value)->

  PatternID=ecomet_object:get_pattern(OID),
  ObjectID=ecomet_object:get_id(OID),
  IDHN=ObjectID div ?BITSTRING_LENGTH,
  IDLN=ObjectID rem ?BITSTRING_LENGTH,

  lists:append([[
    {{Tag,patterns}, false},
    {{Tag,[idh,PatternID]}, false},
    {{Tag,[idl,PatternID,IDHN]}, false},
    {{Tag,[idl,PatternID,IDHN,IDLN]}, Value}
  ] || Tag <- Tags]).

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
    ({{Tag,[idl,PatternID,IDH,IDL]}, Value},Acc)->
      TAcc = maps:get(Tag,Acc,#{}),
      PAcc = maps:get(PatternID,TAcc,#{}),
      IDHAcc = maps:get(IDH,PAcc,#{}),
      Acc#{ Tag => TAcc#{ PatternID=> PAcc#{ IDH =>IDHAcc#{ IDL => Value }}}};
    (_,Acc)->
      Acc
  end, #{} ,Updates).

update_tag( Module, Ref, Tag, Patterns )->
  Ps =
    maps:fold(fun(P,IDHs,PAcc)->
      Hs = maps:fold(fun(H,Ls,HAcc)->
        case build_bitmap(Module, Ref, {Tag,[idl,P,H]},Ls) of
          stop-> HAcc;
          Value -> HAcc#{ H => Value }
        end
      end,#{},IDHs),
      case build_bitmap(Module, Ref, {Tag,[idh,P]}, Hs) of
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
      [PatternID,IDH]->{Tag,[idl,PatternID,IDH]};
      [PatternID]->{Tag,[idh,PatternID]};
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