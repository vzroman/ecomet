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
  prepare_write/3
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

-define(EMPTY, <<0,0,0,0,0,0,0,0>>).

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
build_index(OID, Changes, Index0) when map_size( Index0 ) =:= 0->

  #acc{
    index = Index,
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


  { Index, Log };

%---------------------Update-----------------------------------
build_index(OID, Changes, Index0)->

  #acc{
    index = Index,
    log = Log
  } = maps:fold(fun(Field, {Value, Index, Storage}, Acc)->

    update_index(#index{
      field = Field,
      type = ?AS_LIST( Index ),
      value = ?AS_LIST( Value ),
      storage = Storage
    }, Acc)

  end, #acc{
    index = Index0,
    log = #{}
  }, Changes ),

  %--------------------Commit index-------------------------------
  DB = ecomet_object:get_db_name( OID ),
  maps:foreach(fun( S, {AddTags, _Other, DelTags} )->

    CommitAdd = prepare_commit( AddTags, OID, true ),
    CommitDel = prepare_commit( DelTags, OID , false ),
    ok = ecomet_db:bulk_write(DB, ?INDEX, S, CommitAdd ++ CommitDel, _Lock = none)

  end, Log ),

  %------------------Add tags from vot changed storages------------
  FullLog =
    maps:fold(fun(S, SFields, SAcc)->
      maps:fold(fun(F, Types, FAcc)->
        case maps:is_key(F,Changes) of
          true ->
            % Skip the field if it's in the Changes because it's log is ready
            FAcc;
          false ->
            maps:fold(fun(T, Values, Acc) ->
              {Add,STags0,Del} = maps:get( S, Acc, {[],[],[]} ),
              Tags = [{F,V,T} || V <- Values],
              STags = Tags ++ STags0,
              Acc#{ S => {Add,STags,Del} }
            end, FAcc, Types )
        end
      end, SAcc, SFields )
    end, Log, Index0 ),

  { Index, FullLog }.


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
  index = IndexAcc0
} = Acc)->

  SLogAcc = maps:get( Storage, LogAcc, {[],[],[]}),
  SIndexAcc0 = maps:get( Storage, IndexAcc0, #{} ),
  Index0 = maps:get( Field, SIndexAcc0, #{} ),

  {Index, Log} =
    lists:foldl(fun(T, FieldAcc)->
      TValues = ordsets:from_list( build_values(T, Values) ),
      update_field_index(TValues, T, Field, FieldAcc )
    end, {Index0, SLogAcc}, Types),

  SIndexAcc =
    if
      map_size( Index ) =:= 0 -> maps:remove( Field, SIndexAcc0 );
      true -> SIndexAcc0#{ Field => Index }
    end,

  IndexAcc =
    if
      map_size( SIndexAcc ) =:= 0 -> maps:remove( Storage, IndexAcc0 );
      true -> IndexAcc0#{ Storage => SIndexAcc }
    end,

  Acc#acc{
    index = IndexAcc,
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
        { Tags ++ LogAddAcc, LogOtherAcc,  LogDelAcc}
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

  [{{Tag,[idl,PatternID,IDHN,IDLN]}, Value} || Tag <- Tags].

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
prepare_write(Module, Ref, Updates)->
  ByTags = group_by_tags( Updates ),
  maps:fold(fun(Tag, Patterns, Acc)->
    update_tag(Module, Ref, Tag, Patterns, Acc)
  end, {[],[]} , ByTags).

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
  lists:foldl(fun({{Tag,[idl,PatternID,IDH,IDL]}, Value},Acc)->
      TAcc = maps:get(Tag,Acc,#{}),
      PAcc = maps:get(PatternID,TAcc,#{}),
      IDHAcc = maps:get(IDH,PAcc,#{}),
      Acc#{ Tag => TAcc#{ PatternID=> PAcc#{ IDH =>IDHAcc#{ IDL => Value }}}}
  end, #{} ,Updates).

update_tag( Module, Ref, Tag, Patterns, LogAcc )->
  {Ps, LogAcc1} =
    %----------pattern loop------------------------
    maps:fold(fun(P, IDHs, {PAcc, PAccLog})->

      %----------IDHs loop------------------------
      {Hs, PAccLog1} = maps:fold(fun(H, Ls, {HAcc, HAccLog})->
        % Update IDL bitmap
        case build_bitmap(Module, Ref, {Tag,[idl,P,H]}, Ls, HAccLog) of
          {stop, HAccLog1}->  % The bitmap was just updated, no need to update IDH bitmap
            { HAcc, HAccLog1 };
          {Value, HAccLog1} ->  % The bitmap was either created or deleted, we have to update IDH bitmap
            {HAcc#{ H => Value }, HAccLog1 }
        end
      end,{#{}, PAccLog}, IDHs),

      % Update IDH bitmap that need to be modified
      case build_bitmap(Module, Ref, {Tag,[idh,P]}, Hs, PAccLog1) of
        {stop, PAccLog2}->  % The bitmap was just updated, no need to update pattern bitmap
          {PAcc, PAccLog2};
        {Value, PAccLog2} -> % The bitmap was either created or deleted, we have to update pattern bitmap
          { PAcc#{ P => Value }, PAccLog2}
      end

    end,{#{}, LogAcc }, Patterns),

  % Update pattern bitmap that need to be modified
  {_, LogAcc2} = build_bitmap(Module, Ref, {Tag,patterns}, Ps, LogAcc1),
  LogAcc2.

build_bitmap(_Module, _Ref, _Tag, Update, Acc) when map_size(Update) =:= 0->
  {stop, Acc};
build_bitmap(Module, Ref, Tag, Update, { LogAddAcc, LogDelAcc })->
  {Add, Del}=
    maps:fold(fun(ID, BitValue, {AddAcc, DelAcc})->
      if
        BitValue -> { ecomet_bitmap:set_bit(AddAcc,ID), DelAcc };
        true-> { AddAcc, ecomet_bitmap:set_bit(DelAcc,ID) }
      end
    end,{?EMPTY,?EMPTY}, Update),

  Value0 =
    case Module:read(Ref,[{?INDEX,[ Tag ]}]) of
      []-> ?EMPTY;
      [{_,V}]-> V
    end,

  Value = udpate_value( Add, Del, Value0 ),

  case {Value, Value0} of
    { ?EMPTY, ?EMPTY } ->
      { stop, { LogAddAcc, LogDelAcc } };
    { ?EMPTY, _Value0 }->
      { false, { LogAddAcc, [ {?INDEX,[Tag]} | LogDelAcc ] }};
    { Value, ?EMPTY } ->
      { true, { [ {{?INDEX,[Tag]}, Value} |  LogAddAcc ], LogDelAcc } };
    { Value, _Value0 }->
      { stop, { [ {{?INDEX,[Tag]}, Value} |  LogAddAcc ], LogDelAcc } }
  end.

udpate_value( Add, Del, Value )->
  if
    Value =:= ?EMPTY, Del =:= ?EMPTY -> Add;
    Value =:= ?EMPTY, Add =:= ?EMPTY -> ?EMPTY;
    Value =:= ?EMPTY->
      ecomet_bitmap:bit_andnot(Add, Del);
    Del =:= ?EMPTY ->
      ecomet_bitmap:bit_or( Value, Add );
    Add =:= ?EMPTY->
      ecomet_bitmap:bit_andnot(Value, Del);
    true ->
      Value1 = ecomet_bitmap:bit_or( Value, Add ),
      ecomet_bitmap:bit_andnot(Value1, Del)
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