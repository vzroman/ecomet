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
%% Service API
%% ====================================================================
-export([
  start_link/0
]).

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

-define(EMPTY, persistent_term:get({ ?MODULE, empty })).
-define(CACHE_DUR, 10000).  % 10 sec
-define(CACHE_WAIT, 10000). % 10 sec
-define(CACHE_MAX_SIZE, 10000).

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

  { Index, Log }.


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
-record(update_index,{ from, module, ref, tag, add, del }).
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

  Empty = ?EMPTY,
  {Add, Del}=
    maps:fold(fun(ID,Value,{AddAcc, DelAcc})->
      if
        Value -> { ecomet_bitmap:set_bit(AddAcc,ID), DelAcc };
        true-> { AddAcc, ecomet_bitmap:set_bit(DelAcc,ID) }
      end
    end,{Empty,Empty}, Update),

  case update_index(#update_index{
    from = self(),
    module = Module,
    ref = Ref,
    tag = {?INDEX,[Tag]},
    add = Add,
    del = Del
  }) of
    { Empty, Empty } ->
      { stop, { LogAddAcc, LogDelAcc } };
    { Empty, _Value0 }->
      { false, { LogAddAcc, [ {?INDEX,[Tag]} | LogDelAcc ] }};
    { Value, Empty } ->
      { true, { [ {{?INDEX,[Tag]}, Value} |  LogAddAcc ], LogDelAcc } };
    { Value, _Value0 }->
      { stop, { [ {{?INDEX,[Tag]}, Value} |  LogAddAcc ], LogDelAcc } }
  end.

update_index( #update_index{ tag = Tag } = Index )->
  ?MODULE ! Index,
  receive
    { tag, Tag, Value, Value0 }-> { Value, Value0 }
  end.

-record(cache,{ tags, dur, wait, empty }).
start_link()->
  {ok, spawn_link(fun cache_init/0)}.

cache_init()->

  register( ?MODULE, self() ),
  Empty = ecomet_bitmap:create(),
  persistent_term:put( {?MODULE, empty}, Empty ),

  cache_loop( #cache{
    dur = ?CACHE_DUR,
    wait = ?CACHE_WAIT,
    tags = #{},
    empty = Empty
  }).

cache_loop( #cache{ tags = Tags } = State ) when map_size( Tags ) > ?CACHE_MAX_SIZE ->
  State1 = decrease_cache( State ),
  cache_loop( State1 );
cache_loop(#cache{
  tags = Tags,
  wait = Wait,
  empty = Empty
} = State )->
  receive
    #update_index{
      from = From,
      module = Module,
      ref = Ref,
      tag = Tag,
      add = Add,
      del = Del
    } ->

      Value0 = get_tag_value( Tags, Module, Ref, Tag, Empty ),
      Value = udpate_value( Add, Del, Value0, Empty ),

      catch From ! { tag, Tag,  Value, Value0 },

      cache_loop(State#cache{ tags = Tags#{
        { Module, Ref, Tag } => { Value, ecomet_lib:ts()  }
      }});
    _->
      cache_loop( State )
  after
    Wait ->
      State1 = increase_cache( State ),
      cache_loop( State1 )
  end.

get_tag_value( Cache, Module, Ref, Tag, Empty )->
  case Cache of
    #{ { Module, Ref, Tag } := { Value, _TS } } ->
      Value;
    _->
      case Module:read(Ref,[Tag]) of
        []-> Empty;
        [{_,Value}]-> Value
      end
  end.

udpate_value( Add, Del, Value0, Empty )->
  if
    Value0 =:= Empty, Del =:= Empty -> Add;
    Value0 =:= Empty, Add =:= Empty -> Empty;
    Value0 =:= Empty->
      ecomet_bitmap:bit_andnot(Add, Del);
    Del =:= Empty ->
      ecomet_bitmap:bit_or( Value0, Add );
    Add =:= Empty->
      ecomet_bitmap:bit_andnot(Value0, Del);
    true ->
      Value1 = ecomet_bitmap:bit_or( Value0, Add ),
      ecomet_bitmap:bit_andnot(Value1, Del)
  end.

decrease_cache(State)->
  State.
%%decrease_cache(#cache{
%%  tags = Tags0,
%%  dur = Dur0,
%%  wait = Wait0
%%} = State)->
%%
%%  Dur =
%%    if
%%      Dur0 > 2 -> Dur0 bsr 1;
%%      true -> Dur0
%%    end,
%%
%%  Wait =
%%    if
%%      Wait0 > 2 -> Wait0 bsr 1;
%%      true -> Wait0
%%    end,
%%
%%  Tags = clear_cache( Tags0, ecomet_lib:ts() - Dur ),
%%
%%  State#cache{ tags = Tags, dur = Dur, wait = Wait }.

increase_cache(State)->
  State.
%%increase_cache(#cache{
%%  tags = Tags0,
%%  dur = Dur0,
%%  wait = Wait0
%%} = State)->
%%
%%  Dur =
%%    if
%%      Dur0 < ?CACHE_DUR -> Dur0 bsl 1;
%%      true -> Dur0
%%    end,
%%
%%  Wait =
%%    if
%%      Wait0 < ?CACHE_WAIT -> Wait0 bsl 1;
%%      true -> Wait0
%%    end,
%%
%%  Tags = clear_cache( Tags0, ecomet_lib:ts() - Dur ),
%%
%%  State#cache{ tags = Tags, dur = Dur, wait = Wait }.

%%clear_cache( Cache, TS )->
%%  maps:filter(fun(_K, {_, TagTS}) -> TS > TagTS end, Cache).

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