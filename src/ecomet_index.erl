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
  object_tags/1,
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
build_index(Changes, BackTags, Map)->
  % Build index tags for changed fields.
  % BackIndex structure:
  %	#{storage1:=
  %   #{field1:=[{value,type}|...],
  %     field1:=[{value,type}|...]
  %   }
  %   ...
  % }
  %
  maps:fold(fun(Field,{_, Value},Acc)->
    FieldTags =
      if
        Value =/= none->
          field_tags( Field, Map, Value );
        true->
          []
      end,
    {ok,Type} = ecomet_field:get_storage(Map, Field),
    TAcc0 = maps:get(Type, Acc, #{}),
    TAcc =
      if
        length(FieldTags) >0 ->
          TAcc0#{ Field => FieldTags };
        true->
          maps:remove( Field, TAcc0 )
      end,
    if
      map_size( TAcc ) > 0 ->
        Acc#{ Type => TAcc};
      true->
        maps:remove(Type, Acc)
    end
  end,BackTags, Changes).

object_tags(BackTags)->
  maps:keys(maps:fold(fun(_StorageType,Fields,Acc0)->
    maps:fold(fun(Field, Tags, Acc1)->
      lists:foldl(fun({Value,IndexType}, Acc)->
        Acc#{ {Field, Value, IndexType}=>1 }
      end,Acc1,Tags)
    end, Acc0, Fields)
  end,#{}, BackTags)).

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
        [[{TagValue,Type}||TagValue<-build_values(Type,ListValue)]|Acc]
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
  ByDBsTypes = group_by_dbs_types_tags( LogList ),
  maps:fold(fun(DB,Types,_)->
    maps:fold(fun(Type,Tags,_)->
      maps:fold(fun(Tag,OIDs,_)->
        Commit = prepare_commit(Tag, OIDs),
        Rollback = prepare_rollback(Commit),
        ok = ecomet_db:bulk_write(DB,?INDEX,Type,Commit,_Lock = none),
        ok = ecomet_db:bulk_on_abort(DB,?INDEX,Type,Rollback)
      end, undefined, Tags)
    end, undefined, Types)
  end, undefined, ByDBsTypes).


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
  lists:foldl(fun(#{db := DB, object:=#{ <<".oid">>:=OID, object := Object }},Acc)->
    DBAcc1 =lists:foldl(fun(Type, DBAcc)->
      case ecomet_db:changes( DB, ?DATA, Type, OID ) of
        { Data0, Data1 }->
          Tags0 =
            if
              is_map( Data0 )-> maps:get(tags,Data0,#{});
              true -> #{}
            end,
          Tags1 =
            if
              is_map( Data1 )-> maps:get(tags,Data1,#{});
              true -> #{}
            end,
          case Tags0 of
            Tags1->
              DBAcc;
            _->
              Fields = lists:usort(maps:keys(Tags0) ++ maps:keys(Tags1)),
              TypeAcc1 = lists:foldl(fun(Field, TypeAcc)->
                case {maps:get(Field,Tags0,[]), maps:get(Field,Tags1,[])} of
                  {SameFieldTags, SameFieldTags}->
                    % No field tags changes
                    TypeAcc;
                  { FieldTags0, FieldTags1 }->
                    %-----Add tags-------------------------
                    TypeAcc1 =
                      lists:foldl(fun({Value,IndexType}, TAcc)->
                        Tag = {Field,Value,IndexType},
                        TagAcc = maps:get(Tag,TAcc,#{}),
                        TAcc#{ Tag => TagAcc#{ OID => true}}
                      end,TypeAcc, FieldTags1 -- FieldTags0 ),
                    %-----Delete tags-------------------------
                    lists:foldl(fun({Value,IndexType}, TAcc)->
                      Tag = {Field,Value,IndexType},
                      TagAcc = maps:get(Tag,TAcc,#{}),
                      TAcc#{ Tag => TagAcc#{ OID => false}}
                    end, TypeAcc1, FieldTags0 -- FieldTags1 )
                end
              end, maps:get(Type, DBAcc,#{}), Fields),
              DBAcc#{ Type => TypeAcc1 }
          end;
        _->
          % No DB storage type changes
          DBAcc
      end
    end, maps:get( DB, Acc, #{}), ecomet_object:get_storage_types( Object )),
    Acc#{ DB => DBAcc1 }
  end,#{}, LogList).

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
  {Add, Del}=
    maps:fold(fun(ID,Value,{AddAcc,DelAcc})->
      if
        Value -> { ecomet_bitmap:set_bit(AddAcc,ID), DelAcc };
        true-> { AddAcc, ecomet_bitmap:set_bit(DelAcc,ID) }
      end
    end,{<<>>,<<>>}, Update),

  case Module:read(Ref,[{?INDEX,[Tag]}]) of
    []->
      case ecomet_bitmap:zip( ecomet_bitmap:bit_andnot(Add,Del)) of
        <<>>->
          stop;
        LevelValue->
          ok = Module:write( Ref, [{{?INDEX,[Tag]}, LevelValue}]),
          true
      end;
    [{_,LevelValue0}]->
      LevelValue1 = ecomet_bitmap:bit_or(LevelValue0, Add ),
      case ecomet_bitmap:zip( ecomet_bitmap:bit_andnot(LevelValue1,Del)) of
        <<>>->
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