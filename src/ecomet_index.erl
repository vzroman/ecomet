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

-record(log,{field,value,type,storage,oper}).
%% ====================================================================
%% API functions
%% ====================================================================
-export([
  build_index/4,
  delete_object/2,
  read_tag/4,
  get_supported_types/0,

  build_3gram/2,
  build_dt/2,

  % only for tests
  build_bitmap/7
]).

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([
  build_back_tags/2,
  merge_backtags/2,
  dump_log/2,
  get_unchanged/2
]).
-endif.

% Build indexes for changed fields. Return:
% {AddTags,UnchangedTags,DelTags,BackIndex}
% BackIndex structure:
%	#{storage1:=
%   #{field1:=[{value,type}|...],
%     field1:=[{value,type}|...]
%   }
%   ...
% }
%
build_index(OID,Map,Fields,BackTags)->
  % Build update
  UpdatedBackTags=build_back_tags(Fields,Map),
  % Calculate difference
  {NewBackTags,Log}=merge_backtags(UpdatedBackTags,BackTags),
  % Update storage
  {Add,Del}=dump_log(OID,Log),
  % Unchanged intersection of New and Old
  Unchanged=get_unchanged(NewBackTags,BackTags),
  % Return only changed storages
  {Add,Unchanged,Del,maps:with(maps:keys(UpdatedBackTags),NewBackTags)}.

% Delete object from indexes
delete_object(OID,BackIndex)->
  {_,Log}=merge_backtags(BackIndex,#{}),
  dump_log(OID,[TLog#log{oper=del}||TLog<-Log]).
%%------------------------------------------------------------------------------
%%	 Step 1. Build backtag structure for changed fields
%%------------------------------------------------------------------------------
build_back_tags(Fields,Map)->
  lists:foldl(fun({Name,Value},BackTag)->
    case ecomet_field:get_index(Map,Name) of
      {ok,none}->BackTag;
      {ok,Types}->
        ListValue=
          if
            is_list(Value) -> Value;
            Value=:=none ->[];
            true -> [ Value ]
          end,
        FieldTags=
          lists:foldl(fun(Type,ValueList)->
            [{TagValue,Type}||TagValue<-build_values(Type,ListValue)]++ValueList
          end,[],Types),
        {ok,Storage}=ecomet_field:get_storage(Map,Name),
        StorageFields=maps:get(Storage,BackTag,#{}),
        BackTag#{Storage=>StorageFields#{Name=>ordsets:from_list(FieldTags)}}
    end
  end,#{},Fields).

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

%%------------------------------------------------------------------------------
%% 	Step 2. Calculating difference between old tags and new
%%------------------------------------------------------------------------------
merge_backtags(Updated,Old)->
  lists:foldl(fun({Storage,Fields},{BackIndex,SLog})->
    OldFields=maps:get(Storage,BackIndex,#{}),
    Log=
      lists:foldl(fun({Field,Tags},FLog)->
        OldTags=maps:get(Field,OldFields,[]),
        [#log{field=Field,value=Value,type=Type,storage=Storage,oper=add}||{Value,Type}<-ordsets:subtract(Tags,OldTags)]++
        [#log{field=Field,value=Value,type=Type,storage=Storage,oper=del}||{Value,Type}<-ordsets:subtract(OldTags,Tags)]++
        FLog
      end,SLog,maps:to_list(Fields)),
    SFields=maps:merge(OldFields,Fields),
    {BackIndex#{Storage=>SFields},Log}
  end,{Old,[]},maps:to_list(Updated)).

%%------------------------------------------------------------------------------
%%	Step 3. Save indexes to storage
%%------------------------------------------------------------------------------
dump_log(OID,Log)->
  DB=ecomet_object:get_db_name(OID),
  PatternID=ecomet_object:get_pattern(OID),
  ObjectID=ecomet_object:get_id(OID),
  IDHN=ObjectID div ?BITSTRING_LENGTH,
  IDLN=ObjectID rem ?BITSTRING_LENGTH,
  % Sorting solves next issues:
  % 1. log has structure {  field, value , type, storage, oper }. So, we sort fields by their names first.
  % When we run through log, we obtain locks on tags we change. If two concurrent processes want a couple of same tags,
  % then they will try obtain locks on these tags in the same order. So we exclude states when process1 locked tag1 and tries
  % to lock tag2, and this time process2 locked tag2 and tries to lock tag1. Mnesia as backend is deadlock free, but it is time consuming.
  % 2. We reverse sorted tag_logs. So, system fields such as ".folder", ".pattern" and so on are in the tail and  locked after others.
  % We do not hold locks on them while indexing other fields.
  lists:foldl(fun(#log{field=Field,value=Value,type=Type,storage=Storage,oper=Oper},{Add,Del})->
    Tag={Field,Value,Type},
    ecomet_index:build_bitmap(Oper,Tag,DB,Storage,PatternID,IDHN,IDLN),
    case Oper of
      add->{[Tag|Add],Del};
      del->{Add,[Tag|Del]}
    end
  end,{[],[]},lists:reverse(ordsets:from_list(Log))).

%%------------------------------------------------------------------------------
%%	Step 4. Calculate unchanged tags
%%------------------------------------------------------------------------------
get_unchanged(New,Old)->
  maps:fold(fun(Storage,Fields,SAcc)->
    NewFields=maps:get(Storage,New),
    maps:fold(fun(Field,Tags,FAcc)->
      NewTags=maps:get(Field,NewFields),
      [{Field,Value,Type}||{Value,Type}<-ordsets:intersection(Tags,NewTags)]++FAcc
    end,SAcc,maps:with(maps:keys(NewFields),Fields))
  end,[],maps:with(maps:keys(New),Old)).

%%------------------------------------------------------------------------------
%%		Bitmap indexes
%%------------------------------------------------------------------------------
build_bitmap(Oper,Tag,DB,Storage,PatternID,IDHN,IDLN)->
  case bitmap_level(Oper,DB,Storage,{Tag,{idl,PatternID,IDHN}},IDLN) of
    stop->ok;
    up->
      case bitmap_level(Oper,DB,Storage,{Tag,{idh,PatternID}},IDHN) of
        stop->ok;
        up->bitmap_level(Oper,DB,Storage,{Tag,patterns},PatternID)
      end
  end.

%%--------------Add an ID to the index level--------------------------------------
bitmap_level(add,DB,Storage,Tag,ID)->
  case ecomet_backend:read(DB,?INDEX,Storage,Tag,write) of
    not_found->
      Value = ecomet_bitmap:zip(ecomet_bitmap:set_bit(<<>>,ID)),
      ok = ecomet_backend:write(DB,?INDEX,Storage,Tag,Value),
      % The index didn't exist before, we need to add the value to the upper level also
      up;
    LevelValue->
      Value = ecomet_bitmap:zip(ecomet_bitmap:set_bit(LevelValue,ID)),
      ok = ecomet_backend:write(DB,?INDEX,Storage,Tag,Value),
      % The index already exists, no need to update the upper level
      stop
  end;
%%--------------Remove an ID from the index level--------------------------------------
bitmap_level(del,DB,Storage,Tag,ID)->
  case ecomet_backend:read(DB,?INDEX,Storage,Tag,write) of
    not_found->
      % Why are we here?
      ?LOGWARNING("an attempt to delete an ID in the absent index level, ID ~p, DB ~p, storage type ~p, tag ~p",[
        ID,DB,Storage,Tag
      ]),
      % We are not sure if the upper level index exits, so we need to try to update it too
      up;
    LevelValue->
      LeftValue = ecomet_bitmap:zip(ecomet_bitmap:reset_bit(LevelValue,ID)),
      case ecomet_bitmap:is_empty(LeftValue) of
        true->
          ok = ecomet_backend:delete(DB,?INDEX,Storage,Tag),
          % The level is deleted, we need to update the upper level index
          up;
        _->
          ok = ecomet_backend:write(DB,?INDEX,Storage,Tag,LeftValue),
          % The level is updated (not removed) therefore there is no need to update the upper level
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
  case ecomet_backend:dirty_read(DB,?INDEX,Storage,Key) of
    not_found->none;
    IndexValue-> IndexValue
  end.

get_supported_types()->
  [
    simple,
    '3gram',
    datetime
  ].