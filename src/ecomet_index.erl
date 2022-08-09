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
  is_exists/2,is_exists/3,
  get_supported_types/0,

  build_3gram/2,
  build_dt/2

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

-define(INDEX_LOG(Storage, Tag, Key, ID),{index_log, Storage, Tag, Key, ID}).

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
  IDP=ecomet_object:get_pattern(OID),
  ObjectID=ecomet_object:get_id(OID),
  IDH=ObjectID div ?BITSTRING_LENGTH,
  IDL=ObjectID rem ?BITSTRING_LENGTH,

  lists:foldl(fun(#log{field=Field,value=Value,type=Type,storage=Storage,oper=Oper},{Add,Del})->
    Tag={Field,Value,Type},
    Bit = if Oper =:= add-> 1; true -> 0 end,
    write_log( DB,Storage,IDP,IDH,IDL, Tag, Bit ),
    case Oper of
      add->{[Tag|Add],Del};
      del->{Add,[Tag|Del]}
    end
  end,{[],[]},Log).

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

  {Add,Del} = read_log( DB, Storage, Tag, Vector ),

  Result =
    case ecomet_backend:dirty_read(DB,?INDEX,Storage,Key) of
      not_found-> ecomet_bitmap:bit_andnot( Add, Del );
      Index-> ecomet_bitmap:bit_andnot(ecomet_bitmap:bit_or(Index, Add), Del)
    end,
  case ecomet_bitmap:zip(Result) of
    <<>> -> none;
    Zip -> Zip
  end.

is_exists(DB, Tag)->
  is_exists(DB, Tag, ?STORAGE_TYPES).
is_exists(DB, Tag, [Storage|Rest])->
  case read_tag(DB, Storage,[], Tag) of
    none -> is_exists( DB, Tag, Rest );
    _ -> true
  end;
is_exists(_DB, _Tag, [])->
  false.


%%==============================================================================================
%%	Index log
%%==============================================================================================
write_log(DB,Storage,IDP,IDH,IDL, Tag, Bit)->

  LogStorage = log_storage( Storage ),

  ok = ecomet_backend:write(DB,?INDEX,LogStorage, ?INDEX_LOG(Storage,Tag,{idl,IDP,IDH},IDL) ,Bit,_Lock = none),

  case ecomet_backend:dirty_counter( DB, ?INDEX, LogStorage, ?INDEX_LOG(Storage,Tag,{idh,IDP},IDH), 1) of
    1->
      case ecomet_backend:dirty_counter( DB, ?INDEX, LogStorage, ?INDEX_LOG(Storage,Tag,idp,IDP), 1) of
        1->
          ecomet_backend:dirty_counter( DB, ?INDEX, LogStorage, ?INDEX_LOG(Storage,Tag,idp,'$total'), 1);
        _->
          ok
      end;
    _->
      ok
  end,

  ok.

read_log( DB, Storage, Tag, [IDP,IDH])->

  LogStorage = log_storage( Storage ),
  case ecomet_backend:get_counter( DB, ?INDEX, LogStorage, ?INDEX_LOG(Storage,Tag,{idh,IDP},IDH)) of
    0 ->
      % Empty
      {<<>>,<<>>};
    Count ->

      StartKey = ?INDEX_LOG(Storage, Tag, {idl,IDP,IDH}, -1),
      EndKey = ?INDEX_LOG(Storage, Tag, {idl,IDP,IDH}, '$end'),

      RawLog =
        ecomet_backend:dirty_select(DB, ?INDEX, LogStorage, StartKey, EndKey, Count ),

      Log=
        [{IDL,Bit} || {?INDEX_LOG(_,_,_,IDL),Bit} <- RawLog],

      sort_log( Log, {[],[]})
  end;

read_log( DB, Storage, Tag, [IDP])->
  LogStorage = log_storage( Storage ),
  case ecomet_backend:get_counter( DB, ?INDEX, LogStorage,?INDEX_LOG(Storage,Tag,idp,IDP)) of
    0 -> {<<>>,<<>>};
    Count->
      StartKey = ?INDEX_LOG(Storage, Tag, {idh,IDP}, -1),
      EndKey = ?INDEX_LOG(Storage, Tag, {idh,IDP}, '$end'),

      RawLog =
        ecomet_backend:scan_counters(DB, ?INDEX, LogStorage, StartKey, EndKey, Count ),

      {
        ecomet_bitmap:set_bit(<<>>,[ IDH || {?INDEX_LOG(_,_,_,IDH),_} <- RawLog]),
        <<>>
      }
  end;

read_log( DB, Storage, Tag, [])->

  LogStorage = log_storage( Storage ),
  case ecomet_backend:get_counter( DB, ?INDEX, LogStorage,?INDEX_LOG(Storage,Tag,idp,'$total')) of
    0 -> {<<>>,<<>>};
    Count->
      StartKey = ?INDEX_LOG(Storage, Tag, idp, -1),
      EndKey = ?INDEX_LOG(Storage, Tag, idp, '$end'),

      RawLog =
        ecomet_backend:scan_counters(DB, ?INDEX, LogStorage, StartKey, EndKey, Count ),

      {
        ecomet_bitmap:set_bit(<<>>,[ IDP || {?INDEX_LOG(_,_,_,IDP),_} <- RawLog]),
        <<>>
      }
  end.


sort_log([{ID,Bit}|Rest],{Add,Del})->
  if
    Bit =:= 1-> sort_log(Rest,{[ID|Add],Del});
    true -> sort_log(Rest,{Add,[ID|Del]})
  end;
sort_log([],{Add,Del})->
  {
    ecomet_bitmap:set_bit(<<>>, lists:reverse(Add)),
    ecomet_bitmap:set_bit(<<>>, lists:reverse(Del))
  }.

log_storage( Storage ) when Storage =:= ?RAM->
  Storage;
log_storage( _Storage )->
  ?RAMDISC.


get_supported_types()->
  [
    simple,
    '3gram',
    datetime
  ].