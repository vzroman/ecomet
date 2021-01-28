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
-module(ecomet_folder).

-include("ecomet.hrl").
-include("ecomet_schema.hrl").

-behaviour(ecomet_object).

%%=================================================================
%%	API
%%=================================================================
-export([
  oid2path/1,
  path2oid/1,
  find_object/2,find_object_system/2,
  get_db_id/1,
  get_db_name/1,
  find_mount_points/1,
  get_content/1,get_content_system/1,
  is_empty/1
]).

%%===========================================================================
%% Behaviour API
%%===========================================================================
-export([
  on_create/1,
  on_edit/1,
  on_delete/1
]).

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).
-export([
  check_database/1,
  inherit_rights/1,
  recursive_rights/1,
  apply_recursion/1,
  apply_rights/3,
  rights_changes/2
]).
-endif.

%%=================================================================
%%	API
%%=================================================================
oid2path({?FOLDER_PATTERN,?ROOT_FOLDER})->
  <<"/root">>;
oid2path(OID)->
  Object = ecomet_object:construct(OID),
  #{ <<".name">>:=Name, <<".folder">>:=FolderID } = ecomet:read_fields(Object, [<<".folder">>,<<".name">>]),
  <<(oid2path(FolderID))/binary,"/",Name/binary>>.

path2oid(<<"/root">>)->
  {ok, {?FOLDER_PATTERN,?ROOT_FOLDER} };
%%Search object by path
path2oid(<<"/root",_/binary>> = Path)->
  { MountPath, FolderID } = ecomet_schema:get_mounted_folder(Path),
  HeadSize = size(MountPath),
  <<MountPath:HeadSize/binary,Tail/binary>> = Path,
  Tokens = string:tokens(unicode:characters_to_list(Tail),"/"),
  Path1= [ unicode:characters_to_binary(Name) || Name <- Tokens],
  path2oid(FolderID,Path1);
path2oid(_Path)->
  {error,invalid_path}.
path2oid(FolderID,[Name|Tail])->
  case find_object_system(FolderID,Name) of
    {ok,ItemID} -> path2oid(ItemID,Tail);
    _->{ error, invalid_path }
  end;
path2oid(OID,[])->
  {ok,OID}.

find_object(FolderID,Name)->
  DB = get_db_name(FolderID),
  case ecomet_query:get([DB],[<<".oid">>],{'AND',[
    { <<".folder">>,'=',FolderID},
    { <<".name">>,'=',Name }
  ]}) of
    [OID|_]-> { ok, OID };
    _->{ error, not_found }
  end.
find_object_system(FolderID,Name)->
  DB = get_db_name(FolderID),
  case ecomet_query:system([DB],[<<".oid">>],{'AND',[
    {<<".folder">>,'=',FolderID},
    { <<".name">>,'=',Name}
  ]}) of
    [OID|_]-> { ok, OID };
    _->{ error, not_found }
  end.

get_content(Folder)->
  DB = ecomet_object:get_db_name(Folder),
  ecomet_query:get([DB],[<<".oid">>],{<<".folder">>,'=',Folder}).

get_content_system(Folder)->
  DB = get_db_name(Folder),
  ecomet_query:system([DB],[<<".oid">>],{<<".folder">>,'=',Folder}).

is_empty(Folder)->
  DB = get_db_name(Folder),
  0 =:= ecomet_query:system([DB],[count],{<<".folder">>,'=',Folder}).


get_db_id(FolderID)->
  case ecomet_schema:get_mounted_db(FolderID) of
    none->
      % The folder is a simple folder, no DB is mounted to it.
      % Obtain the ID of the db from the ID of the folder
      ecomet_object:get_db_id(FolderID);
    DB->
      % The folder itself is a mounted point
      ecomet_schema:get_db_id(DB)
  end.

get_db_name(FolderID)->
  case ecomet_schema:get_mounted_db(FolderID) of
    none->
      DB_ID=ecomet_object:get_db_id(FolderID),
      ecomet_schema:get_db_name(DB_ID);
    DB->
      DB
  end.

find_mount_points(DB) when is_atom(DB);is_binary(DB)->
  {ok,DB_OID} = ecomet_db:get_by_name(DB),
  find_mount_points(DB_OID);
find_mount_points(ID)->
  DBs = ecomet_db:get_databases(),
  % Folder descendants
  ecomet_query:system(DBs,[<<".oid">>],{'AND',[
    {<<".pattern">>,'=',{?PATTERN_PATTERN,?FOLDER_PATTERN}},
    {<<"database">>,'=',?OID(ID)}
  ]}).


%%=================================================================
%%	Ecomet behaviour
%%=================================================================
on_create(Object)->
  check_database(Object),
  inherit_rights(Object),
  ok.

on_edit(Object)->
  check_database(Object),
  recursive_rights(Object),
  apply_recursion(Object),
  ok.

on_delete(Object)->
  % Recursively delete all the content.
  % IMPORTANT! The search is under admin rights (system), but the remove
  % is in the user context. If the user does not have rights for the object the whole
  % transaction will throw
  [ begin
      Item = ecomet:open(ItemID,_Lock=none),
      ok = ecomet:delete_object(Item)
    end || ItemID <- get_content_system(?OID(Object)) ],

  % Unmount a database if some is mounted
  case ecomet:read_field(Object,<<"database">>) of
    {ok,DB} when DB=/=none->
      ok = ecomet_schema:unmount_db(?OID(Object));
    _->
      ok
  end,

  ok.


inherit_rights(Object)->
  % If the content rights are not defined explicitly they are
  % inherited from the common object rights
  #{
    <<".readgroups">>:=Read,
    <<".writegroups">>:=Write
  } = ecomet:read_fields(Object,#{
    <<".readgroups">> => none,
    <<".writegroups">> => none
  }),
  #{
    <<".contentreadgroups">>:=ContentRead,
    <<".contentwritegroups">>:=ContentWrite
  } = ecomet:read_fields(Object,#{
    <<".contentreadgroups">> => Read,
    <<".contentwritegroups">> => Write
  }),
  ok = ecomet:edit_object(Object,#{
    <<".contentreadgroups">>=>ContentRead,
    <<".contentwritegroups">>=>ContentWrite
  }).

recursive_rights(Object)->
  {ok,Recursion}=ecomet:read_field(Object,<<"recursive_rights">>),
  if
    Recursion=:= true ->
      Read=rights_changes(Object,<<".readgroups">>),
      Write=rights_changes(Object,<<".writegroups">>),
      Changes=
        case {Read,Write} of
          {none,none}->none;
          {none,_}-> #{ <<".contentwritegroups">> => Write };
          {_,none}-> #{ <<".contentreadgroups">> => Read };
          _->
            #{
              <<".contentreadgroups">> => Read,
              <<".contentwritegroups">> => Write
            }
        end,
      if
        Changes=/=none ->
          apply_rights(Object,Changes,[{<<"recursive_rights">>,true}]),
          ok;
        true -> ok
      end;
    true -> ok
  end.

rights_changes(Object,Field)->
  case ecomet:field_changes(Object,Field) of
    none->none;
    {New,Old}->
      New1=
        if is_list(New)-> ordsets:from_list(New); true->[] end,
      Old1=
        if is_list(Old)-> ordsets:from_list(Old); true->[] end,
      Append=ordsets:subtract(New1,Old1),
      Remove=ordsets:subtract(Old1,New1),
      if
        Append=:=[],Remove=:=[]-> none;
        true -> { Append, Remove }
      end
  end.

apply_rights(Object,Changes,Additional)->
  EditFields=
    [case ecomet:read_field(Object,Field) of
       {ok,Value} when is_list(Value)->
         Value1=ordsets:subtract(ordsets:from_list(Value),Remove),
         Value2=ordsets:union(Value1,Append),
         {Field,Value2};
       _->{Field,Append}
     end||{Field,{Append,Remove}}<-maps:to_list(Changes)],
  Object1=ecomet:edit_object(Object,maps:from_list(EditFields++Additional)),
  Object1.

apply_recursion(Object)->
  {ok,Recursion}=ecomet:read_field(Object,<<"recursive_rights">>),
  if
    Recursion=:= true ->
      Read=rights_changes(Object,<<".contentreadgroups">>),
      Write=rights_changes(Object,<<".contentwritegroups">>),
      Changes=
        case {Read,Write} of
          {none,none}->none;
          {none,_}-> #{ <<".writegroups">> => Write };
          {_,none}-> #{ <<".readgroups">> => Read };
          _->
            #{
              <<".readgroups">> => Read,
              <<".writegroups">> => Write
            }
        end,
      if
        Changes=/=none ->
          OID=?OID(Object),
          ecomet:on_commit(fun()->
            [begin
               Item=ecomet:open_nolock(ItemID),
               Additional=
                 case ecomet:read_field(Item,<<"recursive_rights">>) of
                   {ok,_}->[{<<"recursive_rights">>,true}];
                   _->[]
                 end,
               apply_rights(Item,Changes,Additional)
             end||ItemID<-get_content(OID)]
          end);
        true -> ok
      end,
      ok = ecomet:edit_object(Object,[{<<"recursive_rights">>,false}]);
    true ->ok
  end.

check_database(Object)->
  case ecomet:field_changes(Object,<<"database">>) of
    none->ok;
    { MountedDB, UnmountedDB }->
      FolderID = ?OID(Object),
      % Step 1. Unmount the database (if mounted)
      if
        UnmountedDB =/=none->
          % Detaching a database from a folder.
          % Unmount is only allowed when the folder is empty
          case is_empty(FolderID) of
            true->
              ok = ecomet_schema:unmount_db(FolderID);
            _->
              ?ERROR(contains_objects)
          end;
        true -> ok
      end,

      % Step 2. Mount a database
      if
        MountedDB =/=none ->
          DB = ecomet_db:get_name(MountedDB),
          ok = ecomet_schema:mount_db(FolderID,DB);
        true -> ok
      end
  end.