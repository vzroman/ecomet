-module(ecomet_common_test_SUITE).

-include("ecomet.hrl").
-include("ecomet_test.hrl").

-define(OBJECTS,6000).
-define(PERIOD,1000).
-define(ITERATIONS,100).


%% API
-export([
  all/0,
  groups/0,
  init_per_testcase/2,
  end_per_testcase/2, 
  init_per_group/2,
  end_per_group/2,
  init_per_suite/1,
  end_per_suite/1
]).

%% Edit group
-export([
  edit/1
]).


all()->
  [
    {group,edit}
  ].

groups()->[
  {edit,
    [parallel],
    [edit||_<-lists:seq(1,?OBJECTS)]
  }
].

-define(FIELD(Schema),maps:merge(ecomet_field:from_schema(Schema),#{<<".pattern">> => ?OID(<<"/root/.patterns/.field">>)})).

init_per_suite(Config)->
    ?LOGDEBUG("Start backend"),
    ?BACKEND_INIT(),

    application:set_env([{ecomet,[{process_memory_limit, 6144}]}]),
    ecomet:dirty_login(<<"system">>),
    Pattern = ecomet:create_object(#{
      <<".name">> => <<"AI">>,
      <<".pattern">> => ?OID(<<"/root/.patterns/.pattern">>),
      <<".folder">> => ?OID(<<"/root/.patterns">>),
      <<"parent_pattern">> => ?OID(<<"/root/.patterns/.folder">>)
    }),

    FP = ?OID(<<"/root/.patterns/.field">>),
    FF = ?OID(Pattern),

    ecomet:set([root],#{<<"storage">> => ramdisc}, {<<".folder">> ,'=', FF}),
    ecomet:set([root],#{<<"index">> => ['3gram',simple]}, {'AND',[
      {<<".folder">> ,'=', FF},
      {<<".name">>,'=',<<".name">>}
    ]}),

    Fields = [
      {<<"title">>, string, ramdisc},
      {<<"value">>, float, ram},
      {<<"valueH">>, float, ramdisc},
      {<<"unit">>, string , ramdisc},
      {<<"digits">>, integer, ramdisc},
      {<<"length">>, integer, ramdisc},
      {<<"op_valueHiHi">>, float, ramdisc},
      {<<"op_valueHi">>, float, ramdisc},
      {<<"op_valueLo">>, float, ramdisc},
      {<<"op_valueLoLo">>, float, ramdisc},
      {<<"valueL">>, float, ramdisc},
      {<<"op_hysteresis">>, float, ramdisc},
      {<<"op_flagValueHiHi">>, bool, ramdisc},
      {<<"op_flagValueHi">>, bool, ramdisc},
      {<<"op_flagValueLo">>, bool, ramdisc},
      {<<"op_flagValueLoLo">>, bool, ramdisc},
      {<<"flagValueHiHi">>, bool, ram},
      {<<"flagValueHi">>, bool, ram},
      {<<"flagValueLo">>, bool, ram},
      {<<"flagValueLoLo">>, bool, ram}
    ],

    ?LOGDEBUG("fields ~p",[[ ?OID(ecomet:create_object(#{
      <<".name">> => N,
      <<".folder">> => FF,
      <<".pattern">> => FP,
      <<"type">> => T,
      <<"storage">> => S
     })) || {N,T,S} <- Fields]]),

     ?LOGDEBUG("AI pattern is ready"),

     OF = ?OID( ecomet:create_object(#{
      <<".name">> => <<"test_objects">>,
      <<".folder">> => ?OID(<<"/root">>),
      <<".pattern">> => ?OID(<<"/root/.patterns/.folder">>)
     })),

    
    [ ecomet:create_object(#{
      <<".name">> => <<"obj_",(integer_to_binary(I))/binary>>,
      <<".folder">> => OF,
      <<".pattern">> => FF
      }) || I <- lists:seq(1,?OBJECTS) ],

    CounterRef = atomics:new(1, [{signed,false}]),    
    [
      {folder, OF},
      {counter, CounterRef} 
    | Config].

end_per_suite(_Config)->
    ?LOGDEBUG("Stop backend"),
    ?BACKEND_STOP(),
  _Config.


init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

edit(Config)->
  Folder = ?GET(folder,Config),
  Counter = ?GET(counter,Config),

  I = atomics:add_get(Counter,1,1),
  %timer:sleep(10 *I), 
  % Упорядочный лист хешей. Хранение листа в хранилище в котором лежит поле .name
  % 
  {ok,OID} = ecomet_folder:find_object_system(Folder,<<"obj_",(integer_to_binary(I))/binary>>),
  erlang:garbage_collect(self()),


  ?LOGDEBUG("process ~p, object ~p",[I,OID]),
  ecomet:dirty_login(<<"system">>),
  erlang:garbage_collect(self()),

  Obj = ecomet:open( OID ),

  Timings = 
    [begin 
      T0 = erlang:system_time(microsecond),
      ecomet:edit_object(Obj,#{<<"value">> => It}),
      Delta = erlang:system_time(microsecond) - T0,
      timer:sleep(?PERIOD),
      Delta
    end || It <- lists:seq(1, ?ITERATIONS)],

  ?LOGDEBUG("avg time ~p",[lists:sum(Timings)/length(Timings)]),
  ok.