# Ecomet

Object oriented reactive DB engine based on distributed disk storage.
Ecomet feature list:
* fair ACID transaction
* distributed disk storage
* reversed search problem solution

# Basic concepts

Pattern - object template (specification) describing field names and associated descriptions.
Specified by service key <<".pattern">>

Object - instance of particular object template which is instanced by real values

Field - object property of particular type. Object pattern contains field type specification
while object itself contains real field value

Folder - parent of given object. Specified by service key <<".folder">>

Mount point - 

OID - unique object identifier with following rules:
* the id is a tuple of 2 elements: { PatternID, ObjectID }
  - the PatternID is an id of the pattern (type) of the object. It defines its schema.
  The PatternID consists only of the second (ObjectID) of the related pattern
  - the ObjectID is a unique (system wide) id of the object within a pattern (type)
* the ObjectID is a composed integer that can be presented as:
  - IDHIGH = ObjectID div ?BITSTRING_LENGTH. It's sort of high level degree
  - IDLOW  = ObjectID rem ?BITSTRING_LENGTH (low level degree)
* The system wide increment is too expensive, so the initial increment is unique node-wide only
  and then the unique ID of the node is twisted into the IDHIGH. Actually it is added
  as 2 least significant bytes to the IDHIGH (IDHIGH = IDHIGH bsl 16 + NodeID )
* To be able to obtain the database to which the object belongs we insert (code) it into
  the IDHIGH the same way as we do with the NodeID: IDHIGH = IDHIGH bsl 8 + MountID.
The final IDHIGH is:  <IDHIGH,NodeID:16,DB:8>

Path - alternative object identifier aka folder structuer used by OSs


# Basic code snippets

## Login DB
```
4> % access DB as admin
5> ecomet_user:on_init_state().                                                                                                       
undefined
```

## Pattern manipulations

```erlang
1> % create spec for string field type
2> FS = #{type => string, subtype => none, index => none, required => false, storage => ram, default => none, autoincrement => false}.  #{autoincrement => false,default => none,index => none,
  required => false,storage => ram,subtype => none,
  type => string}
> % low level API for pattern creation (pattern OID should be {3, X})
3> ecomet_schema:register_type({3, 42}, Obj#{<<"str">> => FS}).             
```
Edit pattern and update all affected objects - TBD 

## Object manipulation

```erlang
6> % create new DB object based on existing pattern
6> f(NewObj), NewObj = #{<<".name">> => <<"testName">>, <<".folder">> => {2, 999}, <<".pattern">> => {3, 42}, <<"field">> => <<"value1">>}.
#{<<".folder">> => {2,999},
  <<".name">> => <<"testName">>,
  <<".pattern">> => {4,42},
  <<"field">> => <<"value1">>}
7> % create new ecomet object
7> EcometObj = ecomet:create_object(NewObj).
{object,{1001,1},
        true,
        #{index => [],
          <<"field">> =>
              #{autoincrement => false,default => none,index => none,
                required => false,storage => ram,subtype => none,
                type => string}},
        false,undefined}
8> % read fields from object
8> ecomet_object:read_all(EcometObj).
#{<<"field">> => <<"value1">>}
9> % update value of field in object
9> ecomet_object:edit(EcometObj, #{<<"field">> => <<"new_value">>}).
ok
10> ecomet_object:read_all(EcometObj).
#{<<"field">> => <<"new_value">>}
11> % delete object ???
11> ecomet_object:delete(EcometObj).                                 
** exception error: no match of right hand side value {error,undefined_field}
     in function  ecomet_object:delete/1 (/home/nick/proj/ecomet/src/ecomet_object.erl, line 126)
```

## Folder operation
Iterations over objects in single folder - TBD

# Basic queries

```erlang
% low level API
1> Results = ecomet_query:get([root], [<<".name">>, <<".pattern">>, <<"string1">>], {<<"string1">>,'=',<<"value1">>}).
{[<<".name">>,<<".pattern">>,<<"string1">>],
          [[<<"test1">>,{3,1001},<<"value1">>],
           [<<"test101">>,{3,1001},<<"value1">>],
           [<<"test201">>,{3,1001},<<"value1">>],
           [<<"test301">>,{3,1001},<<"value1">>],
           [<<"test401">>,{3,1001},<<"value1">>],
           [<<"test501">>,{3,1001},<<"value1">>],
           [<<"test601">>,{3,1001},<<"value1">>],
           [<<"test701">>,{3,1001},<<"value1">>],
           [<<"test801">>,{3,1001},<<"value1">>],
           [<<"test901">>,{3,1001},<<"value1">>],
           [<<"test1">>,{3,1002},<<"value1">>],
           [<<"test101">>,{3,1002},<<"value1">>],
           [<<"test201">>,{3,1002},<<"value1">>],
           [<<"test301">>,{3,1002},<<"value1">>],
           [<<"test401">>,{3,1002},<<"value1">>],
           [<<"test501">>,{3,1002},<<"value1">>],
           [<<"test601">>,{3,1002},<<"value1">>],
           [<<"test701">>,{3,1002},<<"value1">>],
           [<<"test801">>,{3,1002},<<"value1">>],
           [<<"test901">>,{3,1002},<<"value1">>]]}
% JSON adapter is high level API
2> ecomet_json:encode(Results).
#{
  <<"field_names">> => [<<".name">>,<<".pattern">>,<<"string1">>],
  <<"results">> => [
    #{
      <<".name">> => <<"test1">>,
      <<"test">> => <<"{3,1001}">>,
      <<"string1">> => <<"value1">>
    },
    ...
    #{
      <<".name">> => <<"test901">>,
      <<"test">> => <<"{3,1002}">>,
      <<"string1">> => <<"value1">>
    }
  ]
}
```

## Datatype conversion

| ecomet types       | Json Types     | Notes     |
| :------------- | :----------: | -----------: |
| Erlang binary e.g. <<123, 54>> | "ezY=" | base64 encoded JSON string |
| Erlang string e.g. "123" | "123" | JSON string |
| Erlang atom e.g. '@deleted' | "'@deleted'" | stringified JSON string |
| Erlang special atoms e.g. `true` or `false` or `null` | `true` or `false` or `null` | JSON primitives |
| Erlang numeric e.g. -1, 0.4 | -1, 0.4 | Just Json numeric |
| Erlang map of anything e.g. #{1 => <<"123">>, atom => "123"}| {1: "MTIz", "atom": "123"} | JSON object with recursive application of rules above |
| Erlang tuple e.g. {1, "hello", atom} | "{1, \"hello\", atom}" | stringified JSON string |
| Erlang list e.g. [1, "hello", atom] | "{1, \"hello\", atom}" | stringified JSON string |

 