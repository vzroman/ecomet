# Subscriptions Engine Review

## Findings (ordered by severity)

1. **Critical: existing query memberships are overwritten when adding new ones**
   In [`ecomet_subscription_object.erl#L1886`](./src/ecomet_subscription_object.erl#L1886), `add_queries_to_object/4` sets:
   `queries = ordsets:from_list(maps:keys(AddQueries))`
   This drops previously attached query refs for that object instead of unioning with them.
   Impact: an object that should belong to multiple queries can silently stop notifying older query subscribers and can even be removed from `objects` while still present in query result sets.

2. **High: same client with multiple subscriptions to the same query is broken**
   - Query server only rejects duplicate `SubsID` per client ([`ecomet_subscription_query.erl#L319`](./src/ecomet_subscription_query.erl#L319)).
   - Object worker stores query clients keyed only by `ClientID` ([`ecomet_subscription_object.erl#L583`](./src/ecomet_subscription_object.erl#L583)), so later subs overwrite earlier ones.
   - Unsubscribe removes by client (not by subscription id) ([`ecomet_subscription_query.erl#L480`](./src/ecomet_subscription_query.erl#L480), [`ecomet_subscription_object.erl#L591`](./src/ecomet_subscription_object.erl#L591)).
   Impact: lost notifications and wrong `SubsID` routing.

3. **High: leaked/stale query index entries on init failure**
   In [`ecomet_subscription_query.erl#L411`](./src/ecomet_subscription_query.erl#L411), index is built before `ecomet_query:system/3` ([`#L414`](./src/ecomet_subscription_query.erl#L414)) and before query state is committed.
   If later steps fail, handler returns `State0`, but index/global tags remain.
   Consequence: `find/2` can return refs absent from `queries`, causing wait-query accumulation ([`ecomet_subscription_object.erl#L1843`](./src/ecomet_subscription_object.erl#L1843)).

4. **Medium: potential hang in global tag synchronization**
   `wait_confirm/2` has no timeout ([`ecomet_subscription_object.erl#L189`](./src/ecomet_subscription_object.erl#L189)).
   If any worker cast is dropped (worker restart window), confirmation may never arrive; error branches in `handle_cast` also do not send confirm ([`#L290`](./src/ecomet_subscription_object.erl#L290), [`#L303`](./src/ecomet_subscription_object.erl#L303)).
   This can block subscribe/unsubscribe paths that call `global_set/reset`.

5. **Medium: `ordsets` APIs are used with plain lists in node tracking**
   In [`ecomet_subscription_nodes.erl#L85`](./src/ecomet_subscription_nodes.erl#L85) and [`#L97`](./src/ecomet_subscription_nodes.erl#L97), `ordsets:union/subtract` are called with non-normalized lists.
   Under cluster churn, active-node set correctness is not guaranteed.

6. **Low: unexpected calls to subscription object server can stall callers**
   Fallback `handle_call` returns `{noreply, State}` without replying ([`ecomet_subscription_object.erl#L225`](./src/ecomet_subscription_object.erl#L225)).
   Unexpected callers will just timeout.

---

## Test coverage gaps

1. Query index behavior is explicitly untested: [`ecomet_subscription_query_SUITE.erl#L398`](./test/subscriptions/ecomet_subscription_query_SUITE.erl#L398) (`index_test` is TODO).
2. No test for one client creating multiple subs IDs for the same query (bug #2).
3. No test where one object matches multiple queries simultaneously; current query test uses mutually exclusive predicates (`f3=12` vs `f3=23`) ([`ecomet_subscription_object_SUITE.erl#L1465`](./test/subscriptions/ecomet_subscription_object_SUITE.erl#L1465), [`#L1482`](./test/subscriptions/ecomet_subscription_object_SUITE.erl#L1482)).
4. No fault-injection tests for init failure after index build or for missing global confirmations.
5. No dedicated suite for `ecomet_subscription_nodes`.

---

## What was run

- `ERL_FLAGS="-ecomet http undefined" ./rebar3 ct --spec=./test/subscriptions/test.spec`
- Result: **All 16 subscription tests passed** (the issues above are latent and currently untested).
