# Erlang Application Style Guide

Status: accepted.

Sources:

- `ErlangProgramming.pdf`, Chapter 20, "Style and Efficiency".
- Example modules:
  - `/home/roman/PROJECTS/SOURCES/ecomet/src/ecomet_subscription_nodes.erl`
  - `/home/roman/PROJECTS/SOURCES/ecomet/src/ecomet_subscription_object.erl`
  - `/home/roman/PROJECTS/SOURCES/ecomet/src/ecomet_subscription_pool.erl`
  - `/home/roman/PROJECTS/SOURCES/ecomet/src/ecomet_subscription_query.erl`
  - `/home/roman/PROJECTS/SOURCES/ecomet/src/ecomet_subscription_sup.erl`
  - `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_copy.erl`
  - `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction.erl`

This guide separates strict requirements from softer recommendations. Requirements
are intended for normal production code. Recommendations are review heuristics and
may be ignored when a module has a documented reason.

## Observed Local Style

- Application modules use clear prefixes: `ecomet_...` and `zaya_...`.
- Export declarations are commonly grouped by role with banner comments:
  `API`, `Transaction API`, `Query API`, `Remote API`, `Internal API`, and
  `OTP API`.
- Public process APIs usually hide `gen_server:call/3`, `gen_server:cast/2`, and
  direct message protocols from callers.
- Registered names normally follow module names, or a bounded worker-name macro
  such as `ecomet_subscription_object_N`.
- Long-running servers keep state in records, while nested dynamic collections are
  mostly maps, `ordsets`, `gb_sets`, result sets, or ETS tables.
- State-transforming helper functions are grouped by domain sections, for example
  object subscriptions, query subscriptions, state transformations, notify, locks,
  and commit phases.
- Distributed coordination uses monitors, linked workers, tagged messages, and
  explicit phase comments.
- Logging macros are used consistently for unexpected messages and exceptional
  paths.

## Requirements

### ERL-001: Keep Application Boundaries Explicit

Each application must use a stable module-name prefix and expose a small public
surface. External callers should enter through explicit API modules or clearly marked
API functions, not through incidental helper modules.

Rationale: Chapter 20 recommends small external interfaces and low intermodule
dependency. The example modules already follow prefix naming.

### ERL-002: Group Exports by Role

Do not use a single mixed export block in nontrivial modules. Group exports by
purpose, using local section names such as:

- `API`
- `Transaction API`
- `Query API`
- `Remote API`
- `Internal API`
- `OTP API`

Every exported function must belong to one of these groups or to another clearly
named role.

### ERL-003: Hide Process and Message Protocols Behind Functions

Callers must not send raw messages to a process owned by another module. Expose a
function such as `unsubscribe/2`, `notify/1`, or `global_set/2` and keep the message
shape inside the owning module.

Direct `Pid ! Message` is allowed inside the module that owns the protocol, inside a
small distributed protocol implementation, or for a documented best-effort
notification path.

### ERL-004: Tag All Messages

All messages must be tagged with an atom or a structured tag. Avoid receive clauses
that match only broad unbound variables or untagged tuples.

Good:

```erlang
Receiver ! {write_batch, self(), Batch}.
```

Avoid:

```erlang
Receiver ! {self(), Batch}.
```

### ERL-005: Correlate Replies in Shared Mailboxes

When a process can have several in-flight requests or can receive similar replies from
multiple processes, include either a unique reference from `make_ref/0` or a monitored
worker pid in the protocol.

Use monitors or links to distinguish process failure from protocol timeout.

### ERL-006: Unexpected `handle_call/3` Must Reply

An unexpected `handle_call/3` must return `{reply, Reply, State}` or stop the server.
It must not return `{noreply, State}`, because the caller will wait until timeout.

Recommended default:

```erlang
handle_call(Request, From, State) ->
  ?LOGWARNING("unexpected call request ~p from ~p", [Request, From]),
  {reply, {error, {unexpected_call, Request}}, State}.
```

### ERL-007: Use Standard Return Shapes

Functions that may fail should return one of the standard shapes:

- `ok`
- `{ok, Result}`
- `{error, Reason}`

Functions that are expected to be total may return the direct value. Do not wrap direct
values in `{ok, Value}` unless the caller needs to distinguish success from failure.

### ERL-008: Let Internal Programming Errors Crash

Do not add defensive catchall clauses for data that should already be valid inside the
system. Validate data at external boundaries. For internal corruption, prefer a clear
crash with a useful stack trace.

Expected domain failures may still be represented explicitly, for example
`{error, Reason}` or a documented `throw(Reason)` caught at the API boundary.

### ERL-009: Keep `try`/`catch` Boundaries Small and Intentional

Use `try`/`catch` around a narrow operation whose failure policy is clear. Avoid broad
callback-wide catches unless the module deliberately preserves server state after
domain errors.

If a callback catches exceptions and continues with the old state, it must log the
exception and stack, and the module should document why continuing is safe.

### ERL-010: Restrict `throw/1` to Local Control Flow

`throw/1` may be used for nonlocal returns inside one module or one tightly coupled
operation. The corresponding catch should be nearby and should translate the throw
to the module's public return contract.

Do not expose raw throws as part of a public API unless the API explicitly documents
that convention.

### ERL-011: Isolate Dirty Erlang

The following are dirty or high-risk techniques and must be isolated and documented:

- process dictionary: `put/2`, `get/1`, `erase/1`
- `process_info/2`
- dynamic atom creation
- assumptions about record tuple layout
- broad `catch`
- direct use of internal ETS table layout by other modules

Example: `zaya_transaction` uses the process dictionary as transaction-local context.
That pattern should remain encapsulated in the transaction module and be documented
as intentional dirty code.

### ERL-012: Avoid Dynamic Atom Creation from External Data

Never call `list_to_atom/1` or `binary_to_atom/1` on unbounded external input.

Bounded internal atom creation is allowed only when the possible atom set is small and
known, for example worker names derived from a fixed pool size. Prefer existing atoms
or `list_to_existing_atom/1` where possible.

### ERL-013: Keep Internal Data Structures Private

Public APIs must not require callers to know whether internal state is a map, record,
ETS table, result set, or list. Provide constructors, readers, writers, and domain
functions.

Do not pattern match on record tuple representation. Use record syntax only.

### ERL-014: Prefer Records for Stable State Shapes

Use records for stable compound state such as server state, clients, subscriptions,
commit records, and protocol accumulators. Use maps for dynamic key/value
collections inside those records.

Place private records near the top of the module. Move shared records to include
files only when several modules are intended to use them.

### ERL-015: Keep One Main Process Role per Module

A module that implements a process loop or OTP server should own one main process
role. Client functions, callbacks, and state transformation helpers for that role should
stay together.

If a module owns several independent process roles, split it unless the protocol is
small and tightly coupled.

### ERL-016: Split Very Large Modules

Use 400 lines excluding comments as the normal review threshold. A larger module
must have a reason and clear internal sections.

When a module grows beyond the threshold, consider splitting by role:

- public API facade
- OTP server callbacks
- state transformations
- query/index logic
- notification delivery
- distributed protocol phase handling

Sample pressure points: `ecomet_subscription_object.erl`,
`ecomet_subscription_query.erl`, `zaya_transaction.erl`, and `zaya_copy.erl`.

### ERL-017: Keep Related Functions Together

Place public API functions near their export group. Place OTP callbacks together. Place
helper functions immediately after the callback or API section they serve, or in a
clearly named section.

For paired operations such as `start/stop`, `init/terminate`, `commit/rollback`,
`add/remove`, keep both sides close enough that readers can compare them.

### ERL-018: Prefer Pattern Matching Over Deep Nesting

Avoid deeply nested `case`, `if`, `receive`, and anonymous function bodies. Prefer:

- pattern matching in function heads
- pattern matching in `case`
- helper functions for separate decisions
- temporary tuples for related values

Two nested levels should be treated as a review warning.

### ERL-019: Use `case` Before `if` for Value Dispatch

Use `if` only for guard-style decisions. When branching on values, records, maps, or
tuples, prefer `case` or function-head pattern matching.

### ERL-020: Keep Formatting Consistent

Use the existing two-space indentation style in Erlang bodies. Keep spacing around
commas, arrows, and map updates consistent inside a module.

Target 80 characters per line. Treat lines over 100 characters as review findings unless
the long line is mechanically clearer than wrapping.

### ERL-021: Use Meaningful Names Without Long-Line Drift

Names should describe domain roles: `ClientID`, `SubsID`, `CopyRef`, `LockNodes`,
`CommitData`, `FieldsRef`.

Avoid near-duplicate names that are easy to confuse. If a function uses many
variables and their names start to feel confusing, treat this as a warning marker:
the function is probably doing too much and is a candidate for task decomposition or
splitting into smaller helper functions.

If names become very long, extract helper functions or temporary variables instead
of widening the line.

### ERL-022: Avoid Long Argument Lists

Functions with many arguments are hard to read, call, refactor, and pass through
helper chains. Treat more than five arguments as a warning marker.

Prefer a record or map contract when many values travel together, especially when
most of the arguments are passed through additional helper functions.

Prefer records for stable contracts because the IDE knows record fields and can help
the user navigate and complete them. Prefer maps when the structure is dynamic,
partially optional, externally shaped, or too flexible for a record.

### ERL-023: Name Unused Variables When They Explain the Shape

Use `_Reason`, `_State`, `_From`, `_Options`, or `_IsActive` when the ignored value
helps the reader understand the clause. Use bare `_` only when the value truly carries
no information.

Do not reuse a named underscore variable in the same pattern scope as if it were a
wildcard. `_Name` is still a normal Erlang variable.

### ERL-024: Document Exported Interfaces

Every exported function should have either:

- a `-spec`, or
- a short comment documenting parameters, return values, side effects, and failure
  behavior.

Prefer `-spec` for public APIs, remote APIs, callback-style APIs, and functions with
non-obvious return contracts.

### ERL-025: Comments Should Explain What Nearby Code Does Not

Keep section banners for navigation in large modules. Comments should explain what
is not obvious from the nearby code, especially non-obvious solutions and assumptions.
They may describe:

- protocol phases
- concurrency assumptions
- why an exception is caught
- why a process continues with old state
- why dirty code is acceptable
- invariants of maps, records, ETS tables, and locks
- example structures for complex arguments, maps, messages, and return values

Remove obsolete debug snippets and commented-out code.

### ERL-026: Keep Side Effects at Clear Boundaries

Prefer pure state transformation helpers that take state and return new state. Keep
side effects such as ETS writes, persistent term updates, remote calls, direct process
sends, logging, and database writes in clearly named boundary functions.

When side effects and state transformation must be combined, make the order explicit.

### ERL-027: Treat ETS and `persistent_term` as Owned Storage

The module that creates an ETS table or `persistent_term` key owns its layout. Other
modules should read it only through exported functions unless the table is explicitly
documented as shared.

`persistent_term` should be reserved for rarely changing values. Use it carefully for
frequently updated state.

### ERL-028: Prefer Deterministic Concurrency

When starting, copying, committing, or coordinating across processes, prefer a
deterministic protocol:

- start or confirm phases explicitly
- monitor participants
- handle down messages
- tag phase messages
- keep rollback paths visible

Avoid protocols that depend on mailbox order unless the ordering assumption is
documented and enforced.

### ERL-029: Flush or Correlate Late Timeout Replies

If a protocol uses timeouts, late replies must not be able to satisfy a later request.
Use unique references, monitored workers, or an explicit flush strategy.

For process death detection, prefer monitors or links over timeouts.

### ERL-030: Avoid Premature Optimization

First make the code correct and readable. Optimize only after measurement.

Efficiency rules that are valid without measurement:

- avoid appending single elements with `List ++ [Item]` in loops
- avoid unnecessary `lists:flatten/1`
- prefer iolists for output construction
- use binaries for large messages or socket/port data
- avoid dynamic atom creation

### ERL-031: Add Tests Around Protocol and State Invariants

State-heavy modules and distributed protocols should have tests for:

- duplicate subscription IDs
- client down cleanup
- add/remove symmetry
- notification filtering
- lock release on errors
- commit rollback paths
- timeout or participant-down behavior

The style guide should not require exhaustive tests for every helper, but it should
require tests around invariants that are hard to recover manually.

## Recommendations

### REC-001: Prefer One Public Facade per Application Area

For a cluster of modules, consider a small facade module for external callers, with
worker, index, pool, and supervisor modules treated as internal implementation.

### REC-002: Separate API, Callback, and Pure Logic in Large Modules

For modules over the line-count threshold, consider extracting pure logic into helper
modules before changing behavior. This makes tests cheaper and OTP callbacks
smaller.

### REC-003: Prefer Explicit Domain Errors to Generic Atoms

Use errors such as `{not_unique_subscription, SubsID}`, `{unavailable, DB}`, or
`{read_only, DB}` rather than generic `error` or `badarg`, unless the failure is a true
programming error.

### REC-004: Normalize Empty Results

Within one API area, use one empty result convention consistently: `[]`, `{ok, []}`,
`ignore`, or `ok`. Do not mix them unless each value has a distinct meaning.

### REC-005: Keep Logging Context Structured

When logging errors, include the operation, key identifiers, reason, and stack where
available. Avoid logging only the error atom in state-heavy code.

### REC-006: Prefer `erlang:send_after/3` Over `timer:send_after/2`

For repeated server timers, prefer `erlang:send_after/3` unless the `timer` module is
needed for a specific reason. The `timer` server can become a bottleneck under load.

### REC-007: Keep Debug Helpers Out of Production APIs

Functions such as `debug/2`, `fill/2`, or `get_hash/1` should live in test/support
modules or be clearly marked as diagnostic-only exports.

### REC-008: Prefer Small Reviewable Refactors

When applying this guide to existing modules, do not mix style cleanup with behavior
changes. For large modules, first add tests or characterization checks, then split or
reformat.
