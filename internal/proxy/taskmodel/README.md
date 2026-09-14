# TaskModel Package

The `taskmodel` package is the shared task model layer of the Milvus proxy. It
defines the interfaces and value types that decouple the task scheduler from
the concrete task implementations that remain in the `internal/proxy` root
package.

This package was extracted from the proxy root package (issue #44761): the
`task` interface, `baseTask`, the `Condition`/`TaskCondition` primitives, the
TSO allocator interface, and the channel/timestamp value types moved here
verbatim. The concrete task structs (search/query/insert/delete/upsert and the
DDL tasks) still live in the root package and implement these interfaces.

## Overview

The proxy's scheduler consumes the `Task` / `DMLTask` interfaces, so it never
needs to know about the concrete task types. That one-way dependency
(`scheduler -> taskmodel`) is what allowed the scheduler to be extracted into
its own package without dragging every task implementation along.

## Responsibilities

1. **`Task`** — the contract every proxy task implements (lifecycle:
   `PreExecute`/`Execute`/`PostExecute`, timing bookkeeping, `GetMetaCache`,
   `WaitToFinish`/`Notify`).
2. **`DMLTask`** — the `Task` variant for insert/delete/upsert tasks, which
   resolve their physical channels (`SetChannels`/`GetChannels`) before
   enqueueing.
3. **`BaseTask`** — embedded by concrete tasks to provide the shared meta-cache
   accessor and queue/execute timing fields.
4. **`Condition` / `TaskCondition`** — the notification primitive tasks embed to
   implement `WaitToFinish`/`Notify`/`Ctx`.
5. **`TsoAllocator`** — the timestamp-allocation interface implemented by the
   proxy's timestamp allocator and consumed by the scheduler.
6. **Value types** — `UniqueID`, `Timestamp`, `VChan`, `PChan`,
   `PChanStatistics`, and the `BaseInsertTask` alias.

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                        taskmodel                           │
│                                                            │
│   Task  ◄─────────────  DMLTask                            │
│    ▲                     ▲                                 │
│    │ embeds              │ embeds                          │
│   BaseTask ── Condition / TaskCondition                    │
│                                                            │
│   TsoAllocator    UniqueID / Timestamp / VChan / PChan     │
│   PChanStatistics  BaseInsertTask                          │
└────────────────────────────────────────────────────────────┘
```

### Key types

```go
type Task interface {
    TraceCtx() context.Context
    ID() UniqueID
    SetID(uid UniqueID)
    Name() string
    Type() commonpb.MsgType
    BeginTs() Timestamp
    EndTs() Timestamp
    SetTs(ts Timestamp)
    OnEnqueue() error
    PreExecute(ctx context.Context) error
    Execute(ctx context.Context) error
    PostExecute(ctx context.Context) error
    WaitToFinish() error
    Notify(err error)
    CanSkipAllocTimestamp() bool
    GetMetaCache() metacache.Cache
    SetOnEnqueueTime()
    GetDurationInQueue() time.Duration
    IsSubTask() bool
    SetExecutingTime()
    GetDurationInExecuting() time.Duration
}

type DMLTask interface {
    Task
    SetChannels() error
    GetChannels() []PChan
}

type TsoAllocator interface {
    AllocOne(ctx context.Context) (Timestamp, error)
}
```

## Dependency rule

`taskmodel` imports only `metacache` (for the `Cache` type), `msgstream` (for
the `BaseInsertTask` alias), the proto packages, and `pkg/v3`. It never imports
the proxy root package, so concrete tasks can implement these interfaces
without introducing an import cycle.

## Related components

- **scheduler** (`internal/proxy/scheduler/`): the sole consumer of `Task` /
  `DMLTask` / `TsoAllocator`.
- **proxy root** (`internal/proxy/`): keeps the ~30 concrete task structs,
  which embed `BaseTask` and `Condition` and return the shared task-name
  constants.
- **metacache** (`internal/proxy/metacache/`): provides the `Cache` interface
  exposed through `Task.GetMetaCache()`.
