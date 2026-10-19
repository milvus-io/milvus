# DDL Package

The `ddl` package owns the proxy's **DDL (data definition language)** task
implementations: collection, partition, index, snapshot, database, alias,
flush, import and resource-group tasks. It was extracted from the proxy root
package (issue #44761) as the third step of the proxy task-package split, after
`dql` and `dml`.

The package is the leaf of the proxy task split: it imports `taskmodel`,
`metacache`, `fieldvalidator`, `channelmgr`, `types` and `pkg/v3`, but never the
proxy root package. Root builds DDL tasks through the exported `NewXxxTask`
constructors, which derive everything from the `taskmodel.TaskNode` contract;
results are read through `Result()` getters. The only root-owned seams are:

- `CheckVecIndexWithDataTypeExist` (cgo) is injected as a field on
  `CreateIndexTask` / `AlterCollectionSchemaTask`.
- Content-driven privilege checks (`CheckManageRLSPrivilege`,
  `CheckClusterPrivilege`) and RLS enforcement route through the host node's
  `taskmodel.TaskNode`, keeping the authorization machinery in root.

## Overview

DDL requests arrive at the proxy's gRPC/REST handlers, which construct a task
via the exported constructors, enqueue it on the scheduler's `DdQueue` (or
`DmQueue` for import), and wait for the result. Each task validates its request
in `PreExecute`, issues the coordinators' RPC in `Execute`, and exposes the
result through `Result()`/`Request()` accessors.

The package also owns the describe-collection projection helpers
(`DescribeCollectionErrorStatus`, `ProjectDescribeCollectionSchema`,
`DescribeCollectionRPCContext`) and the DDL util closure (name/field validators,
collection/partition load checks, partition-key mode helpers).

## Key Packages / Files

- `task.go` — collection/partition/load/release and resource-group tasks.
- `task_index.go` — index tasks + `checkTrain` + injected cgo check.
- `task_snapshot.go` — snapshot tasks.
- `task_database.go`, `task_alias.go`, `task_flush*.go`, `task_import.go`,
  `function_task.go` — the remaining DDL task families.
- `task_constructors.go` — exported constructors + `Result()`/`Request()`
  accessors for the root composition root.
- `util_ddl.go` — the duplicated DDL util closure.
- `describe_util.go` — describe-collection projection helpers.
