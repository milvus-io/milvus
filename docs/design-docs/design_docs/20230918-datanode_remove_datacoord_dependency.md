# MEP: Datanode remove dependency of `Datacoord`

Current state: "Accepted" 

ISSUE: https://github.com/milvus-io/milvus/issues/26758

Keywords: datacoord, datanode, flush, dependency, roll-upgrade

## Summary

Remove the dependency of `Datacoord` for `Datanodes`.

## Motivation

1. Datanodes shall be always be running even when the data coordinator is not alive

If datanodes performs `sync` during rolling upgrade, it needs datacoord to change the related meta in metastore. If datacoord happens to be offline or it is during some period of rolling-upgrade, datanode has to panic to ensure there is no data lost.

2. Flush operation is complex and error-prone due since the whole procedure involves datacoord, datanodes and grpc

This proposal means to remove the dependency of datacoord ensuring:

- the data is integrate and no duplicate data is kept in records
- no compatibility issue during or after rolling upgrade
- `Datacoord` shall be able to detect the segment meta updates and provides recent targets for `QueryCoord`

## Design Details

The most brief description if this proposal is to:

- Make `Datanode` operating the segment meta directly
- Make `Datacoord` refresh the latest segment change periodically


### Preventing multiple writers

There is a major concern that if multiple `Datanodes` are handling the same dml channel, there shall be only one `DataNode` could update segment meta successfully.

This guarantee is previously implemented by singleton writer in `Datacoord`: it checks the valid watcher id before update the segment meta when receiving the `SaveBinlogPaths` grpc call.

In this proposal, `DataNodes` update segment meta on its own, so we need to introduce a new mechanism to prevent this error from happening:

{% note %}

**Note:** Like the "etcd lease for key", the ownership of each dml channel is bound to a lease id. This lease id shall be recorded in metastore (etcd/tikv or any other implementation).
When a `DataNode` start to watch a dml channel, it shall read this lease id (via etcd or grpc call). ANY operations on this dml channel shall under a transaction with the lease id is equal to previously read value.
If a `datanode` finds the lease id is revoke or updated, it shall close the flowgraph/pipeline and cancel all pending operations instead of panicking.

{% endnote %}

- [] Add lease id field in etcd channel watch info/ grpc watch request
- [] Add `TransactionIf` like APIs in `TxnKV` interface

### Updating channel checkpoint

Likewise, all channel checkpoints update operations are performed by `Datacoord` invoking by grpc calls from `DataNodes`. So it has the same problem in previously stated scenarios.

So, "updating channel checkpoint" shall also be processed in `DataNodes` while removing the dependency of `DataCoord`.

The rules system shall follow is:

{% note %}

**Note:** Segments meta shall be updated *BEFORE* changing the channel checkpoint in case of datanode crashing during the prodedure. Under this premise, reconsuming from the old checkpoint shall recover all the data and duplidated entries will be discarded by segment checkpoints.

{% endnote %}

### Updating segment status in `DataCoord`

As previous described, `DataCoord` shall refresh the segment meta and channel checkpoint periodically to provide recent target for `QueryCoord`.

The `watching via Etcd` strategy is ruled out first since `Watch` operation shall avoided in the future design: currently Milvus system tends to not use `Watch` operation and try to remove it from metastore.
Also `Watch` is heavy and has caused lots of issue before.

The winning option is to:

{% note %}

**Note:** `Datacoord` reloads from metastore periodically.
Optimization 1: reload channel checkpoint first, then reload segment meta if newly read revision is greater than in-memory one.
Optimization 2: After `L0 segment` is implemented, datacoord shall refresh growing segments only.

{% endnote %}


## Compatibility, Deprecation, and Migration Plan

This change shall guarantee that:

- When new `Datacoord` starts, it shall be able to upgrade the old watch info and add lease id into it
    - For watch info, release then watch
    - For grpc, `release then watch` is the second choice, try call watch with lease id
- Older `DataNodes` could invoking `SaveBinlogPaths` and other legacy grpc calls without panicking
- The new `DataNodes` receiving old watch request(without lease id) shall fallback to older strategy, which is to update meta via grpc
- `SaveBinlogPaths`, `UpdateChannelCheckpoints` APIs shall be kept until next break change

## Test Plan 

### Unit test
Coverage over 90%

### Integration Test

#### Datacoord offline

1. Insert data without datanodes online
2. Start datanodes
3. Make datacoord go offline after channel assignment
4. Assert no datanode panicking and all data shall be intact
5. Bring back datacoord and test `GetRecoveryInfo`, which shall returns latest target


#### Compatibility

1. Start mock datacoord
2. construct a watch info (without lease)
3. Datanode start to watch dml channel and all meta update shall be performed via grpc

## Rejected Alternatives

DataCoord refresh meta via Etcd watch
