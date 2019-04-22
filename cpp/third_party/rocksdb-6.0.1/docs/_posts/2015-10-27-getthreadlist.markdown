---
title: GetThreadList
layout: post
author: yhciang
category: blog
redirect_from:
  - /blog/2261/getthreadlist/
---

We recently added a new API, called `GetThreadList()`, that exposes the RocksDB background thread activity. With this feature, developers will be able to obtain the real-time information about the currently running compactions and flushes such as the input / output size, elapsed time, the number of bytes it has written. Below is an example output of `GetThreadList`.  To better illustrate the example, we have put a sample output of `GetThreadList` into a table where each column represents a thread status:

<!--truncate-->

<table width="637" >
<tbody >
<tr style="border:2px solid #000000" >

<td style="padding:3px" >ThreadID
</td>

<td >140716395198208
</td>

<td >140716416169728
</td>
</tr>
<tr >

<td style="padding:3px" >DB
</td>

<td >db1
</td>

<td >db2
</td>
</tr>
<tr >

<td style="padding:3px" >CF
</td>

<td >default
</td>

<td >picachu
</td>
</tr>
<tr >

<td style="padding:3px" >ThreadType
</td>

<td >High Pri
</td>

<td >Low Pri
</td>
</tr>
<tr >

<td style="padding:3px" >Operation
</td>

<td >Flush
</td>

<td >Compaction
</td>
</tr>
<tr >

<td style="padding:3px" >ElapsedTime
</td>

<td >143.459 ms
</td>

<td >607.538 ms
</td>
</tr>
<tr >

<td style="padding:3px" >Stage
</td>

<td >FlushJob::WriteLevel0Table
</td>

<td >CompactionJob::Install
</td>
</tr>
<tr >

<td style="vertical-align:top;padding:3px" >OperationProperties
</td>

<td style="vertical-align:top;padding:3px" >
BytesMemtables 4092938
BytesWritten 1050701
</td>

<td style="vertical-align:top" >
BaseInputLevel 1
BytesRead 4876417
BytesWritten 4140109
IsDeletion 0
IsManual 0
IsTrivialMove 0
JobID 146
OutputLevel 2
TotalInputBytes 4883044
</td>
</tr>
</tbody>
</table>

In the above output, we can see `GetThreadList()` reports the activity of two threads: one thread running flush job (middle column) and the other thread running a compaction job (right-most column).  In each thread status, it shows basic information about the thread such as thread id, it's target db / column family, and the job it is currently doing and the current status of the job.  For instance, we can see thread 140716416169728 is doing compaction on the `picachu` column family in database `db2`.  In addition, we can see the compaction has been running for 600 ms, and it has read 4876417 bytes out of 4883044 bytes. This indicates the compaction is about to complete.  The stage property indicates which code block the thread is currently executing.  For instance, thread 140716416169728 is currently running `CompactionJob::Install`, which further indicates the compaction job is almost done.

Below we briefly describe its API.


## How to Enable it?


To enable thread-tracking of a rocksdb instance, simply set `enable_thread_tracking` to true in its DBOptions:

```c++
// If true, then the status of the threads involved in this DB will
// be tracked and available via GetThreadList() API.
//
// Default: false
bool enable_thread_tracking;
```



## The API


The GetThreadList API is defined in [include/rocksdb/env.h](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/env.h#L317-L318), which is an Env
function:

```c++
virtual Status GetThreadList(std::vector* thread_list)
```

Since an Env can be shared across multiple rocksdb instances, the output of
`GetThreadList()` include the background activity of all the rocksdb instances
that using the same Env.

The `GetThreadList()` API simply returns a vector of `ThreadStatus`, each describes
the current status of a thread. The `ThreadStatus` structure, defined in
[include/rocksdb/thread_status.h](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/thread_status.h), contains the following information:

```c++
// An unique ID for the thread.
const uint64_t thread_id;

// The type of the thread, it could be HIGH_PRIORITY,
// LOW_PRIORITY, and USER
const ThreadType thread_type;

// The name of the DB instance where the thread is currently
// involved with. It would be set to empty string if the thread
// does not involve in any DB operation.
const std::string db_name;

// The name of the column family where the thread is currently
// It would be set to empty string if the thread does not involve
// in any column family.
const std::string cf_name;

// The operation (high-level action) that the current thread is involved.
const OperationType operation_type;

// The elapsed time in micros of the current thread operation.
const uint64_t op_elapsed_micros;

// An integer showing the current stage where the thread is involved
// in the current operation.
const OperationStage operation_stage;

// A list of properties that describe some details about the current
// operation. Same field in op_properties[] might have different
// meanings for different operations.
uint64_t op_properties[kNumOperationProperties];

// The state (lower-level action) that the current thread is involved.
const StateType state_type;
```

If you are interested in the background thread activity of your RocksDB application, please feel free to give `GetThreadList()` a try :)
