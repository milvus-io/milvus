# Storage-v2 field-data reads

`VisitFieldDataFromStorageV2` and `GetFieldDatasFromStorageV2` share the paged
implementation in `Util.cpp`. For each file it submits one `LoadWithStrategy`
call covering all selected row groups. `ParallelDegreeSplitStrategy` uses
`DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE` workers, and the output
channel has that same capacity.

Before resolving a field's column group, the shared reader sorts each group's
binlogs by numeric log ID. The complete basename must parse as an `int64_t`,
including for a single-file group; malformed names raise `DataFormatBroken`.
This boundary covers BuildSession's primary and optional fields, cache reads,
and the text/JSON stats build paths. Dense disk-cache pagination sorts each
complete group with the same helper before splitting it into single-file
reads; each subsequent page validates its single basename without sorting
again. Manifest reads use separate visitors and retain the file order
described by their manifest.

The strategy sorts row groups into adjacent blocks. `LoadWithStrategy` consumes
block futures in their original order, and each block reader returns row groups
in increasing order. The caller consumes the channel and invokes the visitor
on its own thread, preserving numeric binlog order and row order within each
file. One visit can contain several
adjacent row groups.

`Stop` ends visitor delivery. Reads already submitted for the current file can
finish; they are drained without visiting their results. A row budget selects
whole row groups starting with the group containing `offset`, and delivery ends
once the budget is reached. A page can therefore include rows before `offset`
or exceed its row budget at a group or block boundary.

On `Stop`, a full page, or a visitor/decoding exception, cleanup first consumes
the channel through its close marker, then calls `DrainFuture`. This order lets
a producer blocked on the bounded channel finish. Normal exhaustion calls
`get()` to propagate producer failures. Cleanup preserves an already propagating
visitor/decoding exception.
