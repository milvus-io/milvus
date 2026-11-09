# Storage-v2 field-data reads

Selected row groups are read in contiguous byte windows; each window's parallel
readers are drained and joined before the next starts. The default budget is
`DEFAULT_FIELD_MAX_MEMORY_LIMIT`, charged by the column-group file's stored Arrow
byte estimate for all columns. Oversized or unknown-size row groups run alone;
reader/decoder buffers and data retained by the visitor add to this estimate.

Before resolving a field's column group, the shared reader sorts each group's
binlogs by numeric log ID. The complete basename must parse as an `int64_t`,
including for a single-file group; malformed names raise `DataFormatBroken`.
This boundary covers BuildSession's primary and optional fields, cache reads,
and the text/JSON stats build paths. Dense disk-cache pagination sorts each
complete group with the same helper before splitting it into single-file
reads; each subsequent page validates its single basename without sorting
again. Manifest reads use separate visitors and retain the file order
described by their manifest.
