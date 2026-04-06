# Scalar Index Streaming Load — Follow-up TODOs

## BitmapIndex: frozenView unification
- Non-mmap path currently deserializes roaring bitmaps into `data_[key]` (full copy)
- Could instead `writeFrozen()` into a contiguous heap buffer + `frozenView()` (zero-copy)
- Same pattern as StringIndexSort's MmapImpl + owned_data_ unification
- Eliminates `is_mmap_` branching in all query methods (In, NotIn, Range, etc.)
- Low priority: BitmapIndex is only used for low cardinality (<500), typical size 3-10MB

## BitmapIndex: resource estimation (max mem vs final mem)
- Non-mmap load peak = ~2x index size (buffer + deserialized roaring structures)
- Steady state = ~1x (buffer released after deserialization)
- IndexFactory resource estimation should reflect this for accurate memory planning
- Related: ScalarIndexSort and StringIndexSort estimates may also need updating

## V3 LoadEntries streaming
- V3 path uses `IndexEntryReader::ReadEntry()` which allocates full entry buffer
- mmap path could use `ReadEntryToFile()` to stream directly to disk (0 extra memory)
- Currently entry buffer + impl construction = 2x for mmap path (StringIndexSort, ScalarIndexSort)
- Requires no IndexEntryReader interface changes — `ReadEntryToFile` already exists

## mmap mode: mmap idx_to_offsets_ to reduce resident memory
- `idx_to_offsets_` is `vector<int32_t>`, size = total_num_rows * 4 bytes (30M rows = 120MB)
- Only used by `Reverse_Lookup` (groupby / output fields), low frequency access
- In mmap mode the index data is already mmap'd, but idx_to_offsets_ stays fully resident
- Fix: after rebuilding idx_to_offsets_, write to local file and mmap it back
- Kernel can evict unused pages; most pages likely never touched
- Applies to both ScalarIndexSort and StringIndexSort (and MmapImpl)

## StringIndexMarisa: trie always requires disk
- marisa::read() only accepts fd, no memory-buffer API
- Non-mmap mode still does: download → write to disk → read fd → delete file
- Could explore marisa::map() if applicable, or accept disk round-trip as library limitation
