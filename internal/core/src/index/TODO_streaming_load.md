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

## StringIndexMarisa: trie always requires disk
- marisa::read() only accepts fd, no memory-buffer API
- Non-mmap mode still does: download → write to disk → read fd → delete file
- Could explore marisa::map() if applicable, or accept disk round-trip as library limitation
