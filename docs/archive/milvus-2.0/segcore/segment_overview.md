# Segment Overview

There are currently two types of Segments
1. Growing segment, dynamic insert is allowed, but can not load index for fast retrieving
2. Sealed segment, dynamic insert is disabled, loading vector index is supported

Both Segment types share the same interface, based on `SegmentInterface`, External callers only need to care about the behavior of the following interface as function declarations and corresponding constructor:

1. `SegmentInterface`
2. `SegmentGrowing` & `CreateGrowingSegment`
3. `SegmentSealed` & `CreateSealedSegment`

Other internal functions are hidden as implementation details in the following classes:

1. `SegmentInternalInterface`
2. `SegmentGrowingImpl`
3. `SegmentSealedImpl`

In principle, the reusable code logic of growing / sealed is written into the 'SegmentInternalInterface' as far as possible. The different parts of the two classes contain more different parts

See more details about segments at:

1. [segment_interface.md](segment_interface.md)
2. [segment_growing.md](segment_growing.md)
3. [segment_sealed.md](segment_sealed.md)
