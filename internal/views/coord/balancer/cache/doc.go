// Package cache maintains the resident read model consumed by query-view
// balancing. Sources synchronously publish committed state; readers retain
// immutable objects without requiring a globally consistent snapshot.
//
// Objects returned by Reader and everything reachable from them are read-only.
// Publication callbacks must not perform I/O or re-enter upstream managers.
//
// Scanner visits an append-ordered collection directory in bounded batches.
// A round retains the directory slice, while publications remain live. Deletion
// marks shared slots; asynchronous compaction replaces the directory without
// moving active scanners. io.EOF ends a round and releases its directory.
package cache
