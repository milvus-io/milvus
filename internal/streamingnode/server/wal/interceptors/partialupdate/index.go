package partialupdate

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const (
	defaultVersionIndexTTL = 30 * time.Second

	// The estimate covers the map key/value, heap pointer, entry object, and
	// allocator overhead. String backing bytes are accounted separately.
	estimatedVersionEntryFixedBytes int64 = 128
	defaultVersionIndexBudgetBytes        = 5_000_000 * estimatedVersionEntryFixedBytes
)

// pkVersionIndex owns one byte budget for a PChannel term and isolates mutable
// PK history by vchannel.
type pkVersionIndex struct {
	ttl      time.Duration
	budget   *versionByteBudget
	channels sync.Map // map[string]*vchannelPKVersionIndex
}

// versionByteBudget bounds the estimated memory retained by all vchannels in
// one PChannel term.
type versionByteBudget struct {
	limit int64
	used  atomic.Int64
}

func newVersionByteBudget(limit int64) *versionByteBudget {
	if limit < 0 {
		limit = 0
	}
	return &versionByteBudget{limit: limit}
}

func (b *versionByteBudget) tryReserve(bytes int64) bool {
	if bytes <= 0 {
		return true
	}
	for {
		used := b.used.Load()
		if bytes > b.limit-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

func (b *versionByteBudget) release(bytes int64) {
	if bytes <= 0 {
		return
	}
	if remaining := b.used.Add(-bytes); remaining < 0 {
		panic("partial update version index byte budget underflow")
	}
}

// vchannelPKVersionIndex tracks recent PK writes on one vchannel.
type vchannelPKVersionIndex struct {
	mu                sync.Mutex
	ttl               time.Duration
	budget            *versionByteBudget
	retainedSinceTS   uint64
	lastMissedWriteTS uint64
	versions          map[any]*versionEntry
	expirations       versionMinHeap
}

type versionEntry struct {
	pk             any
	commitTS       uint64
	estimatedBytes int64
	index          int
}

type versionMinHeap []*versionEntry

func (h versionMinHeap) Len() int {
	return len(h)
}

func (h versionMinHeap) Less(i, j int) bool {
	return h[i].commitTS < h[j].commitTS
}

func (h versionMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *versionMinHeap) Push(value any) {
	entry := value.(*versionEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *versionMinHeap) Pop() any {
	old := *h
	last := len(old) - 1
	entry := old[last]
	old[last] = nil
	entry.index = -1
	*h = old[:last]
	return entry
}

// newPKVersionIndex creates a byte-bounded index for one PChannel term.
func newPKVersionIndex(ttl time.Duration, maxBytes int64) *pkVersionIndex {
	return &pkVersionIndex{
		ttl:    ttl,
		budget: newVersionByteBudget(maxBytes),
	}
}

func (idx *pkVersionIndex) channel(vchannel string) *vchannelPKVersionIndex {
	if existing, ok := idx.channels.Load(vchannel); ok {
		return existing.(*vchannelPKVersionIndex)
	}
	created := &vchannelPKVersionIndex{
		ttl:      idx.ttl,
		budget:   idx.budget,
		versions: make(map[any]*versionEntry),
	}
	actual, _ := idx.channels.LoadOrStore(vchannel, created)
	return actual.(*vchannelPKVersionIndex)
}

// Remove releases all retained PK-version state for a terminal vchannel.
// DropCollection serializes this call with writes through the vchannel lock.
func (idx *pkVersionIndex) Remove(vchannel string) {
	value, ok := idx.channels.LoadAndDelete(vchannel)
	if !ok {
		return
	}
	value.(*vchannelPKVersionIndex).clear()
}

func (idx *vchannelPKVersionIndex) clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var releasedBytes int64
	for _, entry := range idx.versions {
		releasedBytes += entry.estimatedBytes
	}
	idx.versions = make(map[any]*versionEntry)
	idx.expirations = nil
	idx.retainedSinceTS = 0
	idx.lastMissedWriteTS = 0
	idx.budget.release(releasedBytes)
}

// UpdateAll records one committed write batch atomically within its vchannel.
func (idx *pkVersionIndex) UpdateAll(vchannel string, pks []any, commitTS uint64) {
	candidates := make([]versionCandidate, 0, len(pks))
	for _, pk := range pks {
		candidate, err := newVersionCandidate(pk)
		if err != nil {
			// All live and recovered writes validate PK types before publication.
			panic(err)
		}
		candidates = append(candidates, candidate)
	}
	idx.channel(vchannel).updateAll(candidates, commitTS)
}

func (idx *vchannelPKVersionIndex) updateAll(candidates []versionCandidate, commitTS uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.advanceRetentionLocked(commitTS)
	for _, candidate := range candidates {
		entry, ok := idx.versions[candidate.pk]
		if ok && entry.commitTS >= commitTS {
			continue
		}
		if ok {
			entry.commitTS = commitTS
			heap.Fix(&idx.expirations, entry.index)
			continue
		}
		if commitTS < idx.retainedSinceTS {
			continue
		}
		if !idx.budget.tryReserve(candidate.estimatedBytes) {
			// Once a committed write is omitted, exact conflict detection is
			// incomplete until that write leaves every valid read window.
			if commitTS > idx.lastMissedWriteTS {
				idx.lastMissedWriteTS = commitTS
			}
			continue
		}
		entry = &versionEntry{
			pk:             candidate.pk,
			commitTS:       commitTS,
			estimatedBytes: candidate.estimatedBytes,
		}
		idx.versions[candidate.pk] = entry
		heap.Push(&idx.expirations, entry)
	}
}

// Advance moves retention forward without publishing a PK write. TimeTick
// messages use it to reclaim memory and restore complete read windows while idle.
func (idx *pkVersionIndex) Advance(currentTS uint64) {
	idx.channels.Range(func(_, value any) bool {
		value.(*vchannelPKVersionIndex).advance(currentTS)
		return true
	})
}

func (idx *vchannelPKVersionIndex) advance(currentTS uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.advanceRetentionLocked(currentTS)
}

// Verify rejects reads whose retained PK version changed after readTS.
func (idx *pkVersionIndex) Verify(vchannel string, pks []any, readTS, commitTS uint64) error {
	return idx.channel(vchannel).verify(vchannel, pks, readTS, commitTS)
}

func (idx *vchannelPKVersionIndex) verify(vchannel string, pks []any, readTS, commitTS uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.advanceRetentionLocked(commitTS)
	if err := idx.verifyReadWindowLocked(readTS, commitTS); err != nil {
		return err
	}

	for _, pk := range pks {
		candidate, err := newVersionCandidate(pk)
		if err != nil {
			return err
		}
		if entry := idx.versions[candidate.pk]; entry != nil && entry.commitTS > readTS {
			return status.NewPartialUpdateRetryable("partial update pk conflict, vchannel: %s, read ts: %d, last commit ts: %d", vchannel, readTS, entry.commitTS)
		}
	}
	return nil
}

func (idx *vchannelPKVersionIndex) verifyReadWindowLocked(readTS, commitTS uint64) error {
	if readTS < idx.retainedSinceTS {
		return status.NewPartialUpdateRetryable("partial update read ts %d is older than retained since ts %d", readTS, idx.retainedSinceTS)
	}
	if commitTS < readTS {
		return status.NewUnrecoverableError("partial update commit ts %d is older than read ts %d", commitTS, readTS)
	}
	if time.Duration(tsoutil.CalculateDuration(commitTS, readTS))*time.Millisecond > idx.ttl {
		return status.NewPartialUpdateRetryable("partial update read window exceeds max age, read ts: %d, commit ts: %d, max age: %s", readTS, commitTS, idx.ttl)
	}
	if idx.lastMissedWriteTS != 0 {
		return status.NewPartialUpdateRetryable("partial update version index is incomplete after missing write ts %d", idx.lastMissedWriteTS)
	}
	return nil
}

// advanceRetentionLocked evicts only versions older than the maximum valid
// read window. Moving retainedSinceTS with the same cutoff preserves fail-closed
// verification for reads whose conflict history has been discarded.
func (idx *vchannelPKVersionIndex) advanceRetentionLocked(commitTS uint64) {
	evictUntilTS := retentionCutoffTS(commitTS, idx.ttl)
	if evictUntilTS == 0 {
		return
	}
	if evictUntilTS <= idx.retainedSinceTS {
		return
	}
	idx.retainedSinceTS = evictUntilTS
	idx.sweepLocked(evictUntilTS)
	if idx.lastMissedWriteTS != 0 && idx.lastMissedWriteTS <= evictUntilTS {
		idx.lastMissedWriteTS = 0
	}
}

func retentionCutoffTS(ts uint64, ttl time.Duration) uint64 {
	physical, logical := tsoutil.ParseHybridTs(ts)
	cutoffPhysical := physical - ttl.Milliseconds()
	if cutoffPhysical <= 0 {
		return 0
	}
	return tsoutil.ComposeTS(cutoffPhysical, logical)
}

func (idx *vchannelPKVersionIndex) sweepLocked(evictUntilTS uint64) {
	for idx.expirations.Len() > 0 && idx.expirations[0].commitTS <= evictUntilTS {
		entry := heap.Pop(&idx.expirations).(*versionEntry)
		delete(idx.versions, entry.pk)
		idx.budget.release(entry.estimatedBytes)
	}
}

type versionCandidate struct {
	pk             any
	estimatedBytes int64
}

func newVersionCandidate(pk any) (versionCandidate, error) {
	switch value := pk.(type) {
	case int64:
		return versionCandidate{pk: value, estimatedBytes: estimatedVersionEntryFixedBytes}, nil
	case string:
		return versionCandidate{
			pk:             value,
			estimatedBytes: estimatedVersionEntryFixedBytes + int64(len(value)),
		}, nil
	default:
		return versionCandidate{}, status.NewUnrecoverableError("partial update pk must be int64 or string")
	}
}

// collectionFenceIndex records collection-wide writes that cannot be
// represented as exact PK updates. Fences live for the collection lifetime
// within one PChannel term and are removed explicitly on DropCollection.
type collectionFenceIndex struct {
	mu     sync.RWMutex
	fences map[collectionFenceKey]uint64
}

type collectionFenceKey struct {
	vchannel     string
	collectionID int64
}

// newCollectionFenceIndex creates an empty collection fence index.
func newCollectionFenceIndex() *collectionFenceIndex {
	return &collectionFenceIndex{
		fences: make(map[collectionFenceKey]uint64),
	}
}

// Update advances the collection fence monotonically.
func (idx *collectionFenceIndex) Update(vchannel string, collectionID int64, ts uint64) {
	if collectionID == 0 {
		return
	}
	key := collectionFenceKey{vchannel: vchannel, collectionID: collectionID}
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if ts > idx.fences[key] {
		idx.fences[key] = ts
	}
}

// Remove deletes the fence for a collection after DropCollection is durable.
func (idx *collectionFenceIndex) Remove(vchannel string, collectionID int64) {
	if collectionID == 0 {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.fences, collectionFenceKey{vchannel: vchannel, collectionID: collectionID})
}

// Verify rejects reads older than a collection-wide invalidation.
func (idx *collectionFenceIndex) Verify(vchannel string, collectionID int64, readTS uint64) error {
	if collectionID == 0 {
		return status.NewUnrecoverableError("partial update collection fence id is empty")
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := collectionFenceKey{vchannel: vchannel, collectionID: collectionID}
	fenceTS := idx.fences[key]
	if fenceTS > readTS {
		return status.NewPartialUpdateRetryable("partial update read ts %d is older than collection fence ts %d", readTS, fenceTS)
	}
	return nil
}

// vchannelFenceIndex records commits whose complete transaction write set was
// not observed in the current WAL lifecycle. It conservatively invalidates all
// earlier CAS reads on that vchannel without changing transaction recovery.
type vchannelFenceIndex struct {
	mu     sync.RWMutex
	fences map[string]uint64
}

func newVChannelFenceIndex() *vchannelFenceIndex {
	return &vchannelFenceIndex{fences: make(map[string]uint64)}
}

func (idx *vchannelFenceIndex) Update(vchannel string, ts uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if ts > idx.fences[vchannel] {
		idx.fences[vchannel] = ts
	}
}

// Remove deletes the conservative fence for a terminal vchannel.
func (idx *vchannelFenceIndex) Remove(vchannel string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.fences, vchannel)
}

func (idx *vchannelFenceIndex) Verify(vchannel string, readTS uint64) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	fenceTS := idx.fences[vchannel]
	if fenceTS > readTS {
		return status.NewPartialUpdateRetryable(
			"partial update read ts %d is older than incomplete transaction fence ts %d",
			readTS,
			fenceTS,
		)
	}
	return nil
}
