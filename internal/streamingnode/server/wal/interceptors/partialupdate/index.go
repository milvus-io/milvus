package partialupdate

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const (
	defaultVersionIndexTTL = 30 * time.Second

	// The estimate covers the map key/value, heap pointer, entry object, and
	// allocator overhead. String backing bytes are accounted separately.
	estimatedVersionEntryFixedBytes int64 = 128
)

type primaryKeyKind uint8

const (
	primaryKeyKindNone primaryKeyKind = iota
	primaryKeyKindInt64
	primaryKeyKindString
	primaryKeyKindMixed
)

type primaryKeys struct {
	kind         primaryKeyKind
	int64Values  []int64
	stringValues []string
}

func (p primaryKeys) Len() int {
	return len(p.int64Values) + len(p.stringValues)
}

func (p primaryKeys) clone() primaryKeys {
	return primaryKeys{
		kind:         p.kind,
		int64Values:  append([]int64(nil), p.int64Values...),
		stringValues: append([]string(nil), p.stringValues...),
	}
}

// toAny keeps the existing test and helper API while the append path uses the
// typed representation above.
func (p primaryKeys) toAny() []any {
	values := make([]any, 0, p.Len())
	for _, value := range p.int64Values {
		values = append(values, value)
	}
	for _, value := range p.stringValues {
		values = append(values, value)
	}
	return values
}

func (p *primaryKeys) append(other primaryKeys) {
	if other.Len() == 0 {
		return
	}
	if p.kind == primaryKeyKindNone {
		p.kind = other.kind
	}
	if p.kind != other.kind && p.kind != primaryKeyKindMixed {
		p.kind = primaryKeyKindMixed
	}
	p.int64Values = append(p.int64Values, other.int64Values...)
	p.stringValues = append(p.stringValues, other.stringValues...)
}

func primaryKeysFromAny(values []any) (primaryKeys, error) {
	result := primaryKeys{}
	for _, value := range values {
		var current primaryKeys
		switch value := value.(type) {
		case int64:
			current = primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{value}}
		case string:
			current = primaryKeys{kind: primaryKeyKindString, stringValues: []string{value}}
		default:
			return primaryKeys{}, status.NewUnrecoverableError("partial update pk must be int64 or string")
		}
		result.append(current)
	}
	return result, nil
}

// pkVersionIndex isolates mutable PK history by vchannel while sharing a
// StreamingNode-wide byte budget with other WALs built from the same builder.
type pkVersionIndex struct {
	ttl      time.Duration
	budget   *versionByteBudget
	channels sync.Map // map[string]*vchannelPKVersionIndex
}

// versionByteBudget bounds the estimated memory retained by all WAL indexes on
// one StreamingNode.
type versionByteBudget struct {
	limit        int64
	used         atomic.Int64
	usedMetric   prometheus.Gauge
	missedMetric prometheus.Counter
}

func newVersionByteBudget(limit int64) *versionByteBudget {
	return newVersionByteBudgetWithMetrics(limit, nil, nil)
}

func newVersionByteBudgetWithMetrics(limit int64, usedMetric prometheus.Gauge, missedMetric prometheus.Counter) *versionByteBudget {
	if limit < 0 {
		limit = 0
	}
	return &versionByteBudget{limit: limit, usedMetric: usedMetric, missedMetric: missedMetric}
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
			if b.usedMetric != nil {
				b.usedMetric.Add(float64(bytes))
			}
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
	if b.usedMetric != nil {
		b.usedMetric.Sub(float64(bytes))
	}
}

func (b *versionByteBudget) recordMissedWrite(missed bool) {
	if missed && b.missedMetric != nil {
		b.missedMetric.Inc()
	}
}

// vchannelPKVersionIndex tracks recent PK writes on one vchannel.
type vchannelPKVersionIndex struct {
	mu                sync.Mutex
	ttl               time.Duration
	budget            *versionByteBudget
	retainedSinceTS   uint64
	lastMissedWriteTS uint64
	int64Versions     map[int64]*versionEntry
	stringVersions    map[string]*versionEntry
	expirations       versionMinHeap
}

type versionEntry struct {
	int64PK        int64
	stringPK       string
	stringKey      bool
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

// newPKVersionIndex creates a byte-bounded index with a private budget.
func newPKVersionIndex(ttl time.Duration, maxBytes int64) *pkVersionIndex {
	return newPKVersionIndexWithBudget(ttl, newVersionByteBudget(maxBytes))
}

func newPKVersionIndexWithBudget(ttl time.Duration, budget *versionByteBudget) *pkVersionIndex {
	return &pkVersionIndex{
		ttl:    ttl,
		budget: budget,
	}
}

func (idx *pkVersionIndex) channel(vchannel string) *vchannelPKVersionIndex {
	if existing, ok := idx.channels.Load(vchannel); ok {
		return existing.(*vchannelPKVersionIndex)
	}
	created := &vchannelPKVersionIndex{
		ttl:            idx.ttl,
		budget:         idx.budget,
		int64Versions:  make(map[int64]*versionEntry),
		stringVersions: make(map[string]*versionEntry),
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

func (idx *pkVersionIndex) Close() {
	idx.channels.Range(func(key, _ any) bool {
		idx.Remove(key.(string))
		return true
	})
}

// UpdateAll records one committed write batch atomically within its vchannel.
func (idx *pkVersionIndex) UpdateAll(vchannel string, pks []any, commitTS uint64) {
	keys, err := primaryKeysFromAny(pks)
	if err != nil {
		panic(err)
	}
	idx.UpdateAllTyped(vchannel, keys, commitTS)
}

func (idx *vchannelPKVersionIndex) clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var releasedBytes int64
	for _, entry := range idx.int64Versions {
		releasedBytes += entry.estimatedBytes
	}
	for _, entry := range idx.stringVersions {
		releasedBytes += entry.estimatedBytes
	}
	idx.int64Versions = make(map[int64]*versionEntry)
	idx.stringVersions = make(map[string]*versionEntry)
	idx.expirations = nil
	idx.retainedSinceTS = 0
	idx.lastMissedWriteTS = 0
	idx.budget.release(releasedBytes)
}

func (idx *pkVersionIndex) UpdateAllTyped(vchannel string, pks primaryKeys, commitTS uint64) {
	idx.channel(vchannel).updateAll(pks, commitTS)
}

func (idx *vchannelPKVersionIndex) updateAll(pks primaryKeys, commitTS uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.advanceRetentionLocked(commitTS)
	missedWrite := false
	for _, pk := range pks.int64Values {
		entry, ok := idx.int64Versions[pk]
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
		if !idx.budget.tryReserve(estimatedVersionEntryFixedBytes) {
			// Once a committed write is omitted, exact conflict detection is
			// incomplete until that write leaves every valid read window.
			if commitTS > idx.lastMissedWriteTS {
				idx.lastMissedWriteTS = commitTS
			}
			missedWrite = true
			continue
		}
		entry = &versionEntry{
			int64PK:        pk,
			commitTS:       commitTS,
			estimatedBytes: estimatedVersionEntryFixedBytes,
		}
		idx.int64Versions[pk] = entry
		heap.Push(&idx.expirations, entry)
	}
	for _, pk := range pks.stringValues {
		entry, ok := idx.stringVersions[pk]
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
		estimatedBytes := estimatedVersionEntryFixedBytes + int64(len(pk))
		if !idx.budget.tryReserve(estimatedBytes) {
			if commitTS > idx.lastMissedWriteTS {
				idx.lastMissedWriteTS = commitTS
			}
			missedWrite = true
			continue
		}
		entry = &versionEntry{
			stringPK:       pk,
			stringKey:      true,
			commitTS:       commitTS,
			estimatedBytes: estimatedBytes,
		}
		idx.stringVersions[pk] = entry
		heap.Push(&idx.expirations, entry)
	}
	idx.budget.recordMissedWrite(missedWrite)
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

func (idx *pkVersionIndex) VerifyTyped(vchannel string, pks primaryKeys, readTS, commitTS uint64) error {
	return idx.channel(vchannel).verify(vchannel, pks, readTS, commitTS)
}

// Verify rejects reads whose retained PK version changed after readTS.
func (idx *pkVersionIndex) Verify(vchannel string, pks []any, readTS, commitTS uint64) error {
	keys, err := primaryKeysFromAny(pks)
	if err != nil {
		return err
	}
	return idx.VerifyTyped(vchannel, keys, readTS, commitTS)
}

func (idx *vchannelPKVersionIndex) verify(vchannel string, pks primaryKeys, readTS, commitTS uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.advanceRetentionLocked(commitTS)
	if err := idx.verifyReadWindowLocked(readTS, commitTS); err != nil {
		return err
	}

	for _, pk := range pks.int64Values {
		if entry := idx.int64Versions[pk]; entry != nil && entry.commitTS > readTS {
			return status.NewPartialUpdateRetryable("partial update pk conflict, vchannel: %s, read ts: %d, last commit ts: %d", vchannel, readTS, entry.commitTS)
		}
	}
	for _, pk := range pks.stringValues {
		if entry := idx.stringVersions[pk]; entry != nil && entry.commitTS > readTS {
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
		if entry.stringKey {
			delete(idx.stringVersions, entry.stringPK)
		} else {
			delete(idx.int64Versions, entry.int64PK)
		}
		idx.budget.release(entry.estimatedBytes)
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
