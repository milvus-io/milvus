// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// SuffixSnapshotTombstone special value for tombstone mark
var SuffixSnapshotTombstone = []byte{0xE2, 0x9B, 0xBC}

// IsTombstone used in migration tool also.
func IsTombstone(value string) bool {
	return bytes.Equal([]byte(value), SuffixSnapshotTombstone)
}

// ConstructTombstone used in migration tool also.
func ConstructTombstone() []byte {
	return common.CloneByteSlice(SuffixSnapshotTombstone)
}

// SuffixSnapshot implements SnapshotKV
// this is a simple replacement for MetaSnapshot, which is not available due to etcd compaction
// SuffixSnapshot record timestamp as prefix of a key under the Snapshot prefix path
type SuffixSnapshot struct {
	// internal kv which SuffixSnapshot based on
	kv.MetaKv
	// rw mutex provided range lock
	sync.RWMutex
	// lastestTS latest timestamp for each key
	// note that this map is lazy loaded
	// which means if a key is never used in current session, no ts related to the key is stored
	// so is ts is not recorded in map, a `load ts` process will be triggered
	// a `0` for a key means that the key is not a snapshot key
	lastestTS map[string]typeutil.Timestamp
	// separator is the conjunction string between actual key and trailing ts
	// used in composing ts-key and check whether a key is a ts-key
	separator string
	// rootPrefix is used to hide detail rootPrefix in kv
	rootPrefix string
	// rootLen pre calculated offset when hiding root prefix
	rootLen int
	// snapshotPrefix serves a prefix stores snapshot with ts
	snapshotPrefix string
	// snapshotLen pre calculated offset when parsing snapshot key
	snapshotLen int

	paginationSize int

	// gcCursor tracks the last snapshot key processed by GC.
	// Empty string means start from the beginning of the snapshot prefix.
	// Only accessed by the single GC goroutine — no lock needed.
	gcCursor string

	closeGC    chan struct{}
	closeOnce  sync.Once
	gcWg       sync.WaitGroup
	cancelFunc context.CancelFunc
}

// tsv struct stores kv with timestamp
type tsv struct {
	value string
	ts    typeutil.Timestamp
}

// type conversion make sure implementation
var _ kv.SnapShotKV = (*SuffixSnapshot)(nil)

// NewSuffixSnapshot creates a NewSuffixSnapshot with provided kv
func NewSuffixSnapshot(metaKV kv.MetaKv, sep, root, snapshot string) (*SuffixSnapshot, error) {
	if metaKV == nil {
		return nil, retry.Unrecoverable(errors.New("MetaKv is nil"))
	}

	// handles trailing / logic
	tk := path.Join(snapshot, "k")
	snapshotLen := len(tk) - 1
	// makes sure snapshot has trailing '/'
	snapshot = tk[:len(tk)-1]
	tk = path.Join(root, "k")
	rootLen := len(tk) - 1

	ctx, cancel := context.WithCancel(context.Background())
	ss := &SuffixSnapshot{
		MetaKv:         metaKV,
		lastestTS:      make(map[string]typeutil.Timestamp),
		separator:      sep,
		snapshotPrefix: snapshot,
		snapshotLen:    snapshotLen,
		rootPrefix:     root,
		rootLen:        rootLen,
		paginationSize: paramtable.Get().MetaStoreCfg.PaginationSize.GetAsInt(),
		closeGC:        make(chan struct{}, 1),
		cancelFunc:     cancel,
	}
	ss.gcWg.Add(1)
	go ss.startBackgroundGC(ctx)
	return ss, nil
}

// isTombstone helper function to check whether is tombstone mark
func (ss *SuffixSnapshot) isTombstone(value string) bool {
	return IsTombstone(value)
}

// hideRootPrefix helper function to hide root prefix from key
func (ss *SuffixSnapshot) hideRootPrefix(value string) string {
	return value[ss.rootLen:]
}

// composeSnapshotPrefix build a prefix for load snapshots
// formated like [snapshotPrefix]/key[sep]
func (ss *SuffixSnapshot) composeSnapshotPrefix(key string) string {
	return path.Join(ss.snapshotPrefix, key+ss.separator)
}

// ComposeSnapshotKey used in migration tool also, in case of any rules change.
func ComposeSnapshotKey(snapshotPrefix string, key string, separator string, ts typeutil.Timestamp) string {
	// [key][sep][ts]
	return path.Join(snapshotPrefix, fmt.Sprintf("%s%s%d", key, separator, ts))
}

// composeTSKey unified tsKey composing method
// uses key, ts and separator to form a key
func (ss *SuffixSnapshot) composeTSKey(key string, ts typeutil.Timestamp) string {
	// [key][sep][ts]
	return ComposeSnapshotKey(ss.snapshotPrefix, key, ss.separator, ts)
}

// isTSKey checks whether a key is in ts-key format
// if true, also returns parsed ts value
func (ss *SuffixSnapshot) isTSKey(key string) (typeutil.Timestamp, bool) {
	// not in snapshot path
	if !strings.HasPrefix(key, ss.snapshotPrefix) {
		return 0, false
	}
	key = key[ss.snapshotLen:]
	idx := strings.LastIndex(key, ss.separator)
	if idx == -1 {
		return 0, false
	}
	ts, err := strconv.ParseUint(key[idx+len(ss.separator):], 10, 64)
	if err != nil {
		return 0, false
	}
	return ts, true
}

// isTSOfKey check whether a key is in ts-key format of provided group key
// if true, also returns parsed ts value
func (ss *SuffixSnapshot) isTSOfKey(key string, groupKey string) (typeutil.Timestamp, bool) {
	// not in snapshot path
	if !strings.HasPrefix(key, ss.snapshotPrefix) {
		return 0, false
	}
	key = key[ss.snapshotLen:]

	idx := strings.LastIndex(key, ss.separator)
	if idx == -1 {
		return 0, false
	}
	if key[:idx] != groupKey {
		return 0, false
	}
	ts, err := strconv.ParseUint(key[idx+len(ss.separator):], 10, 64)
	if err != nil {
		return 0, false
	}
	return ts, true
}

// checkKeyTS checks provided key's latest ts is before provided ts
// lock is needed
func (ss *SuffixSnapshot) checkKeyTS(ctx context.Context, key string, ts typeutil.Timestamp) (bool, error) {
	latest, has := ss.lastestTS[key]
	if !has {
		err := ss.loadLatestTS(ctx, key)
		if err != nil {
			return false, err
		}
		latest = ss.lastestTS[key]
	}
	return latest <= ts, nil
}

// loadLatestTS load the latest ts for specified key
func (ss *SuffixSnapshot) loadLatestTS(ctx context.Context, key string) error {
	prefix := ss.composeSnapshotPrefix(key)
	keys, _, err := ss.MetaKv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		log.Warn("SuffixSnapshot MetaKv LoadWithPrefix failed", zap.String("key", key),
			zap.Error(err))
		return err
	}
	tss := make([]typeutil.Timestamp, 0, len(keys))
	for _, key := range keys {
		ts, ok := ss.isTSKey(ss.hideRootPrefix(key))
		if ok {
			tss = append(tss, ts)
		}
	}
	if len(tss) == 0 {
		ss.lastestTS[key] = 0
		return nil
	}
	sort.Slice(tss, func(i, j int) bool {
		return tss[i] > tss[j]
	})
	ss.lastestTS[key] = tss[0]
	return nil
}

// binarySearchRecords returns value in sorted order with index i
// which satisfies records[i].ts <= ts && records[i+1].ts > ts
func binarySearchRecords(records []tsv, ts typeutil.Timestamp) (string, bool) {
	if len(records) == 0 {
		return "", false
	}
	// sort records by ts
	sort.Slice(records, func(i, j int) bool {
		return records[i].ts < records[j].ts
	})

	if ts < records[0].ts {
		return "", false
	}
	if ts > records[len(records)-1].ts {
		return records[len(records)-1].value, true
	}
	i, j := 0, len(records)
	for i+1 < j {
		k := (i + j) / 2
		if records[k].ts == ts {
			return records[k].value, true
		}

		if records[k].ts < ts {
			i = k
		} else {
			j = k
		}
	}
	return records[i].value, true
}

// Save stores key-value pairs with timestamp
// if ts == 0, SuffixSnapshot works as a MetaKv
// otherwise, SuffixSnapshot will store a ts-key as "key[sep]ts"-value pair in snapshot path
// and for acceleration store original key-value if ts is the latest
func (ss *SuffixSnapshot) Save(ctx context.Context, key string, value string, ts typeutil.Timestamp) error {
	// if ts == 0, act like MetaKv
	// will not update lastestTs since ts not not valid
	if ts == 0 {
		return ss.MetaKv.Save(ctx, key, value)
	}

	ss.Lock()
	defer ss.Unlock()

	tsKey := ss.composeTSKey(key, ts)

	// provided key value is latest
	// stores both tsKey and original key
	after, err := ss.checkKeyTS(ctx, key, ts)
	if err != nil {
		return err
	}
	if after {
		err := ss.MetaKv.MultiSave(ctx, map[string]string{
			key:   value,
			tsKey: value,
		})
		// update latestTS only when MultiSave succeeds
		if err == nil {
			ss.lastestTS[key] = ts
		}
		return err
	}

	// modifying history key, just save tskey-value
	return ss.MetaKv.Save(ctx, tsKey, value)
}

func (ss *SuffixSnapshot) Load(ctx context.Context, key string, ts typeutil.Timestamp) (string, error) {
	// if ts == 0 or typeutil.MaxTimestamp, load latest by definition
	// and with acceleration logic, just do load key will do
	if ts == 0 || ts == typeutil.MaxTimestamp {
		value, err := ss.MetaKv.Load(ctx, key)
		if ss.isTombstone(value) {
			return "", errors.New("no value found")
		}
		return value, err
	}

	ss.Lock()
	after, err := ss.checkKeyTS(ctx, key, ts)
	ss.Unlock()

	ss.RLock()
	defer ss.RUnlock()
	// ts after latest ts, load key as acceleration
	if err != nil {
		return "", err
	}
	if after {
		value, err := ss.MetaKv.Load(ctx, key)
		if ss.isTombstone(value) {
			return "", errors.New("no value found")
		}
		return value, err
	}

	// before ts, do time travel
	// 1. load all tsKey with key/ prefix
	keys, values, err := ss.MetaKv.LoadWithPrefix(ctx, ss.composeSnapshotPrefix(key))
	if err != nil {
		log.Warn("prefixSnapshot MetaKv LoadWithPrefix failed", zap.String("key", key), zap.Error(err))
		return "", err
	}

	records := make([]tsv, 0, len(keys))
	// 2. validate key and parse ts
	for i := 0; i < len(keys); i++ {
		ts, ok := ss.isTSKey(ss.hideRootPrefix(keys[i]))
		if !ok {
			continue
		}
		records = append(records, tsv{
			value: values[i],
			ts:    ts,
		})
	}

	if len(records) == 0 {
		return "", errors.New("not value found")
	}

	// 3. find i which ts[i] <= ts && ts[i+1] > ts
	// corner cases like len(records)==0, ts < records[0].ts is covered in binarySearch
	// binary search
	value, found := binarySearchRecords(records, ts)
	if !found {
		log.Warn("not found")
		return "", errors.New("no value found")
	}
	// check whether value is tombstone
	if ss.isTombstone(value) {
		log.Warn("tombstone", zap.String("value", value))
		return "", errors.New("not value found")
	}
	return value, nil
}

// MultiSave save multiple kvs
// if ts == 0, act like MetaKv
// each key-value will be treated using same logic like Save
func (ss *SuffixSnapshot) MultiSave(ctx context.Context, kvs map[string]string, ts typeutil.Timestamp) error {
	// if ts == 0, act like MetaKv
	if ts == 0 {
		return ss.MetaKv.MultiSave(ctx, kvs)
	}
	ss.Lock()
	defer ss.Unlock()
	var err error

	// process each key, checks whether is the latest
	execute, updateList, err := ss.generateSaveExecute(ctx, kvs, ts)
	if err != nil {
		return err
	}

	// multi save execute map; if succeeds, update ts in the update list
	err = ss.MetaKv.MultiSave(ctx, execute)
	if err == nil {
		for _, key := range updateList {
			ss.lastestTS[key] = ts
		}
	}
	return err
}

// generateSaveExecute examine each key is the after the corresponding latest
// returns calculated execute map and update ts list
func (ss *SuffixSnapshot) generateSaveExecute(ctx context.Context, kvs map[string]string, ts typeutil.Timestamp) (map[string]string, []string, error) {
	var after bool
	var err error
	execute := make(map[string]string)
	updateList := make([]string, 0, len(kvs))
	for key, value := range kvs {
		tsKey := ss.composeTSKey(key, ts)
		// provided key value is latest
		// stores both tsKey and original key
		after, err = ss.checkKeyTS(ctx, key, ts)
		if err != nil {
			return nil, nil, err
		}
		if after {
			execute[key] = value
			execute[tsKey] = value
			updateList = append(updateList, key)
		} else {
			execute[tsKey] = value
		}
	}
	return execute, updateList, nil
}

// LoadWithPrefix load keys with provided prefix and returns value in the ts
func (ss *SuffixSnapshot) LoadWithPrefix(ctx context.Context, key string, ts typeutil.Timestamp) ([]string, []string, error) {
	// ts 0 case shall be treated as fetch latest/current value
	if ts == 0 || ts == typeutil.MaxTimestamp {
		fks := make([]string, 0)
		fvs := make([]string, 0)
		// hide rootPrefix from return value
		applyFn := func(key []byte, value []byte) error {
			// filters tombstone
			if ss.isTombstone(string(value)) {
				return nil
			}
			fks = append(fks, ss.hideRootPrefix(string(key)))
			fvs = append(fvs, string(value))
			return nil
		}

		err := ss.MetaKv.WalkWithPrefix(ctx, key, ss.paginationSize, applyFn)
		return fks, fvs, err
	}
	ss.Lock()
	defer ss.Unlock()

	resultKeys := make([]string, 0)
	resultValues := make([]string, 0)

	latestOriginalKey := ""
	tValueGroups := make([]tsv, 0)

	prefix := path.Join(ss.snapshotPrefix, key)
	appendResultFn := func(ts typeutil.Timestamp) {
		value, ok := binarySearchRecords(tValueGroups, ts)
		if !ok || ss.isTombstone(value) {
			return
		}

		resultKeys = append(resultKeys, latestOriginalKey)
		resultValues = append(resultValues, value)
	}

	err := ss.MetaKv.WalkWithPrefix(ctx, prefix, ss.paginationSize, func(k []byte, v []byte) error {
		sKey := string(k)
		sValue := string(v)

		snapshotKey := ss.hideRootPrefix(sKey)
		curOriginalKey, err := ss.getOriginalKey(snapshotKey)
		if err != nil {
			return err
		}

		// reset if starting look up a new key group
		if latestOriginalKey != "" && latestOriginalKey != curOriginalKey {
			appendResultFn(ts)
			tValueGroups = make([]tsv, 0)
		}

		targetTs, ok := ss.isTSKey(snapshotKey)
		if !ok {
			log.Warn("skip key because it doesn't contain ts", zap.String("key", key))
			return nil
		}

		tValueGroups = append(tValueGroups, tsv{value: sValue, ts: targetTs})
		latestOriginalKey = curOriginalKey
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	appendResultFn(ts)
	return resultKeys, resultValues, nil
}

// MultiSaveAndRemove save multiple kvs and remove as well
// if ts == 0, act like MetaKv
// each key-value will be treated in same logic like Save
func (ss *SuffixSnapshot) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	// if ts == 0, act like MetaKv
	if ts == 0 {
		return ss.MetaKv.MultiSaveAndRemove(ctx, saves, removals)
	}
	ss.Lock()
	defer ss.Unlock()
	var err error

	// process each key, checks whether is the latest
	execute, updateList, err := ss.generateSaveExecute(ctx, saves, ts)
	if err != nil {
		return err
	}

	// load each removal, change execution to adding tombstones
	for _, removal := range removals {
		// if save batch contains removal, skip remove op
		if _, ok := execute[removal]; ok {
			continue
		}
		value, err := ss.MetaKv.Load(ctx, removal)
		if err != nil {
			log.Warn("SuffixSnapshot MetaKv Load failed", zap.String("key", removal), zap.Error(err))
			if errors.Is(err, merr.ErrIoKeyNotFound) {
				continue
			}
			return err
		}
		// add tombstone to original key and add ts entry
		if IsTombstone(value) {
			continue
		}
		execute[removal] = string(SuffixSnapshotTombstone)
		execute[ss.composeTSKey(removal, ts)] = string(SuffixSnapshotTombstone)
		updateList = append(updateList, removal)
	}

	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	err = etcd.SaveByBatchWithLimit(execute, maxTxnNum, func(partialKvs map[string]string) error {
		return ss.MetaKv.MultiSave(ctx, partialKvs)
	})
	if err == nil {
		for _, key := range updateList {
			ss.lastestTS[key] = ts
		}
	}
	return err
}

// MultiSaveAndRemoveWithPrefix save multiple kvs and remove as well
// if ts == 0, act like MetaKv
// each key-value will be treated in same logic like Save
func (ss *SuffixSnapshot) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	// if ts == 0, act like MetaKv
	if ts == 0 {
		return ss.MetaKv.MultiSaveAndRemoveWithPrefix(ctx, saves, removals)
	}
	ss.Lock()
	defer ss.Unlock()
	var err error

	// process each key, checks whether is the latest
	execute, updateList, err := ss.generateSaveExecute(ctx, saves, ts)
	if err != nil {
		return err
	}

	// load each removal, change execution to adding tombstones
	for _, removal := range removals {
		keys, values, err := ss.MetaKv.LoadWithPrefix(ctx, removal)
		if err != nil {
			log.Warn("SuffixSnapshot MetaKv LoadwithPrefix failed", zap.String("key", removal), zap.Error(err))
			return err
		}

		// add tombstone to original key and add ts entry
		for idx, key := range keys {
			if IsTombstone(values[idx]) {
				continue
			}
			key = ss.hideRootPrefix(key)
			execute[key] = string(SuffixSnapshotTombstone)
			execute[ss.composeTSKey(key, ts)] = string(SuffixSnapshotTombstone)
			updateList = append(updateList, key)
		}
	}

	// multi save execute map; if succeeds, update ts in the update list
	err = ss.MetaKv.MultiSave(ctx, execute)
	if err == nil {
		for _, key := range updateList {
			ss.lastestTS[key] = ts
		}
	}
	return err
}

func (ss *SuffixSnapshot) Close() {
	ss.closeOnce.Do(func() {
		ss.cancelFunc()
		close(ss.closeGC)
	})
	ss.gcWg.Wait()
}

func (ss *SuffixSnapshot) getOriginalKey(snapshotKey string) (string, error) {
	if !strings.HasPrefix(snapshotKey, ss.snapshotPrefix) {
		return "", fmt.Errorf("get original key failed, invaild snapshot key:%s", snapshotKey)
	}
	// collect keys that parent node is snapshot node if the corresponding the latest ts is expired.
	idx := strings.LastIndex(snapshotKey, ss.separator)
	if idx == -1 {
		return "", fmt.Errorf("get original key failed, snapshot key:%s", snapshotKey)
	}
	prefix := snapshotKey[:idx]
	return prefix[ss.snapshotLen:], nil
}

// errGCBatchFull is a sentinel error used to interrupt WalkWithPrefixFrom
// when enough keys have been collected for a single GC batch.
var errGCBatchFull = fmt.Errorf("gc batch full")

// startBackgroundGC runs incremental cursor-based GC for expired snapshot keys.
// It scans batchSize keys per tick, advances the cursor, and wraps around when
// the end of the snapshot keyspace is reached.
func (ss *SuffixSnapshot) startBackgroundGC(ctx context.Context) {
	defer ss.gcWg.Done()
	log := log.Ctx(ctx)

	getGCInterval := func() time.Duration {
		d := paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotGCInterval.GetAsDuration(time.Second)
		if d <= 0 {
			d = 10 * time.Second // defensive fallback
		}
		return d
	}
	getGCBatchSize := func() int {
		n := paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotGCBatchSize.GetAsInt()
		if n <= 0 {
			n = 10000 // defensive fallback
		}
		return n
	}

	gcInterval := getGCInterval()
	log.Info("snapshot GC goroutine started",
		zap.Int("batchSize", getGCBatchSize()),
		zap.Duration("interval", gcInterval))

	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ss.closeGC:
			log.Info("snapshot GC goroutine stopped")
			return
		case <-ticker.C:
			// Re-read parameters each tick to support dynamic configuration
			batchSize := getGCBatchSize()
			newInterval := getGCInterval()
			if newInterval != gcInterval {
				gcInterval = newInterval
				ticker.Reset(gcInterval)
				log.Info("snapshot GC interval updated", zap.Duration("interval", gcInterval))
			}

			deleted, scanned, err := ss.gcOneBatch(ctx, batchSize)
			if err != nil {
				log.Warn("snapshot GC batch error", zap.Error(err))
				continue
			}
			if deleted > 0 {
				log.Info("snapshot GC batch deleted keys",
					zap.Int("scanned", scanned),
					zap.Int("deleted", deleted),
					zap.String("cursor", ss.gcCursor))
			} else {
				log.Debug("snapshot GC batch done",
					zap.Int("scanned", scanned),
					zap.String("cursor", ss.gcCursor))
			}
		}
	}
}

// gcOneBatch scans up to batchSize snapshot keys starting from gcCursor,
// identifies expired keys, and deletes them in cross-group aggregated batches.
// Returns the number of deleted and scanned keys.
func (ss *SuffixSnapshot) gcOneBatch(ctx context.Context, batchSize int) (deleted int, scanned int, err error) {
	ttlTime := paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotTTLSeconds.GetAsDuration(time.Second)
	reserveTime := paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotReserveTimeSeconds.GetAsDuration(time.Second)
	now := time.Now()
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	startCursor := ss.gcCursor

	// ========== Phase 1: Scan — collect candidate expired keys ==========
	expiredKeys := make([]string, 0, batchSize)
	// Track the latest snapshot key per originalKey to avoid deleting it.
	// Since keys are lexicographically sorted and ts is part of the key suffix,
	// the last seen key for each originalKey within this batch is the latest.
	latestPerGroup := make(map[string]string) // originalKey -> latest snapshot key
	// Track which groups contain tombstone values. For tombstone groups,
	// we use reserveTime (shorter) instead of ttlTime for faster cleanup.
	tombstoneGroups := make(map[string]bool) // originalKey -> is tombstone
	// Track total and expired counts per group to detect fully-expired tombstone groups.
	groupTotalCount := make(map[string]int)   // originalKey -> total snapshot keys seen
	groupExpiredCount := make(map[string]int) // originalKey -> expired snapshot keys count
	// Cache parsed ts per key to avoid re-parsing in Phase 2.
	keyTsCache := make(map[string]typeutil.Timestamp) // snapshot key -> parsed ts
	count := 0
	lastKey := ""
	// Track boundary groups that may be split across batches.
	var firstGroup, lastGroup string

	walkErr := ss.MetaKv.WalkWithPrefixFrom(ctx, ss.snapshotPrefix, ss.gcCursor, batchSize, func(k []byte, v []byte) error {
		count++
		key := ss.hideRootPrefix(string(k))
		lastKey = key

		ts, ok := ss.isTSKey(key)
		if !ok {
			return nil
		}

		originalKey, err := ss.getOriginalKey(key)
		if err != nil {
			log.Ctx(ctx).Warn("snapshot GC: failed to parse original key", zap.String("key", key), zap.Error(err))
			return nil
		}

		// Track boundary groups
		if firstGroup == "" {
			firstGroup = originalKey
		}
		lastGroup = originalKey

		// Track latest per group (keys are sorted, so last seen = latest)
		latestPerGroup[originalKey] = key
		// Track tombstone status per group (last value = latest status)
		tombstoneGroups[originalKey] = ss.isTombstone(string(v))
		groupTotalCount[originalKey]++

		// Check if expired using reserveTime (the shorter window).
		// Phase 2 will apply the longer TTL for non-tombstone groups.
		expireTime, _ := tsoutil.ParseTS(ts)
		if expireTime.Add(reserveTime).Before(now) {
			expiredKeys = append(expiredKeys, key)
			keyTsCache[key] = ts
			groupExpiredCount[originalKey]++
		}

		if count >= batchSize {
			return errGCBatchFull
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, errGCBatchFull) {
		return 0, count, walkErr
	}

	// ========== Cursor update ==========
	// Update AFTER scan completes. Cursor semantics: "everything up to here
	// has been scanned in this round."
	//
	// If scanned < batchSize: we've reached the end of the snapshot keyspace.
	// Reset cursor to "" so next tick starts a new round from the beginning.
	// This costs one extra tick (scanning the first batch again), but those keys
	// may have newly expired since the last round, so the scan is not wasted.
	if count < batchSize {
		ss.gcCursor = ""
	} else {
		ss.gcCursor = lastKey
	}

	// ========== Phase 2: Filter — protect latest-per-group & apply TTL ==========
	// Three filtering rules:
	// 1. For non-tombstone groups: protect the latest snapshot key (needed for
	//    time-travel), and apply the longer TTL check. Phase 1 used reserveTime
	//    (shorter) to collect candidates; now we filter out keys that haven't
	//    actually reached TTL expiry yet.
	// 2. For tombstone groups where NOT all versions are expired: same as
	//    non-tombstone (protect latest, apply reserveTime which is already met).
	// 3. For tombstone groups where ALL versions are expired: delete everything
	//    including the latest snapshot key AND the plain key (originalKey).
	//    This cleans up the tombstone from the plain key namespace.
	toDelete := make([]string, 0, len(expiredKeys))
	latestSet := make(map[string]struct{}, len(latestPerGroup))
	for _, v := range latestPerGroup {
		latestSet[v] = struct{}{}
	}

	// Identify fully-expired tombstone groups — include latest + plain key.
	// Boundary groups (first/last in the batch) may be split across batches,
	// so we exclude them from full cleanup to avoid premature plain key deletion.
	// They will be handled in a future batch when they appear completely.
	fullyExpiredTombstones := make(map[string]bool)
	isBoundaryGroup := func(key string) bool {
		return (startCursor != "" && key == firstGroup) || (count >= batchSize && key == lastGroup)
	}
	for originalKey, isTombstone := range tombstoneGroups {
		if isTombstone && groupTotalCount[originalKey] == groupExpiredCount[originalKey] && !isBoundaryGroup(originalKey) {
			fullyExpiredTombstones[originalKey] = true
		}
	}

	for _, key := range expiredKeys {
		originalKey, _ := ss.getOriginalKey(key)

		if _, isLatest := latestSet[key]; isLatest {
			// Only delete latest if this is a fully-expired tombstone group
			if !fullyExpiredTombstones[originalKey] {
				continue
			}
		}

		// For non-tombstone groups, apply the longer TTL filter
		if !tombstoneGroups[originalKey] {
			ts := keyTsCache[key]
			expireTime, _ := tsoutil.ParseTS(ts)
			if !expireTime.Add(ttlTime).Before(now) {
				continue // not yet expired by TTL
			}
		}
		toDelete = append(toDelete, key)
	}

	// For fully-expired tombstone groups, also delete the plain key
	for originalKey := range fullyExpiredTombstones {
		toDelete = append(toDelete, originalKey)
	}

	if len(toDelete) == 0 {
		return 0, count, nil
	}

	// ========== Phase 3: Delete — cross-group aggregated batch delete ==========
	// All expired keys from ALL groups in this batch are deleted together,
	// instead of one MultiRemove per group. This reduces etcd transaction count
	// from potentially thousands to tens (toDelete / maxTxnNum).
	removeFn := func(partialKeys []string) error {
		return ss.MetaKv.MultiRemove(ctx, partialKeys)
	}
	if err := etcd.RemoveByBatchWithLimit(toDelete, maxTxnNum, removeFn); err != nil {
		return 0, count, err
	}

	return len(toDelete), count, nil
}
