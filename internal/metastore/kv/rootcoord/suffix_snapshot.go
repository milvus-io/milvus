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
	"github.com/milvus-io/milvus/pkg/v2/util"
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

	// ensure snapshot has trailing '/'
	if !strings.HasSuffix(snapshot, "/") {
		snapshot += "/"
	}
	snapshotLen := len(snapshot)
	// keep empty root unchanged to preserve rootLen=0 behavior.
	if root != "" && !strings.HasSuffix(root, "/") {
		root += "/"
	}
	rootLen := len(root)

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
// formated like [snapshotPrefix]key[sep]
func (ss *SuffixSnapshot) composeSnapshotPrefix(key string) string {
	return ss.snapshotPrefix + key + ss.separator
}

// ComposeSnapshotKey used in migration tool also, in case of any rules change.
func ComposeSnapshotKey(snapshotPrefix string, key string, separator string, ts typeutil.Timestamp) string {
	// [key][sep][ts]
	return util.GetPath(snapshotPrefix, fmt.Sprintf("%s%s%d", key, separator, ts))
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

	prefix := ss.snapshotPrefix + key
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

// batchMultiLoad loads values for the given keys in batches to avoid exceeding
// etcd transaction limit (maxTxnNum). Returns a map of key -> (value exists).
// Note: MultiLoad returns error if any key is not found, but this is expected
// behavior for deleted collections. We handle this by checking values array.
func (ss *SuffixSnapshot) batchMultiLoad(ctx context.Context, keys []string, maxTxnNum int) (map[string]bool, error) {
	result := make(map[string]bool, len(keys))

	for i := 0; i < len(keys); i += maxTxnNum {
		end := i + maxTxnNum
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]
		values, err := ss.MetaKv.MultiLoad(ctx, batch)

		// MultiLoad returns error if any key is not found, but still returns values array
		// with empty strings for missing keys. This is expected for deleted collections.
		// We only fail on actual errors (len mismatch indicates etcd malfunction).
		//
		// Safety guarantee: If we encounter real errors (network failures, etcd timeouts, etc.),
		// we return error immediately without marking any keys for deletion. This prevents
		// incorrectly treating valid keys as "non-existent" and deleting them.
		if err != nil && len(values) != len(batch) {
			// Real error: values array is incomplete, not just missing keys
			return nil, err
		}

		for j, key := range batch {
			result[key] = values[j] != ""
		}
	}

	return result, nil
}

// startBackgroundGC runs incremental cursor-based GC for expired snapshot keys.
// It scans batchSize keys per tick, advances the cursor, and wraps around when
// the end of the snapshot keyspace is reached.
func (ss *SuffixSnapshot) startBackgroundGC(ctx context.Context) {
	defer ss.gcWg.Done()
	log := log.Ctx(ctx)

	gcInterval := paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotGCInterval.GetAsDuration(time.Second)
	batchSize := paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotGCBatchSize.GetAsInt()
	ttl := paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotTTLSeconds.GetAsDuration(time.Second)
	log.Info("snapshot GC goroutine started",
		zap.Int("batchSize", batchSize),
		zap.Duration("interval", gcInterval),
		zap.Duration("ttl", ttl))

	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ss.closeGC:
			log.Info("snapshot GC goroutine stopped")
			return
		case <-ticker.C:
			// Re-read parameters each tick to support dynamic configuration.
			batchSize = paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotGCBatchSize.GetAsInt()
			ttl = paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotTTLSeconds.GetAsDuration(time.Second)
			newInterval := paramtable.Get().ServiceParam.MetaStoreCfg.SnapshotGCInterval.GetAsDuration(time.Second)
			if newInterval != gcInterval {
				gcInterval = newInterval
				ticker.Reset(gcInterval)
				log.Info("snapshot GC interval updated", zap.Duration("interval", gcInterval))
			}

			deleted, scanned, err := ss.gcOneBatch(ctx, batchSize, ttl)
			if err != nil {
				log.Warn("snapshot GC batch error", zap.Error(err))
				continue
			}
			log.Info("snapshot GC batch done",
				zap.Int("scanned", scanned),
				zap.Int("deleted", deleted),
				zap.String("cursor", ss.gcCursor))
		}
	}
}

// gcCandidate holds metadata for a snapshot key collected during scan.
type gcCandidate struct {
	key         string
	originalKey string
	ts          typeutil.Timestamp
}

// gcOneBatch scans up to batchSize snapshot keys starting from gcCursor,
// identifies expired keys, and deletes them in cross-group aggregated batches.
// Returns the number of deleted and scanned keys.
func (ss *SuffixSnapshot) gcOneBatch(ctx context.Context, batchSize int, ttlTime time.Duration) (deleted int, scanned int, err error) {
	now := time.Now()
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()

	// ========== Phase 1: Scan ==========
	candidates := make([]gcCandidate, 0, batchSize)
	latestPerGroup := make(map[string]string) // originalKey -> latest snapshot key
	count := 0
	lastKey := ""

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

		// Keys are lexicographically sorted, so last seen = latest within this batch.
		// If a group is split across batches, this may not be the global latest —
		// but that only causes at most one extra key to be retained per split group,
		// which will be cleaned up in the next GC round when it is no longer latest.
		latestPerGroup[originalKey] = key
		candidates = append(candidates, gcCandidate{key: key, originalKey: originalKey, ts: ts})

		if count >= batchSize {
			return errGCBatchFull
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, errGCBatchFull) {
		return 0, count, walkErr
	}

	// ========== Phase 2: Check plain key existence ==========
	// If the plain key is gone, the entity was deleted (ts=0 direct removal).
	// All its snapshot keys are orphans — delete everything including latest.
	originalKeys := make([]string, 0, len(latestPerGroup))
	for originalKey := range latestPerGroup {
		originalKeys = append(originalKeys, originalKey)
	}

	plainExists, err := ss.batchMultiLoad(ctx, originalKeys, maxTxnNum)
	if err != nil {
		return 0, count, err
	}

	// ========== Phase 3: Filter ==========
	toDelete := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if !plainExists[c.originalKey] {
			// Orphan: plain key is gone, delete unconditionally.
			// When a collection/partition is dropped, RootCoord's tombstone sweeper removes
			// the plain key after DataCoord confirms all segments are GC'd. If the plain key
			// is missing, it means the collection/partition has been deleted, and all its
			// snapshot keys are orphans that should be cleaned up immediately (including latest).
			toDelete = append(toDelete, c.key)
			continue
		}

		// Plain key exists: only delete expired non-latest keys.
		expireTime, _ := tsoutil.ParseTS(c.ts)
		if !expireTime.Add(ttlTime).Before(now) {
			continue
		}
		if c.key == latestPerGroup[c.originalKey] {
			continue
		}

		toDelete = append(toDelete, c.key)
	}

	// ========== Phase 4: Delete ==========
	if len(toDelete) > 0 {
		removeFn := func(partialKeys []string) error {
			return ss.MetaKv.MultiRemove(ctx, partialKeys)
		}
		if err := etcd.RemoveByBatchWithLimit(toDelete, maxTxnNum, removeFn); err != nil {
			return 0, count, err
		}
	}

	// ========== Cursor update ==========
	// Only update cursor after all operations (scan, load, delete) succeed.
	// This ensures we don't skip keys if an error occurs mid-batch.
	if count < batchSize {
		ss.gcCursor = ""
	} else {
		ss.gcCursor = lastKey
	}

	return len(toDelete), count, nil
}
