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
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	// SuffixSnapshotTombstone special value for tombstone mark
	SuffixSnapshotTombstone = []byte{0xE2, 0x9B, 0xBC}
	PaginationSize          = 5000
)

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
	// exp is the shortcut format checker for ts-key
	// composed with separator only
	exp *regexp.Regexp

	closeGC chan struct{}
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

	ss := &SuffixSnapshot{
		MetaKv:         metaKV,
		lastestTS:      make(map[string]typeutil.Timestamp),
		separator:      sep,
		exp:            regexp.MustCompile(fmt.Sprintf(`^(.+)%s(\d+)$`, sep)),
		snapshotPrefix: snapshot,
		snapshotLen:    snapshotLen,
		rootPrefix:     root,
		rootLen:        rootLen,
		closeGC:        make(chan struct{}, 1),
	}
	go ss.startBackgroundGC()
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
	matches := ss.exp.FindStringSubmatch(key)
	if len(matches) < 3 {
		return 0, false
	}
	// err ignores since it's protected by the regexp
	ts, _ := strconv.ParseUint(matches[2], 10, 64)
	return ts, true
}

// isTSOfKey check whether a key is in ts-key format of provided group key
// if true, laso returns parsed ts value
func (ss *SuffixSnapshot) isTSOfKey(key string, groupKey string) (typeutil.Timestamp, bool) {
	// not in snapshot path
	if !strings.HasPrefix(key, ss.snapshotPrefix) {
		return 0, false
	}
	key = key[ss.snapshotLen:]

	matches := ss.exp.FindStringSubmatch(key)
	if len(matches) < 3 {
		return 0, false
	}
	if matches[1] != groupKey {
		return 0, false
	}
	// err ignores since it's protected by the regexp
	ts, _ := strconv.ParseUint(matches[2], 10, 64)
	return ts, true
}

// checkKeyTS checks provided key's latest ts is before provided ts
// lock is needed
func (ss *SuffixSnapshot) checkKeyTS(key string, ts typeutil.Timestamp) (bool, error) {
	latest, has := ss.lastestTS[key]
	if !has {
		err := ss.loadLatestTS(key)
		if err != nil {
			return false, err
		}
		latest = ss.lastestTS[key]
	}
	return latest <= ts, nil
}

// loadLatestTS load the loatest ts for specified key
func (ss *SuffixSnapshot) loadLatestTS(key string) error {
	prefix := ss.composeSnapshotPrefix(key)
	keys, _, err := ss.MetaKv.LoadWithPrefix(prefix)
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
		//log.Warn("", zap.Int("i", i), zap.Int("j", j), zap.Int("k", k))
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
// if ts is 0, SuffixSnapshot works as a MetaKv
// otherwise, SuffixSnapshot will store a ts-key as "key[sep]ts"-value pair in snapshot path
// and for acceleration store original key-value if ts is the latest
func (ss *SuffixSnapshot) Save(key string, value string, ts typeutil.Timestamp) error {
	// if ts == 0, act like MetaKv
	// will not update lastestTs since ts not not valid
	if ts == 0 {
		return ss.MetaKv.Save(key, value)
	}

	ss.Lock()
	defer ss.Unlock()

	tsKey := ss.composeTSKey(key, ts)

	// provided key value is latest
	// stores both tsKey and original key
	after, err := ss.checkKeyTS(key, ts)
	if err != nil {
		return err
	}
	if after {
		err := ss.MetaKv.MultiSave(map[string]string{
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
	return ss.MetaKv.Save(tsKey, value)
}

func (ss *SuffixSnapshot) Load(key string, ts typeutil.Timestamp) (string, error) {
	// if ts == 0, load latest by definition
	// and with acceleration logic, just do load key will do
	if ts == 0 {
		value, err := ss.MetaKv.Load(key)
		if ss.isTombstone(value) {
			return "", errors.New("no value found")
		}
		return value, err
	}

	ss.Lock()
	after, err := ss.checkKeyTS(key, ts)
	ss.Unlock()

	ss.RLock()
	defer ss.RUnlock()
	// ts after latest ts, load key as acceleration
	if err != nil {
		return "", err
	}
	if after {
		value, err := ss.MetaKv.Load(key)
		if ss.isTombstone(value) {
			return "", errors.New("no value found")
		}
		return value, err
	}

	// before ts, do time travel
	// 1. load all tsKey with key/ prefix
	keys, values, err := ss.MetaKv.LoadWithPrefix(ss.composeSnapshotPrefix(key))
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
	//binary search
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
func (ss *SuffixSnapshot) MultiSave(kvs map[string]string, ts typeutil.Timestamp) error {
	// if ts == 0, act like MetaKv
	if ts == 0 {
		return ss.MetaKv.MultiSave(kvs)
	}
	ss.Lock()
	defer ss.Unlock()
	var err error

	// process each key, checks whether is the latest
	execute, updateList, err := ss.generateSaveExecute(kvs, ts)
	if err != nil {
		return err
	}

	// multi save execute map; if succeeds, update ts in the update list
	err = ss.MetaKv.MultiSave(execute)
	if err == nil {
		for _, key := range updateList {
			ss.lastestTS[key] = ts
		}
	}
	return err
}

// generateSaveExecute examine each key is the after the corresponding latest
// returns calculated execute map and update ts list
func (ss *SuffixSnapshot) generateSaveExecute(kvs map[string]string, ts typeutil.Timestamp) (map[string]string, []string, error) {
	var after bool
	var err error
	execute := make(map[string]string)
	updateList := make([]string, 0, len(kvs))
	for key, value := range kvs {
		tsKey := ss.composeTSKey(key, ts)
		// provided key value is latest
		// stores both tsKey and original key
		after, err = ss.checkKeyTS(key, ts)
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
func (ss *SuffixSnapshot) LoadWithPrefix(key string, ts typeutil.Timestamp) ([]string, []string, error) {
	// ts 0 case shall be treated as fetch latest/current value
	if ts == 0 {
		keys, values, err := ss.MetaKv.LoadWithPrefix(key)
		fks := keys[:0]   //make([]string, 0, len(keys))
		fvs := values[:0] //make([]string, 0, len(values))
		// hide rootPrefix from return value
		for i, k := range keys {
			// filters tombstone
			if ss.isTombstone(values[i]) {
				continue
			}
			fks = append(fks, ss.hideRootPrefix(k))
			fvs = append(fvs, values[i])
		}
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

	err := ss.MetaKv.WalkWithPrefix(prefix, PaginationSize, func(k []byte, v []byte) error {
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

// MultiSaveAndRemoveWithPrefix save muiltple kvs and remove as well
// if ts == 0, act like MetaKv
// each key-value will be treated in same logic like Save
func (ss *SuffixSnapshot) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	// if ts == 0, act like MetaKv
	if ts == 0 {
		return ss.MetaKv.MultiSaveAndRemoveWithPrefix(saves, removals)
	}
	ss.Lock()
	defer ss.Unlock()
	var err error

	// process each key, checks whether is the latest
	execute, updateList, err := ss.generateSaveExecute(saves, ts)
	if err != nil {
		return err
	}

	// load each removal, change execution to adding tombstones
	for _, removal := range removals {
		keys, _, err := ss.MetaKv.LoadWithPrefix(removal)
		if err != nil {
			log.Warn("SuffixSnapshot MetaKv LoadwithPrefix failed", zap.String("key", removal), zap.Error(err))
			return err
		}

		// add tombstone to original key and add ts entry
		for _, key := range keys {
			key = ss.hideRootPrefix(key)
			execute[key] = string(SuffixSnapshotTombstone)
			execute[ss.composeTSKey(key, ts)] = string(SuffixSnapshotTombstone)
			updateList = append(updateList, key)
		}
	}

	// multi save execute map; if succeeds, update ts in the update list
	err = ss.MetaKv.MultiSave(execute)
	if err == nil {
		for _, key := range updateList {
			ss.lastestTS[key] = ts
		}
	}
	return err
}

func (ss *SuffixSnapshot) Close() {
	close(ss.closeGC)
}

// startBackgroundGC the data will clean up if key ts!=0 and expired
func (ss *SuffixSnapshot) startBackgroundGC() {
	log.Debug("suffix snapshot GC goroutine start!")
	ticker := time.NewTicker(60 * time.Minute)
	defer ticker.Stop()

	params := paramtable.Get()
	retentionDuration := params.CommonCfg.RetentionDuration.GetAsDuration(time.Second)

	for {
		select {
		case <-ss.closeGC:
			log.Warn("quit suffix snapshot GC goroutine!")
			return
		case now := <-ticker.C:
			err := ss.removeExpiredKvs(now, retentionDuration)
			if err != nil {
				log.Warn("remove expired data fail during GC", zap.Error(err))
			}
		}
	}
}

func (ss *SuffixSnapshot) getOriginalKey(snapshotKey string) (string, error) {
	if !strings.HasPrefix(snapshotKey, ss.snapshotPrefix) {
		return "", fmt.Errorf("get original key failed, invailed snapshot key:%s", snapshotKey)
	}
	// collect keys that parent node is snapshot node if the corresponding the latest ts is expired.
	idx := strings.LastIndex(snapshotKey, ss.separator)
	if idx == -1 {
		return "", fmt.Errorf("get original key failed, snapshot key:%s", snapshotKey)
	}
	prefix := snapshotKey[:idx]
	return prefix[ss.snapshotLen:], nil
}

func (ss *SuffixSnapshot) batchRemoveExpiredKvs(keyGroup []string, originalKey string, includeOriginalKey bool) error {
	if includeOriginalKey {
		keyGroup = append(keyGroup, originalKey)
	}

	// to protect txn finished with ascend order, reverse the latest kv with tombstone to tail of array
	sort.Strings(keyGroup)
	removeFn := func(partialKeys []string) error {
		return ss.MetaKv.MultiRemove(keyGroup)
	}
	return etcd.RemoveByBatch(keyGroup, removeFn)
}

func (ss *SuffixSnapshot) removeExpiredKvs(now time.Time, retentionDuration time.Duration) error {
	keyGroup := make([]string, 0)
	latestOriginalKey := ""
	latestValue := ""
	groupCnt := 0

	removeFn := func(curOriginalKey string) error {
		if !ss.isTombstone(latestValue) {
			return nil
		}
		return ss.batchRemoveExpiredKvs(keyGroup, curOriginalKey, groupCnt == len(keyGroup))
	}

	// walk all kvs with SortAsc, we need walk to the latest key for each key group to check the kv
	// whether contains tombstone, then if so, it represents the original key has been removed.
	// TODO: walk with Desc
	err := ss.MetaKv.WalkWithPrefix(ss.snapshotPrefix, PaginationSize, func(k []byte, v []byte) error {
		key := string(k)
		value := string(v)

		key = ss.hideRootPrefix(key)
		ts, ok := ss.isTSKey(key)
		// it is original key if the key doesn't contain ts
		if !ok {
			log.Warn("skip key because it doesn't contain ts", zap.String("key", key))
			return nil
		}

		curOriginalKey, err := ss.getOriginalKey(key)
		if err != nil {
			return err
		}

		// reset if starting look up a new key group
		if latestOriginalKey != "" && latestOriginalKey != curOriginalKey {
			// it indicates all keys need to remove that the prefix is original key
			// it means the latest original kvs has already been removed if the latest kv has tombstone marker.
			if err := removeFn(latestOriginalKey); err != nil {
				return err
			}

			keyGroup = make([]string, 0)
			groupCnt = 0
		}

		latestValue = value
		groupCnt++
		latestOriginalKey = curOriginalKey

		// record keys if the kv is expired
		pts, _ := tsoutil.ParseTS(ts)
		expireTime := pts.Add(retentionDuration)
		// break loop if it reaches expire time
		if expireTime.Before(now) {
			keyGroup = append(keyGroup, key)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return removeFn(latestOriginalKey)
}
