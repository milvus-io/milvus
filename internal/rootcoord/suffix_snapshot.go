// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rootcoord

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

var (
	// suffixSnapshotTombstone special value for tombstone mark
	suffixSnapshotTombstone = []byte{0xE2, 0x9B, 0xBC}
)

// suffixSnapshot implements SnapshotKV
// this is a simple replacement for metaSnapshot, which is not available due to etcd compaction
// suffixSnapshot record timestamp as prefix of a key under the snapshot prefix path
type suffixSnapshot struct {
	// internal kv which suffixSnapshot based on
	kv.TxnKV
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
}

// tsv struct stores kv with timestamp
type tsv struct {
	value string
	ts    typeutil.Timestamp
}

// type conversion make sure implementation
var _ kv.SnapShotKV = (*suffixSnapshot)(nil)

// newSuffixSnapshot creates a newSuffixSnapshot with provided kv
func newSuffixSnapshot(txnKV kv.TxnKV, sep, root, snapshot string) (*suffixSnapshot, error) {
	if txnKV == nil {
		return nil, retry.NoRetryError(errors.New("txnKV is nil"))
	}

	// handles trailing / logic
	tk := path.Join(snapshot, "k")
	snapshotLen := len(tk) - 1
	// makes sure snapshot has trailing '/'
	snapshot = tk[:len(tk)-1]
	tk = path.Join(root, "k")
	rootLen := len(tk) - 1

	return &suffixSnapshot{
		TxnKV:          txnKV,
		lastestTS:      make(map[string]typeutil.Timestamp),
		separator:      sep,
		exp:            regexp.MustCompile(fmt.Sprintf(`^(.+)%s(\d+)$`, sep)),
		snapshotPrefix: snapshot,
		snapshotLen:    snapshotLen,
		rootPrefix:     root,
		rootLen:        rootLen,
	}, nil
}

// isTombstone helper function to check whether is tombstone mark
func (ss *suffixSnapshot) isTombstone(value string) bool {
	return bytes.Equal([]byte(value), suffixSnapshotTombstone)
}

// hideRootPrefix helper function to hide root prefix from key
func (ss *suffixSnapshot) hideRootPrefix(value string) string {
	return value[ss.rootLen:]
}

// composeSnapshotPrefix build a prefix for load snapshots
// formated like [snapshotPrefix]/key[sep]
func (ss *suffixSnapshot) composeSnapshotPrefix(key string) string {
	return path.Join(ss.snapshotPrefix, key+ss.separator)
}

// composeTSKey unified tsKey composing method
// uses key, ts and separator to form a key
func (ss *suffixSnapshot) composeTSKey(key string, ts typeutil.Timestamp) string {
	// [key][sep][ts]
	return path.Join(ss.snapshotPrefix, fmt.Sprintf("%s%s%d", key, ss.separator, ts))
}

// isTSKey checks whether a key is in ts-key format
// if true, also returns parsed ts value
func (ss *suffixSnapshot) isTSKey(key string) (typeutil.Timestamp, bool) {
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
func (ss *suffixSnapshot) isTSOfKey(key string, groupKey string) (typeutil.Timestamp, bool) {
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
func (ss *suffixSnapshot) checkKeyTS(key string, ts typeutil.Timestamp) (bool, error) {
	latest, has := ss.lastestTS[key]
	if !has {
		err := ss.loadLatestTS(key)
		if err != nil {
			return false, err
		}
		latest = ss.lastestTS[key]
	}
	return latest < ts, nil
}

// loadLatestTS load the loatest ts for specified key
func (ss *suffixSnapshot) loadLatestTS(key string) error {
	prefix := ss.composeSnapshotPrefix(key)
	keys, _, err := ss.TxnKV.LoadWithPrefix(prefix)
	if err != nil {
		log.Warn("suffixSnapshot txnkv LoadWithPrefix failed", zap.String("key", key),
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
// if ts is 0, suffixSnapshot works as a TxnKV
// otherwise, suffixSnapshot will store a ts-key as "key[sep]ts"-value pair in snapshot path
// and for acceleration store original key-value if ts is the latest
func (ss *suffixSnapshot) Save(key string, value string, ts typeutil.Timestamp) error {
	// if ts == 0, act like TxnKv
	// will not update lastestTs since ts not not valid
	if ts == 0 {
		return ss.TxnKV.Save(key, value)
	}

	ss.Lock()
	defer ss.Unlock()

	tsKey := ss.composeTSKey(key, ts)

	// provided key value is latest
	// stores both tsKey and orignal key
	after, err := ss.checkKeyTS(key, ts)
	if err != nil {
		return err
	}
	if after {
		err := ss.TxnKV.MultiSave(map[string]string{
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
	return ss.TxnKV.Save(tsKey, value)
}

func (ss *suffixSnapshot) Load(key string, ts typeutil.Timestamp) (string, error) {
	// if ts == 0, load latest by definition
	// and with acceleration logic, just do load key will do
	if ts == 0 {
		value, err := ss.TxnKV.Load(key)
		if ss.isTombstone(value) {
			return "", errors.New("no value found")
		}
		return value, err
	}

	ss.Lock()
	after, err := ss.checkKeyTS(key, ts)
	ss.Unlock()

	log.Warn("", zap.Bool("after", after), zap.Error(err))
	ss.RLock()
	defer ss.RUnlock()
	// ts after latest ts, load key as acceleration
	if err != nil {
		return "", err
	}
	if after {
		value, err := ss.TxnKV.Load(key)
		if ss.isTombstone(value) {
			return "", errors.New("no value found")
		}
		return value, err
	}

	// before ts, do time travel
	// 1. load all tsKey with key/ prefix
	keys, values, err := ss.TxnKV.LoadWithPrefix(ss.composeSnapshotPrefix(key))
	if err != nil {
		log.Warn("prefixSnapshot txnKV LoadWithPrefix failed", zap.String("key", key), zap.Error(err))
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

// MultiSave save muiltple kvs
// if ts == 0, act like TxnKV
// additions is executed before process each kvs
// each key-value (including additions result) will be treat in same logic like Save
func (ss *suffixSnapshot) MultiSave(kvs map[string]string, ts typeutil.Timestamp, additions ...func(ts typeutil.Timestamp) (string, string, error)) error {
	// apply additions
	for _, addition := range additions {
		k, v, err := addition(ts)
		if err != nil {
			continue
		}
		kvs[k] = v
	}
	// if ts == 0, act like TxnKV, with additions executed
	if ts == 0 {
		return ss.TxnKV.MultiSave(kvs)
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
	err = ss.TxnKV.MultiSave(execute)
	if err == nil {
		for _, key := range updateList {
			ss.lastestTS[key] = ts
		}
	}
	return err
}

// generateSaveExecute examine each key is the after the corresponding latest
// returns calculated execute map and update ts list
func (ss *suffixSnapshot) generateSaveExecute(kvs map[string]string, ts typeutil.Timestamp) (map[string]string, []string, error) {
	var after bool
	var err error
	execute := make(map[string]string)
	updateList := make([]string, 0, len(kvs))
	for key, value := range kvs {
		tsKey := ss.composeTSKey(key, ts)
		// provided key value is latest
		// stores both tsKey and orignal key
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
func (ss *suffixSnapshot) LoadWithPrefix(key string, ts typeutil.Timestamp) ([]string, []string, error) {
	// ts 0 case shall be treated as fetch latest/current value
	if ts == 0 {
		keys, values, err := ss.TxnKV.LoadWithPrefix(key)
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

	keys, values, err := ss.TxnKV.LoadWithPrefix(key)
	if err != nil {
		return nil, nil, err
	}

	// kv group stands for
	type kvgroup struct {
		key, value string
		processed  bool
		tsRecords  []tsv
	}
	groups := make([]kvgroup, 0, len(keys))
	for i, key := range keys {
		group := kvgroup{key: key, value: values[i]}
		// load prefix keys contains rootPrefix
		sKeys, sValues, err := ss.TxnKV.LoadWithPrefix(ss.composeSnapshotPrefix(ss.hideRootPrefix(key)))
		if err != nil {
			return nil, nil, err
		}
		group.tsRecords = make([]tsv, 0, len(sKeys))
		for j, sKey := range sKeys {
			ts, ok := ss.isTSOfKey(ss.hideRootPrefix(sKey), ss.hideRootPrefix(key))
			if ok {
				group.tsRecords = append(group.tsRecords, tsv{ts: ts, value: sValues[j]})
			}
		}
		groups = append(groups, group)
	}

	resultKeys := make([]string, 0, len(groups))
	resultValues := make([]string, 0, len(groups))
	// for each group, do ts travel logic if appliable
	for _, group := range groups {
		if len(group.tsRecords) == 0 {
			// not ts maybe, just use k,v
			resultKeys = append(resultKeys, group.key)
			resultValues = append(resultValues, group.value)
			continue
		}
		value, ok := binarySearchRecords(group.tsRecords, ts)
		if ok {
			// tombstone found, skip entry
			if ss.isTombstone(value) {
				continue
			}
			resultKeys = append(resultKeys, group.key)
			resultValues = append(resultValues, value)
		}
	}

	// hide rootPrefix from return value
	for i, k := range resultKeys {
		resultKeys[i] = ss.hideRootPrefix(k)
	}

	return resultKeys, resultValues, nil
}

// MultiSaveAndRemoveWithPrefix save muiltple kvs and remove as well
// if ts == 0, act like TxnKV
// additions is executed before process each kvs
// each key-value (including additions result) will be treat in same logic like Save
func (ss *suffixSnapshot) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string, ts typeutil.Timestamp, additions ...func(ts typeutil.Timestamp) (string, string, error)) error {
	// apply additions
	for _, addition := range additions {
		k, v, err := addition(ts)
		if err != nil {
			continue
		}
		saves[k] = v
	}

	// if ts == 0, act like TxnKV, with additions executed
	if ts == 0 {
		return ss.TxnKV.MultiSaveAndRemoveWithPrefix(saves, removals)
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
		keys, _, err := ss.TxnKV.LoadWithPrefix(removal)
		if err != nil {
			log.Warn("suffixSnapshot TxnKV LoadwithPrefix failed", zap.String("key", removal), zap.Error(err))
			return err
		}

		// add tombstone to orignal key and add ts entry
		for _, key := range keys {
			key = ss.hideRootPrefix(key)
			execute[key] = string(suffixSnapshotTombstone)
			execute[ss.composeTSKey(key, ts)] = string(suffixSnapshotTombstone)
			updateList = append(updateList, key)
		}
	}

	// multi save execute map; if succeeds, update ts in the update list
	err = ss.TxnKV.MultiSave(execute)
	if err == nil {
		for _, key := range updateList {
			ss.lastestTS[key] = ts
		}
	}
	return err
}
