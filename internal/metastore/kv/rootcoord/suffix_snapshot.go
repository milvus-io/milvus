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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
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

// ComposeSnapshotKey used in migration tool also, in case of any rules change.
func ComposeSnapshotKey(snapshotPrefix string, key string, separator string, ts typeutil.Timestamp) string {
	return path.Join(snapshotPrefix, fmt.Sprintf("%s%s%d", key, separator, ts))
}

// SuffixSnapshot implements SnapshotKV
// Simplified: all operations delegate to the underlying MetaKv directly.
// The ts parameter is accepted for interface compatibility but ignored.
// A background goroutine cleans up legacy snapshot keys from previous versions.
type SuffixSnapshot struct {
	// internal kv which SuffixSnapshot based on
	kv.MetaKv
	// rootPrefix is used to hide detail rootPrefix in kv
	rootPrefix string
	// rootLen pre calculated offset when hiding root prefix
	rootLen int
	// snapshotPrefix serves as prefix for legacy snapshot keys cleanup
	snapshotPrefix string

	paginationSize int

	closeGC    chan struct{}
	gcWg       sync.WaitGroup
	cancelFunc context.CancelFunc
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
	// makes sure snapshot has trailing '/'
	snapshot = tk[:len(tk)-1]
	tk = path.Join(root, "k")
	rootLen := len(tk) - 1

	ctx, cancel := context.WithCancel(context.Background())
	ss := &SuffixSnapshot{
		MetaKv:         metaKV,
		snapshotPrefix: snapshot,
		rootPrefix:     root,
		rootLen:        rootLen,
		paginationSize: paramtable.Get().MetaStoreCfg.PaginationSize.GetAsInt(),
		closeGC:        make(chan struct{}, 1),
		cancelFunc:     cancel,
	}
	ss.gcWg.Add(1)
	go ss.startLegacySnapshotCleanup(ctx)
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

// warnIfNonMaxTS logs a warning if the ts parameter is not MaxTimestamp or 0.
// After snapshot versioning removal, non-MaxTimestamp reads are no longer supported.
func (ss *SuffixSnapshot) warnIfNonMaxTS(ctx context.Context, method string, ts typeutil.Timestamp) {
	if ts != 0 && ts != typeutil.MaxTimestamp {
		log.Ctx(ctx).Warn("non-MaxTimestamp metadata read is no longer supported after snapshot removal, returning latest value",
			zap.String("method", method),
			zap.Uint64("ts", ts))
	}
}

// Save stores a key-value pair. The ts parameter is ignored.
func (ss *SuffixSnapshot) Save(ctx context.Context, key string, value string, ts typeutil.Timestamp) error {
	return ss.MetaKv.Save(ctx, key, value)
}

// Load retrieves the value for a key. The ts parameter is ignored.
// Returns error if the value is a tombstone (from legacy data).
func (ss *SuffixSnapshot) Load(ctx context.Context, key string, ts typeutil.Timestamp) (string, error) {
	ss.warnIfNonMaxTS(ctx, "Load", ts)
	value, err := ss.MetaKv.Load(ctx, key)
	if err != nil {
		return "", err
	}
	if ss.isTombstone(value) {
		return "", errors.New("no value found")
	}
	return value, nil
}

// MultiSave saves multiple key-value pairs. The ts parameter is ignored.
func (ss *SuffixSnapshot) MultiSave(ctx context.Context, kvs map[string]string, ts typeutil.Timestamp) error {
	return ss.MetaKv.MultiSave(ctx, kvs)
}

// LoadWithPrefix loads all keys with the given prefix.
// The ts parameter is ignored. Tombstone values are filtered out.
func (ss *SuffixSnapshot) LoadWithPrefix(ctx context.Context, key string, ts typeutil.Timestamp) ([]string, []string, error) {
	ss.warnIfNonMaxTS(ctx, "LoadWithPrefix", ts)
	fks := make([]string, 0)
	fvs := make([]string, 0)
	applyFn := func(key []byte, value []byte) error {
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

// MultiSaveAndRemove saves and removes key-value pairs.
// The ts parameter is ignored. Removals are physical deletes.
func (ss *SuffixSnapshot) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	return ss.MetaKv.MultiSaveAndRemove(ctx, saves, removals)
}

// MultiSaveAndRemoveWithPrefix saves key-value pairs and removes all keys matching the given prefixes.
// The ts parameter is ignored. Removals are physical deletes.
func (ss *SuffixSnapshot) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	return ss.MetaKv.MultiSaveAndRemoveWithPrefix(ctx, saves, removals)
}

func (ss *SuffixSnapshot) Close() {
	ss.cancelFunc()
	close(ss.closeGC)
	ss.gcWg.Wait()
}

// errLegacyCleanupBatchFull is a sentinel error used to interrupt WalkWithPrefix
// when enough keys have been collected for a single cleanup batch.
var errLegacyCleanupBatchFull = fmt.Errorf("legacy cleanup batch full")

const (
	// legacyCleanupBatchSize is the number of legacy snapshot keys to delete per cleanup batch.
	legacyCleanupBatchSize = 1000
	// legacyCleanupInterval is the interval between legacy snapshot cleanup batches.
	legacyCleanupInterval = 10 * time.Minute
)

// startLegacySnapshotCleanup gradually removes legacy snapshot keys left by previous versions.
// It runs an initial batch immediately on startup, then periodically until all legacy keys are cleaned up.
func (ss *SuffixSnapshot) startLegacySnapshotCleanup(ctx context.Context) {
	defer ss.gcWg.Done()
	log := log.Ctx(ctx)
	log.Info("legacy snapshot cleanup goroutine started",
		zap.String("snapshotPrefix", ss.snapshotPrefix),
		zap.Duration("interval", legacyCleanupInterval))

	// Check for cancellation before running the first batch
	if ctx.Err() != nil {
		log.Info("legacy snapshot cleanup goroutine canceled before first batch")
		return
	}

	// Run first batch immediately on startup instead of waiting for the first tick
	cleaned, err := ss.cleanLegacySnapshots(ctx)
	if err != nil {
		log.Warn("legacy snapshot cleanup initial batch error", zap.Error(err))
	} else if cleaned == 0 {
		log.Info("legacy snapshot cleanup complete on startup, no legacy keys found")
		return
	}

	ticker := time.NewTicker(legacyCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ss.closeGC:
			log.Info("legacy snapshot cleanup goroutine stopped")
			return
		case <-ticker.C:
			cleaned, err := ss.cleanLegacySnapshots(ctx)
			if err != nil {
				log.Warn("legacy snapshot cleanup error", zap.Error(err))
				continue
			}
			if cleaned == 0 {
				log.Info("legacy snapshot cleanup complete, no more keys to clean")
				return
			}
		}
	}
}

// cleanLegacySnapshots removes a batch of legacy snapshot keys.
// Returns the number of keys deleted, or 0 if no more keys remain.
func (ss *SuffixSnapshot) cleanLegacySnapshots(ctx context.Context) (int, error) {
	batchSize := legacyCleanupBatchSize
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	keys := make([]string, 0, batchSize)

	startTime := time.Now()
	err := ss.MetaKv.WalkWithPrefix(ctx, ss.snapshotPrefix, ss.paginationSize, func(k []byte, v []byte) error {
		keys = append(keys, ss.hideRootPrefix(string(k)))
		if len(keys) >= batchSize {
			return errLegacyCleanupBatchFull
		}
		return nil
	})
	if err != nil && !errors.Is(err, errLegacyCleanupBatchFull) {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, nil
	}

	removeFn := func(partialKeys []string) error {
		return ss.MetaKv.MultiRemove(ctx, partialKeys)
	}
	if err := etcd.RemoveByBatchWithLimit(keys, maxTxnNum, removeFn); err != nil {
		return 0, err
	}

	elapsed := time.Since(startTime)
	log.Info("legacy snapshot cleanup batch done",
		zap.Int("deleted", len(keys)),
		zap.Int("batchSize", batchSize),
		zap.Duration("elapsed", elapsed),
		zap.String("snapshotPrefix", ss.snapshotPrefix))
	return len(keys), nil
}
