package rootcoord

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// Legacy SuffixSnapshot tombstone cleanup.
//
// Before commit e0873a65d4, SuffixSnapshot.MultiSaveAndRemove(ts != 0) did not
// hard-delete the plain meta key — it overwrote the value with a 3-byte
// tombstone marker (SuffixSnapshotTombstone) and wrote a timestamped copy
// under "snapshots/". After removing SuffixSnapshot, legacy_snapshot_gc.go
// sweeps the "snapshots/" prefix, but tombstone-valued plain keys remain and
// cause proto.Unmarshal to fail on restart. StartLegacyTombstoneGC walks each
// RootCoord meta prefix and removes keys whose value exactly equals the
// tombstone marker.

var legacyTombstoneGCOnce sync.Once

// tombstoneGCPrefixes lists every RootCoord plain meta prefix that could
// hold a legacy tombstone value. Prefixes end with "/" so we never match
// a sibling prefix by accident.
var tombstoneGCPrefixes = []string{
	CollectionMetaPrefix + "/",
	CollectionInfoMetaPrefix + "/",
	PartitionMetaPrefix + "/",
	FieldMetaPrefix + "/",
	StructArrayFieldMetaPrefix + "/",
	FunctionMetaPrefix + "/",
	AliasMetaPrefix + "/",
	CollectionAliasMetaPrefix210 + "/",
	DBInfoMetaPrefix + "/",
}

// StartLegacyTombstoneGC starts a background goroutine that removes legacy
// tombstone-valued plain meta keys. The goroutine self-terminates after a
// full pass across all prefixes finds zero tombstones. Safe to call multiple
// times — only the first call starts the goroutine.
//
// Safety: value comparison is byte-exact against SuffixSnapshotTombstone
// ({0xE2, 0x9B, 0xBC}). Valid protobuf-encoded meta values for these
// prefixes cannot equal those 3 bytes (a non-empty proto starts with a
// field tag byte, and the resulting length varint would require more than
// one additional byte), so no live meta can be mistakenly deleted.
func StartLegacyTombstoneGC(ctx context.Context, metaKV kv.MetaKv) {
	legacyTombstoneGCOnce.Do(func() {
		go runLegacyTombstoneGC(ctx, metaKV)
	})
}

func runLegacyTombstoneGC(ctx context.Context, metaKV kv.MetaKv) {
	logger := log.Ctx(ctx)
	logger.Info("legacy tombstone GC started",
		zap.Strings("prefixes", tombstoneGCPrefixes),
		zap.Int("batchSize", legacyGCBatchSize),
		zap.Duration("interval", legacyGCInterval))

	ticker := time.NewTicker(legacyGCInterval)
	defer ticker.Stop()

	totalDeleted := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			logger.Info("legacy tombstone GC stopped (context canceled)",
				zap.Int("totalDeleted", totalDeleted),
				zap.Duration("totalElapsed", time.Since(startTime)))
			return
		case <-ticker.C:
			deleted, err := gcLegacyTombstonePass(ctx, metaKV)
			if err != nil {
				logger.Warn("legacy tombstone GC pass error",
					zap.Error(err),
					zap.Int("totalDeleted", totalDeleted))
				continue
			}
			if deleted == 0 {
				logger.Info("legacy tombstone GC complete",
					zap.Int("totalDeleted", totalDeleted),
					zap.Duration("totalElapsed", time.Since(startTime)))
				return
			}
			totalDeleted += deleted
			logger.Info("legacy tombstone GC pass done",
				zap.Int("deleted", deleted),
				zap.Int("totalDeleted", totalDeleted),
				zap.Duration("elapsed", time.Since(startTime)))
		}
	}
}

// gcLegacyTombstonePass makes one pass over every tombstone GC prefix,
// deleting keys whose value equals the tombstone marker in batches.
// Returns the total number of keys deleted across all prefixes.
func gcLegacyTombstonePass(ctx context.Context, metaKV kv.MetaKv) (int, error) {
	total := 0
	for _, prefix := range tombstoneGCPrefixes {
		deleted, err := gcLegacyTombstoneBatch(ctx, metaKV, prefix)
		if err != nil {
			return total, err
		}
		total += deleted
	}
	return total, nil
}

// gcLegacyTombstoneBatch collects up to legacyGCBatchSize tombstone-valued
// keys under the given prefix and deletes them.
func gcLegacyTombstoneBatch(ctx context.Context, metaKV kv.MetaKv, prefix string) (int, error) {
	keys := make([]string, 0, legacyGCBatchSize)
	err := metaKV.WalkWithPrefix(ctx, prefix, legacyGCBatchSize,
		func(k []byte, v []byte) error {
			if !IsTombstone(string(v)) {
				return nil
			}
			keys = append(keys, string(k))
			if len(keys) >= legacyGCBatchSize {
				return errBatchFull
			}
			return nil
		})
	if err != nil && !errors.Is(err, errBatchFull) {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, nil
	}

	// WalkWithPrefix returns full etcd paths (with rootPath). MultiRemove
	// expects relative paths because it adds rootPath internally. Strip the
	// rootPath by locating the prefix's full-path form via GetPath.
	relativeKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		rel, ok := stripRootPath(metaKV, key, prefix)
		if !ok {
			log.Warn("legacy tombstone GC: key not under expected prefix, skipping",
				zap.String("key", key),
				zap.String("prefix", prefix))
			continue
		}
		relativeKeys = append(relativeKeys, rel)
	}

	if len(relativeKeys) == 0 {
		return 0, nil
	}

	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	removeFn := func(partialKeys []string) error {
		return metaKV.MultiRemove(ctx, partialKeys)
	}
	if err := etcd.RemoveByBatchWithLimit(relativeKeys, maxTxnNum, removeFn); err != nil {
		return 0, err
	}

	return len(relativeKeys), nil
}

// stripRootPath converts a full etcd path returned by WalkWithPrefix into the
// relative path that MultiRemove expects. Returns (relative, true) on success,
// or ("", false) if the full key does not live under the prefix's full path.
func stripRootPath(metaKV kv.MetaKv, fullKey, prefix string) (string, bool) {
	// GetPath(prefix) yields rootPath + "/" + prefix, the full etcd key for
	// the prefix. Everything after it is the per-entry suffix.
	fullPrefix := metaKV.GetPath(prefix)
	if len(fullKey) < len(fullPrefix) || fullKey[:len(fullPrefix)] != fullPrefix {
		return "", false
	}
	return prefix + fullKey[len(fullPrefix):], true
}
