package rootcoord

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	legacyGCBatchSize = 2000
	legacyGCInterval  = 5 * time.Second
)

// errBatchFull is a sentinel error to stop WalkWithPrefix after collecting enough keys.
var errBatchFull = fmt.Errorf("legacy GC batch full")

var legacyGCOnce sync.Once

// StartLegacySnapshotGC starts a background goroutine to clean up legacy
// snapshot keys (prefix "snapshots/") left from before the SuffixSnapshot removal.
// The goroutine self-terminates once all keys are cleaned.
// Safe to call multiple times — only the first call starts the goroutine.
//
// Safety: it ONLY operates on the "snapshots/" prefix, which is disjoint from all
// plain key paths (root-coord/, datacoord-meta/, etc.). WalkWithPrefix physically
// cannot match a plain key, so this cannot corrupt any active metadata.
func StartLegacySnapshotGC(ctx context.Context, metaKV kv.MetaKv) {
	legacyGCOnce.Do(func() {
		go runLegacySnapshotGC(ctx, metaKV)
	})
}

func runLegacySnapshotGC(ctx context.Context, metaKV kv.MetaKv) {
	logger := log.Ctx(ctx)
	logger.Info("legacy snapshot GC started",
		zap.String("prefix", SnapshotPrefix+"/"),
		zap.Int("batchSize", legacyGCBatchSize),
		zap.Duration("interval", legacyGCInterval))

	ticker := time.NewTicker(legacyGCInterval)
	defer ticker.Stop()

	totalDeleted := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			logger.Info("legacy snapshot GC stopped (context canceled)",
				zap.Int("totalDeleted", totalDeleted),
				zap.Duration("totalElapsed", time.Since(startTime)))
			return
		case <-ticker.C:
			deleted, err := gcLegacySnapshotBatch(ctx, metaKV)
			if err != nil {
				logger.Warn("legacy snapshot GC batch error",
					zap.Error(err),
					zap.Int("totalDeleted", totalDeleted))
				continue
			}
			if deleted == 0 {
				logger.Info("legacy snapshot GC complete",
					zap.Int("totalDeleted", totalDeleted),
					zap.Duration("totalElapsed", time.Since(startTime)))
				return
			}
			totalDeleted += deleted
			logger.Info("legacy snapshot GC batch done",
				zap.Int("deleted", deleted),
				zap.Int("totalDeleted", totalDeleted),
				zap.Duration("elapsed", time.Since(startTime)))
		}
	}
}

// gcLegacySnapshotBatch collects up to legacyGCBatchSize snapshot keys and
// deletes them in batched etcd transactions.
// Returns (0, nil) when no more snapshot keys exist.
func gcLegacySnapshotBatch(ctx context.Context, metaKV kv.MetaKv) (int, error) {
	// Phase 1: Collect keys under "snapshots/" prefix.
	// MetaKv internally prepends rootPath (e.g. "by-dev/meta/"), so this scans
	// "by-dev/meta/snapshots/...". Plain keys live under "by-dev/meta/root-coord/",
	// "by-dev/meta/datacoord-meta/", etc. — no overlap is possible.
	keys := make([]string, 0, legacyGCBatchSize)
	err := metaKV.WalkWithPrefix(ctx, SnapshotPrefix+"/", legacyGCBatchSize,
		func(k []byte, v []byte) error {
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

	// Phase 2: Strip rootPath prefix to get relative keys.
	// WalkWithPrefix returns full etcd paths (with rootPath), but MultiRemove
	// expects relative paths (it adds rootPath internally).
	// GetPath("x") returns rootPath + "/" + "x". So GetPath(SnapshotPrefix) gives
	// us the full etcd prefix for snapshot keys, which we can use to strip.
	snapshotFullPrefix := metaKV.GetPath(SnapshotPrefix)
	relativeKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, snapshotFullPrefix) {
			log.Warn("legacy snapshot GC: key not under expected prefix, skipping",
				zap.String("key", key), zap.String("expectedPrefix", snapshotFullPrefix))
			continue
		}
		// Reconstruct relative key: "snapshots/" + suffix after full prefix
		rel := SnapshotPrefix + key[len(snapshotFullPrefix):]
		relativeKeys = append(relativeKeys, rel)
	}

	if len(relativeKeys) == 0 {
		return 0, nil
	}

	// Phase 3: Delete in batched transactions.
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	removeFn := func(partialKeys []string) error {
		return metaKV.MultiRemove(ctx, partialKeys)
	}
	if err := etcd.RemoveByBatchWithLimit(relativeKeys, maxTxnNum, removeFn); err != nil {
		return 0, err
	}

	return len(relativeKeys), nil
}
