package fileresource

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// ResourceFetcher fetches a requested batch by IDs or names (never both).
// The result must contain every requested resource; errors return no partial list.
type ResourceFetcher func(context.Context, []int64, []string) ([]*internalpb.FileResourceInfo, error)

var analyzerResources struct {
	sync.RWMutex
	manager *AnalyzerResourceManager
}

// SetResourceFetcher installs the process-wide analyzer resource manager,
// independently of legacy Sync modes. Installation happens before consumers
// start; replacing it requires the previous consumers to have stopped.
func SetResourceFetcher(downloader storage.ChunkManager, fetch ResourceFetcher) {
	analyzerResources.Lock()
	defer analyzerResources.Unlock()
	if analyzerResources.manager != nil {
		analyzerResources.manager.Close()
	}
	analyzerResources.manager = nil
	if fetch != nil {
		analyzerResources.manager = NewAnalyzerResourceManager(downloader, fetch)
	}
}

func IsPullEnabled() bool {
	analyzerResources.RLock()
	defer analyzerResources.RUnlock()
	return analyzerResources.manager != nil
}

// CloseAnalyzerResourceManager releases idle files at server shutdown. Active
// consumers keep their files until they release their references.
func CloseAnalyzerResourceManager() {
	analyzerResources.RLock()
	defer analyzerResources.RUnlock()
	if analyzerResources.manager != nil {
		analyzerResources.manager.Close()
	}
}

// Acquire uses the shared manager for analyzer validation, execution and runner
// construction. Nodes without a manager retain their legacy resource bindings.
func Acquire(ctx context.Context, ids []int64, params ...string) (*Resources, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	analyzerResources.RLock()
	manager := analyzerResources.manager
	analyzerResources.RUnlock()
	if manager == nil {
		return &Resources{}, nil
	}
	return manager.Acquire(ctx, ids, params...)
}
