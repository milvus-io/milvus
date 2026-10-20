package fileresource

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"sync"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// ResourceFetcher fetches a requested batch by IDs or names (never both).
// The result must contain every requested resource; errors return no partial list.
type ResourceFetcher func(context.Context, []int64, []string) ([]*internalpb.FileResourceInfo, error)

var resourceFetcher struct {
	sync.RWMutex
	loader *resourceLoader
}

// SetResourceFetcher enables ID-based loading independently of legacy Sync modes.
func SetResourceFetcher(downloader storage.ChunkManager, fetch ResourceFetcher) {
	resourceFetcher.Lock()
	defer resourceFetcher.Unlock()
	resourceFetcher.loader = nil
	if fetch != nil {
		resourceFetcher.loader = &resourceLoader{
			BaseManager: newBaseManager(),
			downloader:  downloader,
			fetch:       fetch,
			entries:     make(map[int64]*resourceEntry),
		}
	}
}

func IsPullEnabled() bool {
	resourceFetcher.RLock()
	defer resourceFetcher.RUnlock()
	return resourceFetcher.loader != nil
}

// retryPreparation keeps transient preparation failures on the worker. Its
// context belongs to the request for metadata, or the shared flight for file
// loading. An inner timeout can retry while that owning context is alive.
func retryPreparation(ctx context.Context, work func() error) error {
	err := retry.Handle(ctx, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		err := work()
		return err != nil && ctx.Err() == nil && retry.IsRecoverable(err) && !merr.IsNonRetryableErr(err) &&
			merr.GetErrorType(err) != merr.InputError && grpcstatus.Code(err) != codes.Unimplemented, err
	}, retry.AttemptAlways())
	// retry.Handle can return its previous attempt's error after cancellation.
	// The shared lifetime, rather than that stale transient cause, ended the wait.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Demand files live outside the directories managed by legacy full-inventory Sync.
const demandStorageName = "analyzer-demand"

type resourceEntry struct {
	id      int64
	refs    int
	workers int
	info    *internalpb.FileResourceInfo // published only after a complete download
}

type resourceLoader struct {
	BaseManager
	downloader storage.ChunkManager
	fetch      ResourceFetcher
	mu         sync.Mutex
	entries    map[int64]*resourceEntry
	flights    syncutil.FlightGroup[int64, struct{}]
}

// Resources pins the exact IDs used by a native consumer. It must be closed on
// constructor failure, or after the analyzer/runner has been destroyed.
type Resources struct {
	loader          *resourceLoader
	entries         []*resourceEntry
	once            sync.Once
	extra           string
	validationExtra string
}

func (r *Resources) ExtraInfo() string { return r.extra }

func (r *Resources) ValidateAnalyzer(params string) ([]int64, error) {
	return analyzer.ValidateAnalyzer(params, r.validationExtra)
}

func (r *Resources) Close() {
	r.once.Do(func() {
		if r.loader == nil {
			return
		}
		r.loader.mu.Lock()
		defer r.loader.mu.Unlock()
		for _, entry := range r.entries {
			entry.refs--
			r.loader.cleanupLocked(entry)
		}
	})
}

func (l *resourceLoader) cleanupLocked(entry *resourceEntry) {
	if entry.refs != 0 || entry.workers != 0 {
		return
	}
	delete(l.entries, entry.id)
	if err := os.RemoveAll(l.resourcePath(entry.id)); err != nil {
		mlog.Warn(context.TODO(), "remove unused analyzer resource", mlog.Int64("resourceID", entry.id), mlog.Err(err))
	}
}

func (l *resourceLoader) resourcePath(id int64) string {
	return path.Join(l.localPath, demandStorageName, fmt.Sprint(id))
}

func (l *resourceLoader) acquire(ctx context.Context, id int64, info *internalpb.FileResourceInfo) (*resourceEntry, error) {
	l.mu.Lock()
	entry := l.entries[id]
	if entry == nil {
		entry = &resourceEntry{id: id}
		l.entries[id] = entry
	}
	entry.refs++
	ready := entry.info != nil
	l.mu.Unlock()
	if ready {
		return entry, nil
	}
	_, err := l.flights.Do(ctx, id, func(ctx context.Context) (struct{}, error) {
		l.mu.Lock()
		// A canceled caller can release its reference before this worker starts.
		if l.entries[id] != entry {
			l.mu.Unlock()
			return struct{}{}, ctx.Err()
		}
		entry.workers++
		ready := entry.info != nil
		l.mu.Unlock()
		defer func() {
			l.mu.Lock()
			entry.workers--
			l.cleanupLocked(entry)
			l.mu.Unlock()
		}()
		if ready {
			return struct{}{}, nil
		}
		err := retryPreparation(ctx, func() error {
			timeout := paramtable.Get().CommonCfg.FileResourceDownloadTimeout.GetAsDurationByParse()
			if err := l.downloadFile(ctx, l.downloader, info, path.Join(l.resourcePath(id), path.Base(info.GetPath())), timeout); err != nil {
				return err
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			entry.info = proto.Clone(info).(*internalpb.FileResourceInfo)
			return nil
		})
		return struct{}{}, err
	})
	if err != nil {
		l.mu.Lock()
		entry.refs--
		l.cleanupLocked(entry)
		l.mu.Unlock()
		return nil, err
	}
	return entry, nil
}

// Acquire fetches missing metadata in one request, then loads files through
// per-ID flights. Overlapping callers may read the same metadata, but share
// downloads. A metadata RPC belongs only to its caller and cannot cancel others.
func Acquire(ctx context.Context, ids []int64, params ...string) (*Resources, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resourceFetcher.RLock()
	loader := resourceFetcher.loader
	resourceFetcher.RUnlock()
	result := &Resources{loader: loader}
	if loader == nil {
		return result, nil
	}
	names := analyzerResourceNames(params)
	if len(names) == 0 {
		result.extra = `{ "resource_map": {} }`
		result.validationExtra = `{ "resource_map": {}, "resource_file_names": {}, "validate_resource_names": true }`
		return result, nil
	}
	var missingIDs []int64
	var missingNames []string
	if len(ids) == 0 {
		// Fresh name queries cannot join a negative read that predates Add.
		for name := range names {
			missingNames = append(missingNames, name)
		}
		sort.Strings(missingNames)
	} else {
		// Pin ready entries before fetching the misses so a concurrent final Close
		// cannot remove the files this consumer has already selected.
		seen := make(map[int64]struct{})
		loader.mu.Lock()
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			if entry := loader.entries[id]; entry != nil && entry.info != nil {
				entry.refs++
				result.entries = append(result.entries, entry)
			} else {
				missingIDs = append(missingIDs, id)
			}
		}
		loader.mu.Unlock()
	}
	if len(missingIDs) > 0 || len(missingNames) > 0 {
		var infos []*internalpb.FileResourceInfo
		err := retryPreparation(ctx, func() error {
			var err error
			infos, err = loader.fetch(ctx, missingIDs, missingNames)
			return err
		})
		if err == nil {
			err = checkFetchedResources(infos, missingIDs, missingNames)
		}
		if err != nil {
			result.Close()
			return nil, err
		}
		// Register all resource waits concurrently. A failed request releases all
		// of its waits; another request's wait keeps that resource's worker alive.
		loadCtx, cancel := context.WithCancel(ctx)
		type loadResult struct {
			entry *resourceEntry
			err   error
		}
		loaded := make(chan loadResult, len(infos))
		for _, info := range infos {
			go func() {
				entry, err := loader.acquire(loadCtx, info.GetId(), info)
				loaded <- loadResult{entry: entry, err: err}
			}()
		}
		for range infos {
			item := <-loaded
			if item.err != nil {
				if err == nil {
					err = item.err
					cancel()
				}
			} else {
				result.entries = append(result.entries, item.entry)
			}
		}
		cancel()
		if err != nil {
			result.Close()
			return nil, err
		}
	}

	resourceMap := make(map[string]int64)
	fileNames := make(map[string]string)
	for _, entry := range result.entries {
		// info is immutable after publication for this ID.
		loader.mu.Lock()
		info := entry.info
		loader.mu.Unlock()
		resourceMap[info.GetName()] = info.GetId()
		fileNames[info.GetName()] = path.Base(info.GetPath())
	}
	extra := map[string]any{"storage_name": demandStorageName, "resource_map": resourceMap}
	data, err := json.Marshal(extra)
	if err != nil {
		result.Close()
		return nil, merr.Wrap(err, "marshal analyzer resource bindings")
	}
	result.extra = string(data)
	extra["resource_file_names"] = fileNames
	extra["validate_resource_names"] = true
	data, err = json.Marshal(extra)
	if err != nil {
		result.Close()
		return nil, merr.Wrap(err, "marshal analyzer validation bindings")
	}
	result.validationExtra = string(data)
	if err := ctx.Err(); err != nil {
		result.Close()
		return nil, err
	}
	return result, nil
}

// Response order is part of the batch RPC contract. Validate the full result
// before publishing any files, including duplicate IDs returned for names.
func checkFetchedResources(infos []*internalpb.FileResourceInfo, ids []int64, names []string) error {
	if len(infos) != len(ids)+len(names) {
		return merr.WrapErrServiceInternalMsg("resource fetch returned an incomplete batch")
	}
	seen := make(map[int64]struct{}, len(infos))
	for i, info := range infos {
		if info == nil {
			return merr.WrapErrServiceInternalMsg("resource fetch returned empty metadata")
		}
		if len(ids) > 0 && info.GetId() != ids[i] || len(names) > 0 && info.GetName() != names[i] {
			return merr.WrapErrServiceInternalMsg("resource fetch returned an unexpected identity")
		}
		if _, ok := seen[info.GetId()]; ok {
			return merr.WrapErrServiceInternalMsg("resource fetch returned duplicate ID %d", info.GetId())
		}
		seen[info.GetId()] = struct{}{}
	}
	return nil
}
