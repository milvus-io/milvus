package fileresource

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"sync"
	"time"

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
	id         int64
	refs       int
	workers    int
	info       *internalpb.FileResourceInfo // published only after a complete download
	lastActive time.Time                    // set when the final consumer releases its reference
	expiration *time.Timer
}

// AnalyzerResourceManager owns immutable resource metadata, local files and
// per-ID download flights for all analyzer consumers in this process.
// Ready resources retain an idle lease after their final consumer releases them.
type AnalyzerResourceManager struct {
	BaseManager
	downloader storage.ChunkManager
	fetch      ResourceFetcher
	lease      time.Duration
	mu         sync.Mutex
	closed     bool
	entries    map[int64]*resourceEntry
	flights    syncutil.FlightGroup[int64, struct{}]
}

// NewAnalyzerResourceManager creates a manager using the configured idle lease.
func NewAnalyzerResourceManager(downloader storage.ChunkManager, fetch ResourceFetcher) *AnalyzerResourceManager {
	return &AnalyzerResourceManager{
		BaseManager: newBaseManager(),
		downloader:  downloader,
		fetch:       fetch,
		lease:       paramtable.Get().CommonCfg.FileResourceCacheTTL.GetAsDurationByParse(),
		entries:     make(map[int64]*resourceEntry),
	}
}

// Close ends idle retention without deleting resources held by a consumer or
// download worker. Their final release performs immediate cleanup.
func (m *AnalyzerResourceManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for _, entry := range m.entries {
		m.cleanupLocked(entry)
	}
}

// Resources pins the exact IDs used by a native consumer. It must be closed on
// constructor failure, or after the analyzer/runner has been destroyed.
type Resources struct {
	manager         *AnalyzerResourceManager
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
		if r.manager == nil {
			return
		}
		r.manager.mu.Lock()
		defer r.manager.mu.Unlock()
		for _, entry := range r.entries {
			r.manager.releaseLocked(entry)
		}
	})
}

func (m *AnalyzerResourceManager) pinLocked(entry *resourceEntry) {
	if entry.expiration != nil {
		entry.expiration.Stop()
		entry.expiration = nil
	}
	entry.lastActive = time.Time{}
	entry.refs++
}

func (m *AnalyzerResourceManager) releaseLocked(entry *resourceEntry) {
	entry.refs--
	if entry.refs == 0 {
		entry.lastActive = time.Now()
	}
	m.cleanupLocked(entry)
}

// pinReadyLocked also checks the deadline so a delayed timer cannot extend a
// lease. Acquisition and removal serialize under mu, including disk cleanup.
func (m *AnalyzerResourceManager) pinReadyLocked(id int64) *resourceEntry {
	entry := m.entries[id]
	if entry == nil || entry.info == nil {
		return nil
	}
	if entry.refs == 0 && entry.workers == 0 && time.Since(entry.lastActive) >= m.lease {
		m.removeLocked(entry)
		return nil
	}
	m.pinLocked(entry)
	return entry
}

func (m *AnalyzerResourceManager) cleanupLocked(entry *resourceEntry) {
	if entry.refs != 0 || entry.workers != 0 {
		return
	}
	// Incomplete downloads are never cached. The lease starts when the last
	// reference leaves, even if an abandoned worker takes longer to unwind.
	if !m.closed && entry.info != nil {
		if remaining := time.Until(entry.lastActive.Add(m.lease)); remaining > 0 {
			if entry.expiration == nil {
				var timer *time.Timer
				timer = time.AfterFunc(remaining, func() {
					m.mu.Lock()
					defer m.mu.Unlock()
					// Stop cannot retract a callback already waiting on mu.
					// Reacquisition, eviction or a later lease invalidates it.
					if m.entries[entry.id] != entry || entry.expiration != timer {
						return
					}
					entry.expiration = nil
					m.cleanupLocked(entry)
				})
				entry.expiration = timer
			}
			return
		}
	}
	m.removeLocked(entry)
}

func (m *AnalyzerResourceManager) removeLocked(entry *resourceEntry) {
	if entry.expiration != nil {
		entry.expiration.Stop()
		entry.expiration = nil
	}
	delete(m.entries, entry.id)
	if err := os.RemoveAll(m.resourcePath(entry.id)); err != nil {
		mlog.Warn(context.TODO(), "remove unused analyzer resource", mlog.Int64("resourceID", entry.id), mlog.Err(err))
	}
}

func (m *AnalyzerResourceManager) resourcePath(id int64) string {
	return path.Join(m.localPath, demandStorageName, fmt.Sprint(id))
}

func (m *AnalyzerResourceManager) acquire(ctx context.Context, id int64, info *internalpb.FileResourceInfo) (*resourceEntry, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, merr.WrapErrServiceNotReadyMsg("analyzer resource manager is closed")
	}
	if entry := m.pinReadyLocked(id); entry != nil {
		m.mu.Unlock()
		return entry, nil
	}
	entry := m.entries[id]
	if entry == nil {
		entry = &resourceEntry{id: id}
		m.entries[id] = entry
	}
	m.pinLocked(entry)
	m.mu.Unlock()
	_, err := m.flights.Do(ctx, id, func(ctx context.Context) (struct{}, error) {
		m.mu.Lock()
		// A canceled caller can release its reference before this worker starts.
		if m.entries[id] != entry {
			m.mu.Unlock()
			return struct{}{}, ctx.Err()
		}
		entry.workers++
		ready := entry.info != nil
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			entry.workers--
			m.cleanupLocked(entry)
			m.mu.Unlock()
		}()
		if ready {
			return struct{}{}, nil
		}
		err := retryPreparation(ctx, func() error {
			timeout := paramtable.Get().CommonCfg.FileResourceDownloadTimeout.GetAsDurationByParse()
			if err := m.downloadFile(ctx, m.downloader, info, path.Join(m.resourcePath(id), path.Base(info.GetPath())), timeout); err != nil {
				return err
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			entry.info = proto.Clone(info).(*internalpb.FileResourceInfo)
			return nil
		})
		return struct{}{}, err
	})
	if err != nil {
		m.mu.Lock()
		m.releaseLocked(entry)
		m.mu.Unlock()
		return nil, err
	}
	return entry, nil
}

// Acquire fetches missing metadata in one request, then loads files through
// per-ID flights. Overlapping callers may read the same metadata, but share
// downloads. A metadata RPC belongs only to its caller and cannot cancel others.
func (m *AnalyzerResourceManager) Acquire(ctx context.Context, ids []int64, params ...string) (*Resources, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return nil, merr.WrapErrServiceNotReadyMsg("analyzer resource manager is closed")
	}
	result := &Resources{manager: m}
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
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, merr.WrapErrServiceNotReadyMsg("analyzer resource manager is closed")
		}
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			if entry := m.pinReadyLocked(id); entry != nil {
				result.entries = append(result.entries, entry)
			} else {
				missingIDs = append(missingIDs, id)
			}
		}
		m.mu.Unlock()
	}
	if len(missingIDs) > 0 || len(missingNames) > 0 {
		var infos []*internalpb.FileResourceInfo
		err := retryPreparation(ctx, func() error {
			var err error
			infos, err = m.fetch(ctx, missingIDs, missingNames)
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
				entry, err := m.acquire(loadCtx, info.GetId(), info)
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
		m.mu.Lock()
		info := entry.info
		m.mu.Unlock()
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
