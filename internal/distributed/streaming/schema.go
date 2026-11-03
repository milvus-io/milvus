package streaming

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type (
	GetCurrentWriteSchemaRequest struct {
		VChannel string
		// BypassCache fetches a new immutable body even when its version is cached.
		BypassCache bool
	}
	GetSchemaVersionRequest struct {
		VChannel    string
		Version     int32
		BypassCache bool
	}
)

// SchemaClient returns shared immutable SchemaInfo values. Callers must not
// mutate their protobufs or indices. The schema includes system fields.
type SchemaClient interface {
	// GetCurrentWriteSchema caches the current write version and immutable body.
	// A schema mismatch on append invalidates the selector for the next lookup.
	GetCurrentWriteSchema(context.Context, GetCurrentWriteSchemaRequest) (*schemautil.SchemaInfo, error)
	// GetSchemaVersion resolves an exact retained version, including zero.
	// BypassCache refreshes only the requested version.
	GetSchemaVersion(context.Context, GetSchemaVersionRequest) (*schemautil.SchemaInfo, error)
}

type schemaIdentity struct {
	collectionID int64
	version      int32
}

// Entry identity is the refresh token: replacing or expiring an entry prevents
// older flights from publishing into its replacement, without a separate epoch map.
type cachedSchema struct {
	info *schemautil.SchemaInfo
}

const unknownWriteSchemaVersion int32 = -1

type cachedWriteVersion struct {
	version int32
}

type writeFlightKey struct {
	channel string
	entry   *cachedWriteVersion
}

type readFlightKey struct {
	schemaIdentity
	entry *cachedSchema
}

const schemaCacheShardCount = 32

type schemaClient struct {
	handlerClient handler.HandlerClient
	shards        [schemaCacheShardCount]*schemaClientShard
	reads         syncutil.FlightGroup[readFlightKey, *schemautil.SchemaInfo]
	writes        syncutil.FlightGroup[writeFlightKey, *schemautil.SchemaInfo]
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
}

// Schema bodies expire and refresh independently by collection and version.
// The current-write selector is removed on invalidation, matching body eviction
// or close; it has no independent TTL.
// Both maps share a shard lock for atomic publication.
type schemaClientShard struct {
	mu            sync.Mutex
	bodies        *schemaCache[schemaIdentity, *cachedSchema]
	writeVersions map[int64]*cachedWriteVersion
	ctx           context.Context
}

func (c *schemaClient) shard(collectionID int64) *schemaClientShard {
	return c.shards[uint64(collectionID)%schemaCacheShardCount]
}

func (s *schemaClientShard) expire(now time.Time) {
	for {
		s.mu.Lock()
		more := s.bodies.expire(now)
		s.mu.Unlock()
		if !more {
			return
		}
	}
}

func newSchemaClient(handlerClient handler.HandlerClient, ttl time.Duration) *schemaClient {
	ctx, cancel := context.WithCancel(context.Background())
	c := &schemaClient{handlerClient: handlerClient, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	for i := range c.shards {
		shard := &schemaClientShard{
			bodies:        newSchemaCache[schemaIdentity, *cachedSchema](ttl),
			writeVersions: make(map[int64]*cachedWriteVersion),
			ctx:           ctx,
		}
		shard.bodies.onEvict = func(key schemaIdentity, _ *cachedSchema) {
			if write := shard.writeVersions[key.collectionID]; write != nil && write.version == key.version {
				delete(shard.writeVersions, key.collectionID)
			}
		}
		c.shards[i] = shard
	}
	go func() {
		defer close(c.done)
		interval := ttl / 2
		if interval <= 0 || interval > time.Minute {
			interval = time.Minute
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				for _, shard := range c.shards {
					shard.expire(now)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return c
}

func (c *schemaClient) close() {
	c.cancel()
	<-c.done
	for _, shard := range c.shards {
		shard.mu.Lock()
		shard.bodies.clear()
		clear(shard.writeVersions)
		shard.mu.Unlock()
	}
}

func (c *schemaClient) GetCurrentWriteSchema(ctx context.Context, req GetCurrentWriteSchemaRequest) (*schemautil.SchemaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ctx.Err() != nil {
		return nil, ErrWALAccesserClosed
	}
	_, id, _, err := funcutil.ParseVChannel(req.VChannel)
	if err != nil {
		return nil, err
	}
	shard := c.shard(id)
	shard.mu.Lock()
	entry, ok := shard.writeVersions[id]
	if ok && !req.BypassCache {
		if info := shard.cachedWriteSchemaLocked(id, entry); info != nil {
			shard.mu.Unlock()
			return info, nil
		}
		if entry.version != unknownWriteSchemaVersion {
			_, ok = shard.bodies.load(schemaIdentity{id, entry.version})
		}
	}
	if !ok || req.BypassCache {
		entry = &cachedWriteVersion{version: unknownWriteSchemaVersion}
		shard.writeVersions[id] = entry
	}
	key := writeFlightKey{req.VChannel, entry}
	shard.mu.Unlock()
	return c.writes.Do(ctx, key, func(ctx context.Context) (*schemautil.SchemaInfo, error) {
		if !req.BypassCache {
			shard.mu.Lock()
			info := shard.cachedWriteSchemaLocked(id, entry)
			shard.mu.Unlock()
			if info != nil {
				return info, nil
			}
		}
		response, err := c.getCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: req.VChannel, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}})
		if err != nil {
			return nil, err
		}
		identity := schemaIdentity{id, response.Version}
		shard.mu.Lock()
		var body *cachedSchema
		if current, ok := shard.writeVersions[id]; ok && current == entry {
			body = shard.schemaEntryLocked(identity, req.BypassCache)
			entry.version = response.Version
		}
		shard.mu.Unlock()
		return shard.cacheSchema(identity, response, body)
	})
}

func (c *schemaClientShard) cachedWriteSchemaLocked(id int64, entry *cachedWriteVersion) *schemautil.SchemaInfo {
	current, ok := c.writeVersions[id]
	if !ok || current != entry || entry.version == unknownWriteSchemaVersion {
		return nil
	}
	if body, ok := c.bodies.load(schemaIdentity{id, entry.version}); ok {
		return body.info
	}
	return nil
}

func (c *schemaClient) GetSchemaVersion(ctx context.Context, req GetSchemaVersionRequest) (*schemautil.SchemaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ctx.Err() != nil {
		return nil, ErrWALAccesserClosed
	}
	_, id, _, err := funcutil.ParseVChannel(req.VChannel)
	if err != nil {
		return nil, err
	}
	identity := schemaIdentity{id, req.Version}
	shard := c.shard(id)
	shard.mu.Lock()
	entry := shard.schemaEntryLocked(identity, req.BypassCache)
	if req.BypassCache {
		// The next write must resolve its current version from SN, independently
		// of which exact version this refresh requested.
		delete(shard.writeVersions, id)
	}
	if entry.info != nil {
		shard.mu.Unlock()
		return entry.info, nil
	}
	key := readFlightKey{identity, entry}
	shard.mu.Unlock()
	return c.reads.Do(ctx, key, func(ctx context.Context) (*schemautil.SchemaInfo, error) {
		shard.mu.Lock()
		cached, ok := shard.bodies.load(identity)
		if ok && cached == entry && cached.info != nil {
			info := cached.info
			shard.mu.Unlock()
			return info, nil
		}
		shard.mu.Unlock()
		response, err := c.getCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: req.VChannel, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: req.Version}})
		if err != nil {
			return nil, err
		}
		if response.Version != req.Version {
			return nil, merr.WrapErrServiceInternalMsg("streaming returned a mismatched schema version")
		}
		return shard.cacheSchema(identity, response, entry)
	})
}

func (c *schemaClientShard) schemaEntryLocked(key schemaIdentity, refresh bool) *cachedSchema {
	if entry, ok := c.bodies.load(key); ok && !refresh {
		return entry
	}
	entry := &cachedSchema{}
	c.bodies.store(key, entry)
	return entry
}

// cacheSchema builds field indices outside the lock. The entry token is checked
// again on publication so expired or refreshed flights cannot overwrite a new one.
func (c *schemaClientShard) cacheSchema(key schemaIdentity, response *schemapb.CollectionSchema, entry *cachedSchema) (*schemautil.SchemaInfo, error) {
	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, ErrWALAccesserClosed
	}
	if current, ok := c.bodies.load(key); ok && current == entry && current.info != nil {
		info := current.info
		c.mu.Unlock()
		return info, nil
	}
	c.mu.Unlock()
	info, err := schemautil.NewSchemaInfo(response)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx.Err() != nil {
		return nil, ErrWALAccesserClosed
	}
	if current, ok := c.bodies.load(key); ok && current == entry {
		if current.info != nil {
			return current.info, nil
		}
		current.info = info
	}
	return info, nil
}

func (c *schemaClient) getCollectionSchema(ctx context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*schemapb.CollectionSchema, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	resp, err := c.handlerClient.GetCollectionSchema(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resp.GetSchema() == nil {
		return nil, merr.WrapErrServiceInternalMsg("streaming returned a nil schema")
	}
	return resp.GetSchema(), nil
}

func (c *schemaClient) invalidateWriteSchema(channel string) {
	_, id, _, err := funcutil.ParseVChannel(channel)
	if err != nil {
		return
	}
	shard := c.shard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	// Existing schema versions remain valid. Only the current-write selector
	// and its pending flights become stale after an append mismatch.
	delete(shard.writeVersions, id)
}
