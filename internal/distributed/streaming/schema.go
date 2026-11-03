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
	// BypassCache also invalidates other versions' non-versioned metadata.
	GetSchemaVersion(context.Context, GetSchemaVersionRequest) (*schemautil.SchemaInfo, error)
}

type schemaIdentity struct {
	collectionID int64
	version      int32
}
type writeFlightKey struct {
	channel     string
	generation  uint64
	bypassCache bool
}

type readFlightKey struct {
	schemaIdentity
	generation  uint64
	bypassCache bool
}

type schemaClient struct {
	handlerClient handler.HandlerClient
	bodies        *schemaCache[schemaIdentity, cachedSchema]
	// Write selectors contain only version numbers, never schema references.
	writeVersions *schemaCache[int64, int32]
	mu            sync.Mutex
	generation    uint64
	// Epochs expire with idle collections; the monotonic allocator prevents ABA
	// when a late RPC outlives its collection's epoch. All access holds mu.
	epochs *schemaCache[int64, uint64]
	reads  syncutil.FlightGroup[readFlightKey, *schemautil.SchemaInfo]
	writes syncutil.FlightGroup[writeFlightKey, *schemautil.SchemaInfo]
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func newSchemaClient(handlerClient handler.HandlerClient, ttl time.Duration) *schemaClient {
	ctx, cancel := context.WithCancel(context.Background())
	c := &schemaClient{handlerClient: handlerClient, bodies: newSchemaCache[schemaIdentity, cachedSchema](ttl), writeVersions: newSchemaCache[int64, int32](ttl), epochs: newSchemaCache[int64, uint64](ttl), ctx: ctx, cancel: cancel, done: make(chan struct{})}
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
				c.bodies.expire(now)
				c.writeVersions.expire(now)
				c.mu.Lock()
				c.epochs.expire(now)
				c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bodies.clear()
	c.writeVersions.clear()
	c.epochs.clear()
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
	c.mu.Lock()
	key := writeFlightKey{req.VChannel, c.schemaEpochLocked(id, req.BypassCache), req.BypassCache}
	if !req.BypassCache {
		if info := c.cachedWriteSchemaLocked(id, key.generation); info != nil {
			c.mu.Unlock()
			return info, nil
		}
	}
	c.mu.Unlock()
	// Concurrent misses share the RPC. Recheck after joining the flight in case
	// an earlier flight populated the cache after the initial lookup.
	return c.writes.Do(ctx, key, func(ctx context.Context) (*schemautil.SchemaInfo, error) {
		if !req.BypassCache {
			c.mu.Lock()
			info := c.cachedWriteSchemaLocked(id, key.generation)
			c.mu.Unlock()
			if info != nil {
				return info, nil
			}
		}
		response, err := c.getCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: req.VChannel, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}})
		if err != nil {
			return nil, err
		}
		body, err := c.cacheSchema(schemaIdentity{id, response.Version}, response, key.generation)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.ctx.Err() != nil {
			return nil, ErrWALAccesserClosed
		}
		if epoch, ok := c.epochs.load(id); ok && epoch == key.generation {
			c.writeVersions.store(id, response.Version)
		}
		return body, err
	})
}

func (c *schemaClient) cachedWriteSchemaLocked(id int64, epoch uint64) *schemautil.SchemaInfo {
	version, ok := c.writeVersions.load(id)
	if !ok {
		return nil
	}
	if cached, ok := c.bodies.load(schemaIdentity{id, version}); ok && cached.epoch == epoch {
		return cached.info
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
	c.mu.Lock()
	key := readFlightKey{identity, c.schemaEpochLocked(id, req.BypassCache), req.BypassCache}
	if !req.BypassCache {
		if cached, ok := c.bodies.load(identity); ok && cached.epoch == key.generation {
			c.mu.Unlock()
			return cached.info, nil
		}
	}
	c.mu.Unlock()
	return c.reads.Do(ctx, key, func(ctx context.Context) (*schemautil.SchemaInfo, error) {
		if !req.BypassCache {
			if cached, ok := c.bodies.load(identity); ok && cached.epoch == key.generation {
				return cached.info, nil
			}
		}
		response, err := c.getCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: req.VChannel, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: req.Version}})
		if err != nil {
			return nil, err
		}
		if response.Version != req.Version {
			return nil, merr.WrapErrServiceInternalMsg("streaming returned a mismatched schema version")
		}
		return c.cacheSchema(identity, response, key.generation)
	})
}

// schemaEpochLocked separates a refresh from older read and write flights.
// Refreshing one collection does not prevent another collection's cache fill.
func (c *schemaClient) schemaEpochLocked(id int64, refresh bool) uint64 {
	if epoch, ok := c.epochs.load(id); ok && !refresh {
		return epoch
	}
	c.generation++
	c.epochs.store(id, c.generation)
	// An exact-version refresh can populate a body without confirming that it
	// is still writable. Never combine that body with an older write selector.
	c.writeVersions.erase(id)
	return c.generation
}

// A refresh changes the collection epoch, invalidating all retained versions'
// metadata without scanning the cache. Old entries expire unless fetched again.
type cachedSchema struct {
	epoch uint64
	info  *schemautil.SchemaInfo
}

// cacheSchema builds field indices outside the client lock. Publication rechecks
// the epoch and cache because another read, write or refresh can finish meanwhile.
func (c *schemaClient) cacheSchema(key schemaIdentity, response *schemapb.CollectionSchema, epoch uint64) (*schemautil.SchemaInfo, error) {
	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, ErrWALAccesserClosed
	}
	if current, ok := c.epochs.load(key.collectionID); ok && current == epoch {
		if cached, ok := c.bodies.load(key); ok && cached.epoch == epoch {
			c.mu.Unlock()
			return cached.info, nil
		}
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
	if current, ok := c.epochs.load(key.collectionID); ok && current == epoch {
		if cached, ok := c.bodies.load(key); ok && cached.epoch == epoch {
			return cached.info, nil
		}
		c.bodies.store(key, cachedSchema{epoch, info})
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
	c.mu.Lock()
	defer c.mu.Unlock()
	// Changing the flight generation prevents an RPC predating this error from
	// publishing a stale write selector or being joined by a later request.
	c.schemaEpochLocked(id, true)
}
