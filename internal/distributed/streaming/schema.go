package streaming

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
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
// mutate their protobufs or indices. FullSchema is available to native consumers.
type SchemaClient interface {
	// GetCurrentWriteSchema confirms write admission with SN and reuses the
	// immutable body and derived indices for the returned collection/version.
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
	c.mu.Unlock()
	// Confirm on every operation, including after a local validation failure.
	// Concurrent callers share the RPC; no published read version is required.
	return c.writes.Do(ctx, key, func(ctx context.Context) (*schemautil.SchemaInfo, error) {
		response, err := c.getCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: req.VChannel, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}})
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.ctx.Err() != nil {
			return nil, ErrWALAccesserClosed
		}
		body, err := c.cacheSchemaLocked(schemaIdentity{id, response.Version}, response, key.generation)
		if epoch, ok := c.epochs.load(id); err == nil && ok && epoch == key.generation {
			c.writeVersions.store(id, response.Version)
		}
		return body, err
	})
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
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.ctx.Err() != nil {
			return nil, ErrWALAccesserClosed
		}
		return c.cacheSchemaLocked(identity, response, key.generation)
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
	return c.generation
}

// A refresh changes the collection epoch, invalidating all retained versions'
// metadata without scanning the cache. Old entries expire unless fetched again.
type cachedSchema struct {
	epoch uint64
	info  *schemautil.SchemaInfo
}

func (c *schemaClient) cacheSchemaLocked(key schemaIdentity, response *schemapb.CollectionSchema, epoch uint64) (*schemautil.SchemaInfo, error) {
	current, ok := c.epochs.load(key.collectionID)
	publish := ok && current == epoch
	if publish {
		if cached, ok := c.bodies.load(key); ok && cached.epoch == epoch {
			return cached.info, nil
		}
	}
	info, err := newSchemaInfo(response)
	if err != nil {
		return nil, err
	}
	if publish {
		c.bodies.store(key, cachedSchema{epoch, info})
	}
	return info, nil
}

// Build the user-field projection and indices only when publishing a new body.
// Native node consumers retain the complete immutable schema from the RPC.
func newSchemaInfo(full *schemapb.CollectionSchema) (*schemautil.SchemaInfo, error) {
	user := schemautil.ShallowSchema(full)
	user.Fields = make([]*schemapb.FieldSchema, 0, len(full.Fields))
	for _, field := range full.Fields {
		if !common.IsSystemField(field.GetFieldID()) {
			user.Fields = append(user.Fields, field)
		}
	}
	info, err := schemautil.NewSchemaInfo(user)
	if err != nil {
		return nil, err
	}
	info.FullSchema = full
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
	c.writeVersions.erase(id)
	// Changing the flight generation prevents an RPC predating this error from
	// publishing a stale write selector or being joined by a later request.
	c.schemaEpochLocked(id, true)
}
