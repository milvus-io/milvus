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

// SchemaSnapshot and all its nested schema nodes are shared and immutable.
// The full WAL schema remains available to node consumers; Proxy's user-field
// projection and indices are built once and share the same field nodes.
type SchemaSnapshot struct {
	Schema   *schemapb.CollectionSchema
	userOnce sync.Once
	user     *schemautil.SchemaInfo
	userErr  error
}

func (s *SchemaSnapshot) UserSchema() (*schemautil.SchemaInfo, error) {
	s.userOnce.Do(func() {
		schema := schemautil.ShallowSchema(s.Schema)
		schema.Fields = make([]*schemapb.FieldSchema, 0, len(s.Schema.Fields))
		for _, field := range s.Schema.Fields {
			if !common.IsSystemField(field.GetFieldID()) {
				schema.Fields = append(schema.Fields, field)
			}
		}
		s.user, s.userErr = schemautil.NewSchemaInfo(schema)
	})
	return s.user, s.userErr
}

type (
	GetCurrentWriteSchemaRequest struct{ VChannel string }
	GetSchemaVersionRequest      struct {
		VChannel string
		Version  int32
	}
)

type SchemaClient interface {
	// GetCurrentWriteSchema confirms write admission with SN and reuses the
	// immutable body and derived indices for the returned collection/version.
	GetCurrentWriteSchema(context.Context, GetCurrentWriteSchemaRequest) (*SchemaSnapshot, error)
	// GetSchemaVersion resolves an exact retained version, including zero.
	GetSchemaVersion(context.Context, GetSchemaVersionRequest) (*SchemaSnapshot, error)
}

type schemaIdentity struct {
	collectionID int64
	version      int32
}
type writeFlightKey struct {
	channel    string
	generation uint64
}

type schemaClient struct {
	handlerClient handler.HandlerClient
	bodies        *schemaCache[schemaIdentity, *SchemaSnapshot]
	// Write selectors contain only version numbers, never schema references.
	writeVersions *schemaCache[int64, int32]
	mu            sync.Mutex
	generation    uint64
	reads         syncutil.FlightGroup[schemaIdentity, *SchemaSnapshot]
	writes        syncutil.FlightGroup[writeFlightKey, *SchemaSnapshot]
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
}

func newSchemaClient(handlerClient handler.HandlerClient, ttl time.Duration) *schemaClient {
	ctx, cancel := context.WithCancel(context.Background())
	c := &schemaClient{handlerClient: handlerClient, bodies: newSchemaCache[schemaIdentity, *SchemaSnapshot](ttl), writeVersions: newSchemaCache[int64, int32](ttl), ctx: ctx, cancel: cancel, done: make(chan struct{})}
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
}

func (c *schemaClient) GetCurrentWriteSchema(ctx context.Context, req GetCurrentWriteSchemaRequest) (*SchemaSnapshot, error) {
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
	key := writeFlightKey{req.VChannel, c.generation}
	c.mu.Unlock()
	// Confirm on every operation, including after a local validation failure.
	// Concurrent callers share the RPC; no published read version is required.
	return c.writes.Do(ctx, key, func(ctx context.Context) (*SchemaSnapshot, error) {
		response, err := c.getCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: req.VChannel, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}})
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.ctx.Err() != nil {
			return nil, ErrWALAccesserClosed
		}
		body := c.bodies.loadOrStore(schemaIdentity{id, response.Schema.Version}, response)
		if c.generation == key.generation {
			c.writeVersions.store(id, response.Schema.Version)
		}
		return body, nil
	})
}

func (c *schemaClient) GetSchemaVersion(ctx context.Context, req GetSchemaVersionRequest) (*SchemaSnapshot, error) {
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
	key := schemaIdentity{id, req.Version}
	if cached, ok := c.bodies.load(key); ok {
		return cached, nil
	}
	return c.reads.Do(ctx, key, func(ctx context.Context) (*SchemaSnapshot, error) {
		if cached, ok := c.bodies.load(key); ok {
			return cached, nil
		}
		response, err := c.getCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: req.VChannel, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: req.Version}})
		if err != nil {
			return nil, err
		}
		if response.Schema.Version != req.Version {
			return nil, merr.WrapErrServiceInternalMsg("streaming returned a mismatched schema version")
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.ctx.Err() != nil {
			return nil, ErrWALAccesserClosed
		}
		return c.bodies.loadOrStore(key, response), nil
	})
}

func (c *schemaClient) getCollectionSchema(ctx context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*SchemaSnapshot, error) {
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
	return &SchemaSnapshot{Schema: resp.GetSchema()}, nil
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
	c.generation++
}
