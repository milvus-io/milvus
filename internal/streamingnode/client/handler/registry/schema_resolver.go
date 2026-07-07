package registry

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ErrNoSchemaResolver is returned when no schema resolver is registered for the
// pchannel of the requested vchannel (no streaming node in this process, the WAL
// of that pchannel is not served locally, or it is shutting down).
var ErrNoSchemaResolver = errors.New("no schema resolver registered for channel")

// schemaResolvers routes pchannel name -> the resolver registered by the local
// wal flusher owning that pchannel's recovery storage. Unlike the process-wide
// wal manager future, presence in this map is the single source of truth, so no
// role gate is applied here: an empty map simply means no local resolver.
var schemaResolvers = typeutil.NewConcurrentMap[string, LocalSchemaResolver]()

// LocalSchemaResolver resolves the collection schema of a vchannel in effect at
// the given timetick, backed by the wal recovery storage's persisted schema
// history. The history is pruned past the flusher checkpoint (minus a tolerance),
// so lookups within the replayable window always succeed; callers MUST fall back
// to the current collection schema on any error.
type LocalSchemaResolver interface {
	GetSchema(ctx context.Context, vchannel string, timetick uint64) (*schemapb.CollectionSchema, error)
}

// RegisterLocalSchemaResolver registers the schema resolver of one pchannel.
// It is called by the local wal flusher when its recovery storage is ready;
// a re-registration after a pchannel term bump overwrites the previous one.
func RegisterLocalSchemaResolver(pchannel string, resolver LocalSchemaResolver) {
	schemaResolvers.Insert(pchannel, resolver)
	mlog.Info(context.TODO(), "register local schema resolver done", mlog.String("pchannel", pchannel))
}

// UnregisterLocalSchemaResolver removes the schema resolver of one pchannel.
// It is called by the local wal flusher on close, BEFORE closing the recovery
// storage, so a resolver obtained from the registry never outlives its backing
// storage.
func UnregisterLocalSchemaResolver(pchannel string) {
	schemaResolvers.Remove(pchannel)
	mlog.Info(context.TODO(), "unregister local schema resolver done", mlog.String("pchannel", pchannel))
}

// GetLocalSchemaResolver returns the schema resolver serving the given vchannel,
// or ErrNoSchemaResolver when the vchannel's pchannel is not served locally.
func GetLocalSchemaResolver(vchannel string) (LocalSchemaResolver, error) {
	resolver, ok := schemaResolvers.Get(funcutil.ToPhysicalChannel(vchannel))
	if !ok {
		return nil, ErrNoSchemaResolver
	}
	return resolver, nil
}
