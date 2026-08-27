package queryclient

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

const (
	defaultMaxAttempts         = 3
	defaultRetryInitialBackoff = 200 * time.Millisecond
	defaultRetryMaxBackoff     = 3 * time.Second
)

// ViewQueryClientConfig configures the transport-neutral Legacy Query Client.
type ViewQueryClientConfig struct {
	// MaxAttempts is the maximum number of complete Phase 1 + Phase 2 attempts
	// for each shard. Values less than one use the default.
	MaxAttempts int
	// RetryInitialBackoff is the delay after the first retryable QueryView
	// failure. Values less than or equal to zero use the default.
	RetryInitialBackoff time.Duration
	// RetryMaxBackoff caps the exponential shard retry delay. Values less than
	// RetryInitialBackoff use the default.
	RetryMaxBackoff time.Duration
}

// Client exposes only the implemented Legacy execution domain.
type Client interface {
	Legacy() LegacyClient
}

// LegacyClient executes Proxy-generated internal requests and returns raw node results.
type LegacyClient interface {
	Search(ctx context.Context, req *LegacySearchRequest) (*LegacySearchResult, error)
	Query(ctx context.Context, req *LegacyQueryRequest) (*LegacyQueryResult, error)
}

type LegacySearchRequest struct {
	// Req is read concurrently while all shards are planned. The caller must not
	// mutate it after Search begins.
	Req *internalpb.SearchRequest
}

type LegacySearchResult struct {
	Results []LegacySearchResultEntry
	Plans   []ShardPlan
}

// LegacySearchResultEntry retains the shard that produced one raw node result.
type LegacySearchResultEntry struct {
	ShardID qviews.ShardID
	Result  *internalpb.SearchResults
}

type LegacyQueryRequest struct {
	// Req is read concurrently while all shards are planned. The caller must not
	// mutate it after Query begins.
	Req *internalpb.RetrieveRequest
}

type LegacyQueryResult struct {
	Results []LegacyQueryResultEntry
	Plans   []ShardPlan
}

// LegacyQueryResultEntry retains the shard that produced one raw node result.
type LegacyQueryResultEntry struct {
	ShardID qviews.ShardID
	Result  *internalpb.RetrieveResults
}

// ShardPlan is the successful Phase 1 snapshot used by the Legacy execution.
type ShardPlan struct {
	ShardID   qviews.ShardID
	Version   *viewpb.QueryViewVersion
	Mvcc      *viewpb.QueryPlanMVCC
	WorkNodes []qviews.WorkNode
	// SkipExecution records an explicit Phase 1 decision that this shard has an
	// empty result and therefore has no Phase 2 work nodes.
	SkipExecution bool
}

// QueryPlanClient executes Phase 1 against the primary StreamingNode.
type QueryPlanClient interface {
	GetQueryPlan(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)
}

// ViewQueryServiceClient executes Phase 2 against a planned work node.
type ViewQueryServiceClient interface {
	SearchOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	QueryOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
}

type legacyOnlyClient struct {
	legacy LegacyClient
}

func (c *legacyOnlyClient) Legacy() LegacyClient {
	return c.legacy
}

// NewLegacyViewQueryClient creates a Primary-only Query Client. All production
// discovery and transport dependencies are supplied by later integration work.
func NewLegacyViewQueryClient(
	cfg ViewQueryClientConfig,
	queryPlanClient QueryPlanClient,
	queryServiceClient ViewQueryServiceClient,
	shardResolver resolver.ShardResolver,
	replicaPicker ReplicaPicker,
) Client {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = defaultMaxAttempts
	}
	if cfg.RetryInitialBackoff <= 0 {
		cfg.RetryInitialBackoff = defaultRetryInitialBackoff
	}
	if cfg.RetryMaxBackoff < cfg.RetryInitialBackoff {
		cfg.RetryMaxBackoff = max(defaultRetryMaxBackoff, cfg.RetryInitialBackoff)
	}
	if replicaPicker == nil {
		replicaPicker = NewPrimaryReplicaPicker()
	}
	return &legacyOnlyClient{legacy: &legacyClient{
		shardClient: newShardViewQueryClient(
			cfg.MaxAttempts,
			cfg.RetryInitialBackoff,
			cfg.RetryMaxBackoff,
			queryPlanClient,
			queryServiceClient,
			shardResolver,
			replicaPicker,
		),
		shardResolver: shardResolver,
	}}
}
