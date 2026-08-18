package queryclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

const defaultMaxRetries = 3

// ViewQueryClientConfig configures the transport-neutral Legacy Query Client.
type ViewQueryClientConfig struct {
	// MaxRetries is the maximum number of attempts for each shard. Values less
	// than one use the default.
	MaxRetries int
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
	Req *internalpb.SearchRequest
}

type LegacySearchResult struct {
	Results []*internalpb.SearchResults
	Plans   []ShardPlan
}

type LegacyQueryRequest struct {
	Req *internalpb.RetrieveRequest
}

type LegacyQueryResult struct {
	Results []*internalpb.RetrieveResults
	Plans   []ShardPlan
}

// ShardPlan is the successful Phase 1 snapshot used by the Legacy execution.
type ShardPlan struct {
	ShardID   qviews.ShardID
	Version   *viewpb.QueryViewVersion
	Mvcc      *viewpb.QueryPlanMVCC
	WorkNodes []qviews.WorkNode
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
	if cfg.MaxRetries < 1 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if replicaPicker == nil {
		replicaPicker = NewPrimaryReplicaPicker()
	}
	return &legacyOnlyClient{legacy: &legacyClient{
		shardClient: newShardViewQueryClient(
			cfg.MaxRetries,
			queryPlanClient,
			queryServiceClient,
			shardResolver,
			replicaPicker,
		),
		shardResolver: shardResolver,
	}}
}
