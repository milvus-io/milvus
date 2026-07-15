package reducer

import "github.com/milvus-io/milvus/pkg/v3/proto/internalpb"

// SearchResultReducerBuilder creates SearchResultReducer instances
// configured for individual search requests.
// The builder examines request parameters (topk, groupby, is_advanced, etc.)
// to select the appropriate reducer implementation.
type SearchResultReducerBuilder interface {
	Build(req *internalpb.SearchRequest) (SearchResultReducer, error)
}

// RetrieveResultReducerBuilder creates RetrieveResultReducer instances
// configured for individual retrieve requests.
type RetrieveResultReducerBuilder interface {
	Build(req *internalpb.RetrieveRequest) (RetrieveResultReducer, error)
}
