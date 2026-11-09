package renderer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Renderer post-processes query results in the Render stage.
//
// For plain queries, the renderer is a noop (or performs simple memory structure conversion).
// For BM25 search with highlighting, the renderer marks matching keywords in text fields.
type Renderer interface {
	// RequiredFields returns the field names needed by this renderer.
	// These fields must be available in the results before Render is called.
	// Used by FieldFetchPlanner to ensure fields are fetched during an earlier stage.
	RequiredFields() []string

	// RenderSearch post-processes search results.
	RenderSearch(ctx context.Context, results *internalpb.SearchResults) (*internalpb.SearchResults, error)

	// RenderRetrieve post-processes retrieve results.
	RenderRetrieve(ctx context.Context, results *internalpb.RetrieveResults) (*internalpb.RetrieveResults, error)
}

// Builder creates a Renderer from request parameters.
// It encapsulates all dependencies needed for renderer construction internally.
type Builder interface {
	// Build creates a Renderer for the given request.
	// Returns a noop renderer if the request does not require special rendering.
	Build(ctx context.Context, req *BuildRequest) (Renderer, error)
}

// BuildRequest contains the parameters needed to construct a renderer.
type BuildRequest struct {
	CollectionID int64
	// Search-specific: highlighter configuration, metric type, etc.
	// Nil for Query requests.
	SearchRequest *internalpb.SearchRequest
}

// NewNoopRenderer returns a renderer that passes results through unchanged.
func NewNoopRenderer() Renderer {
	return noopRenderer{}
}

type noopRenderer struct{}

func (noopRenderer) RequiredFields() []string { return nil }

func (noopRenderer) RenderSearch(_ context.Context, results *internalpb.SearchResults) (*internalpb.SearchResults, error) {
	return results, nil
}

func (noopRenderer) RenderRetrieve(_ context.Context, results *internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	return results, nil
}
