//go:build test && dynamic

package viewquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestSearchTaskRunnerUsesDirectExecutionPath(t *testing.T) {
	_, err := searchTaskRunner{}.Search(context.Background(), nil, nil, &querypb.SearchRequest{}, 1)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "not implemented")
	assert.Contains(t, err.Error(), "nil collection")
}

func TestQueryTaskRunnerUsesDirectExecutionPath(t *testing.T) {
	_, err := queryTaskRunner{}.Query(context.Background(), nil, nil, &querypb.QueryRequest{}, 1)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "not implemented")
	assert.Contains(t, err.Error(), "nil collection")
}
