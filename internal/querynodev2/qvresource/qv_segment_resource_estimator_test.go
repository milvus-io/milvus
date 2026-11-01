//go:build test && dynamic

package qvresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestQueryViewSegmentResourceEstimator_DelegatesToLoader(t *testing.T) {
	reservation := &fakeQVResourceReservation{}
	loader := &fakeQVResourceLoader{reservation: reservation}
	estimator := newQueryViewSegmentResourceEstimator(loader)
	info := &querypb.SegmentLoadInfo{SegmentID: 10}

	got, err := estimator.Reserve(context.Background(), info, fakeQVCollectionRuntime{collectionID: 1})
	require.NoError(t, err)
	assert.Same(t, info, loader.info)
	assert.Same(t, reservation, got)
}
