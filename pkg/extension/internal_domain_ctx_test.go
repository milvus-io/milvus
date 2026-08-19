package extension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInternalDomainMarkRoundTrips(t *testing.T) {
	ctx := context.Background()
	assert.False(t, FromInternalDomain(ctx), "an unmarked context must not read as internal")
	assert.True(t, FromInternalDomain(WithInternalDomain(ctx)))
}

func TestInternalDomainMarkCannotBeForgedWithAStringKey(t *testing.T) {
	// Middleware stuffing string keys into the same context - gin, for one -
	// must not be able to collide with the mark.
	ctx := context.WithValue(context.Background(), "milvus-internal-domain", true) //nolint:staticcheck
	assert.False(t, FromInternalDomain(ctx))
}
