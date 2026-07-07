package registry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

type fakeSchemaResolver struct {
	schema *schemapb.CollectionSchema
}

func (r *fakeSchemaResolver) GetSchema(ctx context.Context, vchannel string, timetick uint64) (*schemapb.CollectionSchema, error) {
	return r.schema, nil
}

func TestLocalSchemaResolverRegistry(t *testing.T) {
	defer ResetRegisterLocalSchemaResolvers()

	pchannel := "by-dev-rootcoord-dml_0"
	vchannel := "by-dev-rootcoord-dml_0_123456v0"

	// no resolver registered for the pchannel yet.
	_, err := GetLocalSchemaResolver(vchannel)
	assert.ErrorIs(t, err, ErrNoSchemaResolver)

	resolver := &fakeSchemaResolver{schema: &schemapb.CollectionSchema{Name: "test", Version: 42}}
	RegisterLocalSchemaResolver(pchannel, resolver)

	got, err := GetLocalSchemaResolver(vchannel)
	assert.NoError(t, err)
	schema, err := got.GetSchema(context.Background(), vchannel, 100)
	assert.NoError(t, err)
	assert.Equal(t, int32(42), schema.GetVersion())

	// another pchannel is still unregistered.
	_, err = GetLocalSchemaResolver("by-dev-rootcoord-dml_1_123456v0")
	assert.ErrorIs(t, err, ErrNoSchemaResolver)

	// unregister removes the resolver.
	UnregisterLocalSchemaResolver(pchannel)
	_, err = GetLocalSchemaResolver(vchannel)
	assert.ErrorIs(t, err, ErrNoSchemaResolver)
}
