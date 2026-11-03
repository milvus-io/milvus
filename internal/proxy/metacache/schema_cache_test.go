package metacache

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type (
	schemaTestWAL    struct{ streaming.WALAccesser }
	schemaTestClient struct{ streaming.SchemaClient }
)

func TestCollectionInfoSchemaAccessors(t *testing.T) {
	client, wal := &schemaTestClient{}, &schemaTestWAL{}
	p := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer p.UnPatch()
	p = mockey.Mock((*schemaTestWAL).SchemaClient).Return(client).Build()
	defer p.UnPatch()
	read, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c", Version: 0})
	require.NoError(t, err)
	write, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c", Version: 2})
	require.NoError(t, err)
	var reads, writes int
	readErr := error(merr.ErrServiceUnavailable)
	p = mockey.Mock((*schemaTestClient).GetSchemaVersion).To(func(_ *schemaTestClient, _ context.Context, req streaming.GetSchemaVersionRequest) (*SchemaInfo, error) {
		reads++
		require.Equal(t, "p_1v0", req.VChannel)
		require.Zero(t, req.Version)
		require.False(t, req.BypassCache)
		if readErr != nil {
			return nil, readErr
		}
		return read, nil
	}).Build()
	defer p.UnPatch()
	p = mockey.Mock((*schemaTestClient).GetCurrentWriteSchema).To(func(_ *schemaTestClient, _ context.Context, req streaming.GetCurrentWriteSchemaRequest) (*SchemaInfo, error) {
		writes++
		require.Equal(t, "p_1v0", req.VChannel)
		require.False(t, req.BypassCache)
		return write, nil
	}).Build()
	defer p.UnPatch()
	info := &CollectionInfo{CollID: 1, ReadSchemaVersion: 0, VChannels: []string{"p_1v0"}}
	actual, err := info.GetWriteSchema(t.Context())
	require.NoError(t, err)
	require.Same(t, write, actual)
	require.Zero(t, reads, "write admission does not depend on published read history")
	_, err = info.GetReadSchema(t.Context())
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, 1, writes, "an unavailable exact version must not fall back to write schema")
	readErr = nil
	for range 2 {
		actual, err = info.GetReadSchema(t.Context())
		require.NoError(t, err)
		require.Same(t, read, actual, "return the shared SchemaInfo without copying or rebuilding")
	}
	require.Zero(t, info.ReadSchemaVersion)
	_, err = (&CollectionInfo{CollID: 2}).GetReadSchema(t.Context())
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	_, err = (&CollectionInfo{CollID: 2}).GetWriteSchema(t.Context())
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
}
