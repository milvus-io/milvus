package streamingcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/pkg/mocks/mock_kv"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
)

func TestCatalog(t *testing.T) {
	kv := mock_kv.NewMockMetaKv(t)

	kvStorage := make(map[string]string)
	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) ([]string, []string, error) {
		keys := make([]string, 0, len(kvStorage))
		vals := make([]string, 0, len(kvStorage))
		for k, v := range kvStorage {
			keys = append(keys, k)
			vals = append(vals, v)
		}
		return keys, vals, nil
	})
	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, kvs map[string]string) error {
		for k, v := range kvs {
			kvStorage[k] = v
		}
		return nil
	})

	catalog := NewCataLog(kv)
	metas, err := catalog.ListPChannel(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, metas)

	err = catalog.SavePChannels(context.Background(), []*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{Name: "test", Term: 1},
			Node:    &streamingpb.StreamingNodeInfo{ServerId: 1},
		},
		{
			Channel: &streamingpb.PChannelInfo{Name: "test2", Term: 1},
			Node:    &streamingpb.StreamingNodeInfo{ServerId: 1},
		},
	})
	assert.NoError(t, err)

	metas, err = catalog.ListPChannel(context.Background())
	assert.NoError(t, err)
	assert.Len(t, metas, 2)

	// error path.
	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, errors.New("load error"))
	metas, err = catalog.ListPChannel(context.Background())
	assert.Error(t, err)
	assert.Nil(t, metas)

	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("save error"))
	assert.Error(t, err)
}
