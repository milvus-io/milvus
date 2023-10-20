package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/resource"
)

func TestReplicateManager(t *testing.T) {
	factory := newMockMsgStreamFactory()
	resourceManager := resource.NewManager(time.Second, 2*time.Second, nil)
	manager := NewReplicateStreamManager(context.Background(), factory, resourceManager)

	{
		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return nil, errors.New("mock msgstream fail")
		}
		_, err := manager.GetReplicateMsgStream(context.Background(), "test")
		assert.Error(t, err)
	}
	{
		mockMsgStream := newMockMsgStream()
		i := 0
		mockMsgStream.setRepack = func(repackFunc msgstream.RepackFunc) {
			i++
		}
		mockMsgStream.asProducer = func(producers []string) {
			i++
		}
		mockMsgStream.enableProduce = func(b bool) {
			i++
		}
		mockMsgStream.close = func() {
			i++
		}
		factory.f = func(ctx context.Context) (msgstream.MsgStream, error) {
			return mockMsgStream, nil
		}
		_, err := manager.GetReplicateMsgStream(context.Background(), "test")
		assert.NoError(t, err)
		assert.Equal(t, 3, i)
		time.Sleep(time.Second)
		_, err = manager.GetReplicateMsgStream(context.Background(), "test")
		assert.NoError(t, err)
		assert.Equal(t, 3, i)
		res := resourceManager.Delete(ReplicateMsgStreamTyp, "test")
		assert.NotNil(t, res)
		time.Sleep(2 * time.Second)

		_, err = manager.GetReplicateMsgStream(context.Background(), "test")
		assert.NoError(t, err)
		assert.Equal(t, 7, i)
	}
	{
		res := resourceManager.Delete(ReplicateMsgStreamTyp, "test")
		assert.NotNil(t, res)
		time.Sleep(2 * time.Second)

		res, err := resourceManager.Get(ReplicateMsgStreamTyp, "test", func() (resource.Resource, error) {
			return resource.NewResource(resource.WithObj("str")), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "str", res.Get())

		_, err = manager.GetReplicateMsgStream(context.Background(), "test")
		assert.ErrorIs(t, err, merr.ErrInvalidStreamObj)
	}

	{
		assert.NotNil(t, manager.GetMsgDispatcher())
	}
}
