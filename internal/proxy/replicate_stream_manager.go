package proxy

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/resource"
)

const (
	ReplicateMsgStreamTyp        = "replicate_msg_stream"
	ReplicateMsgStreamExpireTime = 30 * time.Second
)

type ReplicateStreamManager struct {
	ctx             context.Context
	factory         msgstream.Factory
	dispatcher      msgstream.UnmarshalDispatcher
	resourceManager resource.Manager
}

func NewReplicateStreamManager(ctx context.Context, factory msgstream.Factory, resourceManager resource.Manager) *ReplicateStreamManager {
	manager := &ReplicateStreamManager{
		ctx:             ctx,
		factory:         factory,
		dispatcher:      (&msgstream.ProtoUDFactory{}).NewUnmarshalDispatcher(),
		resourceManager: resourceManager,
	}
	return manager
}

func (m *ReplicateStreamManager) newMsgStreamResource(channel string) resource.NewResourceFunc {
	return func() (resource.Resource, error) {
		msgStream, err := m.factory.NewMsgStream(m.ctx)
		if err != nil {
			log.Ctx(m.ctx).Warn("failed to create msg stream", zap.String("channel", channel), zap.Error(err))
			return nil, err
		}
		msgStream.SetRepackFunc(replicatePackFunc)
		msgStream.AsProducer([]string{channel})
		msgStream.EnableProduce(true)

		res := resource.NewSimpleResource(msgStream, ReplicateMsgStreamTyp, channel, ReplicateMsgStreamExpireTime, func() {
			msgStream.Close()
		})

		return res, nil
	}
}

func (m *ReplicateStreamManager) GetReplicateMsgStream(ctx context.Context, channel string) (msgstream.MsgStream, error) {
	ctxLog := log.Ctx(ctx).With(zap.String("proxy_channel", channel))
	res, err := m.resourceManager.Get(ReplicateMsgStreamTyp, channel, m.newMsgStreamResource(channel))
	if err != nil {
		ctxLog.Warn("failed to get replicate msg stream", zap.String("channel", channel), zap.Error(err))
		return nil, err
	}
	if obj, ok := res.Get().(msgstream.MsgStream); ok && obj != nil {
		return obj, nil
	}
	ctxLog.Warn("invalid resource object", zap.Any("obj", res.Get()))
	return nil, merr.ErrInvalidStreamObj
}

func (m *ReplicateStreamManager) GetMsgDispatcher() msgstream.UnmarshalDispatcher {
	return m.dispatcher
}
