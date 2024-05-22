package walmanager

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

// OpenOption is used to open a wal manager.
type OpenOption struct {
	InterceptorBuilders []wal.InterceptorBuilder // used to open new wal instance.
}

// OpenManager create a wal manager.
func OpenManager(opt *OpenOption) (Manager, error) {
	mqType := util.MustSelectMQType()
	log.Info("open wal manager", zap.String("mqType", mqType))
	opener, err := wal.MustGetBuilder(mqType).Build()
	if err != nil {
		return nil, err
	}
	return newManager(opener, opt), nil
}

// newManager create a wal manager.
func newManager(opener wal.Opener, opt *OpenOption) Manager {
	return &managerImpl{
		lifetime: lifetime.NewLifetime(lifetime.Working),
		wltMap:   typeutil.NewConcurrentMap[string, *walLifetime](),
		opener:   opener,
		openOpt:  opt,
	}
}

// All management operation for a wal will be serialized with order of term.
type managerImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]

	wltMap  *typeutil.ConcurrentMap[string, *walLifetime]
	opener  wal.Opener  // wal allocator
	openOpt *OpenOption // used to allocate timestamp for wal and broadcast the wal timetick message.
}

// Open opens a wal instance for the channel on this Manager.
func (m *managerImpl) Open(ctx context.Context, channel *logpb.PChannelInfo) (err error) {
	// reject operation if manager is closing.
	if m.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("wal manager is closed")
	}
	defer func() {
		m.lifetime.Done()
		if err != nil {
			log.Warn("open wal failed", zap.Error(err), zap.String("channel", channel.Name), zap.Int64("term", channel.Term))
			return
		}
		log.Info("open wal success", zap.String("channel", channel.Name), zap.Int64("term", channel.Term))
	}()

	return m.getWALLifetime(channel.Name).Open(ctx, &wal.BasicOpenOption{
		Channel: channel,
	})
}

// Remove removes the wal instance for the channel.
func (m *managerImpl) Remove(ctx context.Context, channel string, term int64) (err error) {
	// reject operation if manager is closing.
	if m.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("wal manager is closed")
	}
	defer func() {
		m.lifetime.Done()
		if err != nil {
			log.Warn("remove wal failed", zap.Error(err), zap.String("channel", channel), zap.Int64("term", term))
		}
		log.Info("remove wal success", zap.String("channel", channel), zap.Int64("term", term))
	}()

	return m.getWALLifetime(channel).Remove(ctx, term)
}

// GetAvailableWAL returns a available wal instance for the channel.
// Return nil if the wal instance is not found.
func (m *managerImpl) GetAvailableWAL(channelName string, term int64) (wal.WAL, error) {
	// reject operation if manager is closing.
	if m.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal manager is closed")
	}
	defer m.lifetime.Done()

	l := m.getWALLifetime(channelName).GetWAL()
	if l == nil {
		return nil, status.NewChannelNotExist(channelName)
	}

	channelTerm := l.Channel().Term
	if channelTerm != term {
		return nil, status.NewUnmatchedChannelTerm(channelName, term, channelTerm)
	}
	return l, nil
}

// GetAllAvailableChannels returns all available channel info.
func (m *managerImpl) GetAllAvailableChannels() ([]*logpb.PChannelInfo, error) {
	// reject operation if manager is closing.
	if m.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal manager is closed")
	}
	defer m.lifetime.Done()

	// collect all available wal info.
	infos := make([]*logpb.PChannelInfo, 0)
	m.wltMap.Range(func(channel string, lt *walLifetime) bool {
		if l := lt.GetWAL(); l != nil {
			info := l.Channel()
			infos = append(infos, info)
		}
		return true
	})
	return infos, nil
}

// Close these manager and release all managed WAL.
func (m *managerImpl) Close() {
	m.lifetime.SetState(lifetime.Stopped)
	m.lifetime.Wait()
	m.lifetime.Close()

	// close all underlying walLifetime.
	m.wltMap.Range(func(channel string, wlt *walLifetime) bool {
		wlt.Close()
		return true
	})

	// close all underlying wal instance by allocator if there's resource leak.
	m.opener.Close()
}

// getWALLifetime returns the wal lifetime for the channel.
func (m *managerImpl) getWALLifetime(channel string) *walLifetime {
	if wlt, loaded := m.wltMap.Get(channel); loaded {
		return wlt
	}

	newWLT := newWALLifetime(m.opener, channel, m.openOpt)
	wlt, loaded := m.wltMap.GetOrInsert(channel, newWLT)
	// if loaded, lifetime is exist, close the redundant lifetime.
	if loaded {
		newWLT.Close()
	}
	return wlt
}
