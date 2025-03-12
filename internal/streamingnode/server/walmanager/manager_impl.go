package walmanager

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/flusher"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var errWALManagerClosed = status.NewOnShutdownError("wal manager is closed")

// OpenManager create a wal manager.
func OpenManager() (Manager, error) {
	walName := util.MustSelectWALName()
	resource.Resource().Logger().Info("open wal manager", zap.String("walName", walName))
	opener, err := registry.MustGetBuilder(walName,
		redo.NewInterceptorBuilder(),
		flusher.NewInterceptorBuilder(),
		timetick.NewInterceptorBuilder(),
		segment.NewInterceptorBuilder(),
	).Build()
	if err != nil {
		return nil, err
	}
	return newManager(opener), nil
}

// newManager create a wal manager.
func newManager(opener wal.Opener) Manager {
	return &managerImpl{
		lifetime: typeutil.NewGenericLifetime[managerState](managerOpenable | managerRemoveable | managerGetable),
		wltMap:   typeutil.NewConcurrentMap[string, *walLifetime](),
		opener:   opener,
		logger:   resource.Resource().Logger().With(log.FieldComponent("wal-manager")),
	}
}

// All management operation for a wal will be serialized with order of term.
type managerImpl struct {
	lifetime *typeutil.GenericLifetime[managerState]

	wltMap *typeutil.ConcurrentMap[string, *walLifetime]
	opener wal.Opener // wal allocator
	logger *log.MLogger
}

// Open opens a wal instance for the channel on this Manager.
func (m *managerImpl) Open(ctx context.Context, channel types.PChannelInfo) (err error) {
	// reject operation if manager is closing.
	if !m.lifetime.AddIf(isOpenable) {
		return errWALManagerClosed
	}
	defer func() {
		m.lifetime.Done()
		if err != nil {
			m.logger.Warn("open wal failed", zap.Error(err), zap.String("channel", channel.String()))
			return
		}
		m.logger.Info("open wal success", zap.String("channel", channel.String()))
	}()

	return m.getWALLifetime(channel.Name).Open(ctx, channel)
}

// Remove removes the wal instance for the channel.
func (m *managerImpl) Remove(ctx context.Context, channel types.PChannelInfo) (err error) {
	// reject operation if manager is closing.
	if !m.lifetime.AddIf(isRemoveable) {
		return errWALManagerClosed
	}
	defer func() {
		m.lifetime.Done()
		if err != nil {
			m.logger.Warn("remove wal failed", zap.Error(err), zap.String("channel", channel.Name), zap.Int64("term", channel.Term))
			return
		}
		m.logger.Info("remove wal success", zap.String("channel", channel.Name), zap.Int64("term", channel.Term))
	}()

	return m.getWALLifetime(channel.Name).Remove(ctx, channel.Term)
}

// GetAvailableWAL returns a available wal instance for the channel.
// Return nil if the wal instance is not found.
func (m *managerImpl) GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	// reject operation if manager is closing.
	if !m.lifetime.AddIf(isGetable) {
		return nil, errWALManagerClosed
	}
	defer m.lifetime.Done()

	l := m.getWALLifetime(channel.Name).GetWAL()
	if l == nil {
		return nil, status.NewChannelNotExist(channel.Name)
	}

	currentTerm := l.Channel().Term
	if currentTerm != channel.Term {
		return nil, status.NewUnmatchedChannelTerm(channel.Name, channel.Term, currentTerm)
	}
	// wal's lifetime is fully managed by wal manager,
	// so wrap the wal instance to prevent it from being closed by other components.
	return nopCloseWAL{l}, nil
}

// GetAllAvailableChannels returns all available channel info.
func (m *managerImpl) GetAllAvailableChannels() ([]types.PChannelInfo, error) {
	// reject operation if manager is closing.
	if !m.lifetime.AddIf(isGetable) {
		return nil, errWALManagerClosed
	}
	defer m.lifetime.Done()

	// collect all available wal info.
	infos := make([]types.PChannelInfo, 0)
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
	m.lifetime.SetState(managerRemoveable)
	m.lifetime.Wait()
	// close all underlying walLifetime.
	m.wltMap.Range(func(channel string, wlt *walLifetime) bool {
		wlt.Close()
		return true
	})
	m.lifetime.SetState(managerStopped)
	m.lifetime.Wait()

	// close all underlying wal instance by allocator if there's resource leak.
	m.opener.Close()
}

// getWALLifetime returns the wal lifetime for the channel.
func (m *managerImpl) getWALLifetime(channel string) *walLifetime {
	if wlt, loaded := m.wltMap.Get(channel); loaded {
		return wlt
	}

	// Perform a cas here.
	newWLT := newWALLifetime(m.opener, channel, m.logger)
	wlt, loaded := m.wltMap.GetOrInsert(channel, newWLT)
	// if loaded, lifetime is exist, close the redundant lifetime.
	if loaded {
		newWLT.Close()
	}
	return wlt
}

type managerState int32

const (
	managerStopped    managerState = 0
	managerOpenable   managerState = 0x1
	managerRemoveable managerState = 0x1 << 1
	managerGetable    managerState = 0x1 << 2
)

func isGetable(state managerState) bool {
	return state&managerGetable != 0
}

func isRemoveable(state managerState) bool {
	return state&managerRemoveable != 0
}

func isOpenable(state managerState) bool {
	return state&managerOpenable != 0
}

// wal can be only closed by the wal manager.
// So wrap the wal instance to prevent it from being closed by other components.
type nopCloseWAL struct {
	wal.WAL
}

func (w nopCloseWAL) Close() {
	// do nothing
}
