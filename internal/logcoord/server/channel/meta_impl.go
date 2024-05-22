package channel

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// NewMeta creates a new meta instance.
func NewMeta(catalog metastore.LogCoordCataLog, channels map[string]PhysicalChannel) Meta {
	return &metaImpl{
		catalog:  catalog,
		mu:       sync.RWMutex{},
		channels: channels,
	}
}

// TODO: Should we do not use a local cache here, the transaction should be done by metaKV but not mutex.
// If we can promise the logcoord is globally unique, it is safe to use a local cache.
type metaImpl struct {
	catalog metastore.LogCoordCataLog

	mu       sync.RWMutex
	channels map[string]PhysicalChannel
}

// GetPChannel get a pchannel.
func (m *metaImpl) GetPChannel(ctx context.Context, pChannelName string) PhysicalChannel {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.channels[pChannelName]
}

// GetPChannels get all pchannels.
func (m *metaImpl) GetPChannels(ctx context.Context) map[string]PhysicalChannel {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]PhysicalChannel, len(m.channels))
	for name, channel := range m.channels {
		result[name] = channel
	}
	return result
}

// Add add a pchannel.
func (m *metaImpl) CreatePChannel(ctx context.Context, pChannel string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check cache.
	if _, ok := m.channels[pChannel]; ok {
		return ErrExists
	}

	// Save channel if not exists.
	pChannelInfo := &logpb.PChannelInfo{
		Name:          pChannel,
		Term:          0,
		ServerID:      -1,
		VChannelInfos: make([]*logpb.VChannelInfo, 0),
	}
	if err := m.catalog.SavePChannel(ctx, pChannelInfo); err != nil {
		return err
	}
	m.channels[pChannel] = NewPhysicalChannel(m.catalog, pChannelInfo)

	metrics.LogCoordPChannelTotal.WithLabelValues(paramtable.GetStringNodeID()).Set(float64(len(m.channels)))
	return nil
}

// Remove remove a pchannel.
func (m *metaImpl) RemovePChannel(ctx context.Context, pChannel string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check cache.
	if _, ok := m.channels[pChannel]; !ok {
		return ErrNotExists
	}

	// Remove channel if exists.
	err := m.catalog.DropPChannel(ctx, pChannel)
	if err != nil {
		return err
	}
	delete(m.channels, pChannel)

	metrics.LogCoordPChannelTotal.WithLabelValues(paramtable.GetStringNodeID()).Set(float64(len(m.channels)))
	return nil
}
