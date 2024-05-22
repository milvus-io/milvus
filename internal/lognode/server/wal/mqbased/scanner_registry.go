package mqbased

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// newScannerRegistry creates a new scanner registry.
func newScannerRegistry(channel *logpb.PChannelInfo) *scannerRegistry {
	return &scannerRegistry{
		logger:      log.With(zap.Any("channel", channel)),
		channel:     *channel,
		idAllocator: util.NewIDAllocator(),
	}
}

// scannerRegistry is the a registry for manage name and gc of existed scanner.
type scannerRegistry struct {
	logger *log.MLogger

	channel     logpb.PChannelInfo
	idAllocator *util.IDAllocator
}

// AllocateScannerName a scanner name for a scanner.
// The scanner name should be persistent on meta for garbage clean up.
func (m *scannerRegistry) AllocateScannerName() (string, error) {
	name := m.newSubscriptionName()
	// TODO: persistent the subscription name on meta.
	return name, nil
}

// GetAllExpiredScannerNames get all expired scanner.
// (term < current term scanner)
func (m *scannerRegistry) GetAllExpiredScannerNames() ([]string, error) {
	// TODO: scan all scanner of these channel, drop all expired scanner.
	return nil, nil
}

// newSubscriptionName generates a new subscription name.
func (m *scannerRegistry) newSubscriptionName() string {
	id := m.idAllocator.Allocate()
	return fmt.Sprintf("%s/%d/%d", m.channel.Name, m.channel.Term, id)
}
