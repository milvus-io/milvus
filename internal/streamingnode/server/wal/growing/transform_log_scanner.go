package growing

import (
	"context"

	"github.com/cockroachdb/errors"

	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func (m *Manager) Read(ctx context.Context, opt transformlogapi.ReadOption) transformlogapi.Scanner {
	if opt.VChannel == "" {
		return transformlogapi.NewErrorScanner(opt.Name, errors.Wrap(transformlogapi.ErrInvalidReadOption, "vchannel is required"))
	}
	if funcutil.ToPhysicalChannel(opt.VChannel) != m.channelName && m.channelName != "" {
		return transformlogapi.NewErrorScanner(opt.Name, errors.Wrap(transformlogapi.ErrInvalidReadOption, "vchannel does not belong to manager pchannel"))
	}
	vchannel := m.vChannel(opt.VChannel)
	if vchannel == nil {
		return transformlogapi.NewErrorScanner(opt.Name, errors.Wrap(transformlogapi.ErrVChannelUnavailable, "vchannel is not available"))
	}
	transformLog := m.transformLog(opt.VChannel)
	if transformLog == nil {
		return transformlogapi.NewErrorScanner(opt.Name, errors.Wrap(transformlogapi.ErrVChannelUnavailable, "transform log is not available"))
	}
	return transformLog.log.Read(ctx, opt)
}
