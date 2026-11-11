package proxy

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// prewarmCollection admits optional cache work after an ordinary load succeeds.
// Neither admission nor execution delays the load response or changes its result.
func (node *Proxy) prewarmCollection(ctx context.Context, collectionID int64) {
	lifetime, stop := contextutil.MergeContext(context.WithoutCancel(ctx), node.ctx)
	ctx, cancel := context.WithTimeout(lifetime, paramtable.Get().QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))
	go func() {
		defer stop()
		defer cancel()
		var err error
		if node.viewQueryClient == nil {
			err = merr.WrapErrServiceNotReadyMsg("query view client is unavailable")
		} else {
			err = queryclient.Prewarm(ctx, node.viewQueryClient, collectionID)
		}
		if err != nil {
			mlog.Info(ctx, "discard one-time prewarm request", mlog.FieldCollectionID(collectionID), mlog.Err(err))
		}
	}()
}
