package segcore

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// Native cipher Update(empty) removes the entire context. Collection shells
// from different schemas, load configurations and SN runtimes must therefore
// share ownership of that context, independently of their View reference counts.
var collectionCipherContexts = struct {
	sync.Mutex
	refs map[hookutil.EZ]int
}{refs: make(map[hookutil.EZ]int)}

func acquireCollectionCipherContext(properties []*commonpb.KeyValuePair, collectionID int64) (func(), error) {
	ez := hookutil.GetEzByCollProperties(properties, collectionID)
	if ez == nil {
		return nil, nil
	}
	collectionCipherContexts.Lock()
	defer collectionCipherContexts.Unlock()
	if collectionCipherContexts.refs[*ez] == 0 {
		pluginContext, err := hookutil.GetCollectionPluginContext(properties, collectionID)
		if err != nil {
			return nil, err
		}
		if err := PutOrRefPluginContext(ez, pluginContext.GetEncryptionKey()); err != nil {
			return nil, err
		}
	}
	collectionCipherContexts.refs[*ez]++
	return func() {
		collectionCipherContexts.Lock()
		defer collectionCipherContexts.Unlock()
		collectionCipherContexts.refs[*ez]--
		if collectionCipherContexts.refs[*ez] == 0 {
			delete(collectionCipherContexts.refs, *ez)
			if err := UnRefPluginContext(ez); err != nil {
				mlog.Error(context.TODO(), "failed to release collection cipher context",
					mlog.Int64("collectionID", collectionID), mlog.Err(err))
			}
		}
	}, nil
}
