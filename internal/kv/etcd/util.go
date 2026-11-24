package etcdkv

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func parsePredicates(rootPath string, preds ...predicates.Predicate) ([]clientv3.Cmp, error) {
	if len(preds) == 0 {
		return []clientv3.Cmp{}, nil
	}
	result := make([]clientv3.Cmp, 0, len(preds))
	for _, pred := range preds {
		key := util.GetPath(rootPath, pred.Key())
		var cmp clientv3.Cmp
		switch pred.Target() {
		case predicates.PredTargetValueEqual:
			cmp = clientv3.Compare(clientv3.Value(key), "=", pred.TargetValue())
		case predicates.PredTargetKeyNotExist:
			cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		case predicates.PredTargetKeyExist:
			cmp = clientv3.Compare(clientv3.CreateRevision(key), ">", 0)
		case predicates.PredTargetUnchangedSince:
			rev, ok := pred.TargetValue().(int64)
			if !ok {
				return nil, merr.WrapErrParameterInvalid("an int64 revision", fmt.Sprintf("%v", pred.TargetValue()))
			}
			// a missing key has mod revision 0, so it passes
			cmp = clientv3.Compare(clientv3.ModRevision(key), "<", rev+1)
		default:
			return nil, merr.WrapErrParameterInvalid("valid predicate target", fmt.Sprintf("%d", pred.Target()))
		}
		result = append(result, cmp)
	}
	return result, nil
}

// getContextWithTimeout returns a context with timeout
func getContextWithTimeout(ctx context.Context, timeoutDuration time.Duration) (context.Context, context.CancelFunc) {
	// Use original ctx as base to preserve context values,
	// but remove RBAC auth info to avoid auth contamination in etcd requests
	newCtx := ctx

	// Extract a metadata copy from incoming context and remove auth-related keys
	if mdCopy, ok := metadata.FromIncomingContext(ctx); ok {
		mdCopy.Delete(util.HeaderAuthorize)
		mdCopy.Delete(util.HeaderToken)
		newCtx = metadata.NewIncomingContext(ctx, mdCopy)
	}
	return context.WithTimeout(newCtx, timeoutDuration)
}

// markCompacted marks wrapped, the merr form of err, with kv.ErrCompacted
// when err is an etcd compaction: WrapErrIoFailed keeps only the cause's
// text, and a caller reading at a pinned revision (WalkPrefixPage) must
// tell it apart to restart on a newer snapshot.
func markCompacted(err, wrapped error) error {
	if errors.Is(err, rpctypes.ErrCompacted) {
		return errors.Mark(wrapped, kv.ErrCompacted)
	}
	return wrapped
}
