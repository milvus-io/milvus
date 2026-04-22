package etcdkv

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/metadata"

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
		pt, err := parsePredicateType(pred.Type())
		if err != nil {
			return nil, err
		}
		fullKey := util.GetPath(rootPath, pred.Key())
		var cmp clientv3.Cmp
		switch pred.Target() {
		case predicates.PredTargetValue:
			cmp = clientv3.Compare(clientv3.Value(fullKey), pt, pred.TargetValue())
		case predicates.PredTargetCreateRevision:
			cmp = clientv3.Compare(clientv3.CreateRevision(fullKey), pt, pred.TargetValue())
		case predicates.PredTargetModRevision:
			cmp = clientv3.Compare(clientv3.ModRevision(fullKey), pt, pred.TargetValue())
		default:
			return nil, merr.WrapErrParameterInvalid("valid predicate target", fmt.Sprintf("%d", pred.Target()))
		}
		result = append(result, cmp)
	}
	return result, nil
}

// parsePredicateType parse predicates.PredicateType to clientv3.Result
func parsePredicateType(pt predicates.PredicateType) (string, error) {
	switch pt {
	case predicates.PredTypeEqual:
		return "=", nil
	default:
		return "", merr.WrapErrParameterInvalid("valid predicate type", fmt.Sprintf("%d", pt))
	}
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
