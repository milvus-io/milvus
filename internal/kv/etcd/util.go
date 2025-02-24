package etcdkv

import (
	"fmt"
	"path"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v2/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func parsePredicates(rootPath string, preds ...predicates.Predicate) ([]clientv3.Cmp, error) {
	if len(preds) == 0 {
		return []clientv3.Cmp{}, nil
	}
	result := make([]clientv3.Cmp, 0, len(preds))
	for _, pred := range preds {
		switch pred.Target() {
		case predicates.PredTargetValue:
			pt, err := parsePredicateType(pred.Type())
			if err != nil {
				return nil, err
			}
			cmp := clientv3.Compare(clientv3.Value(path.Join(rootPath, pred.Key())), pt, pred.TargetValue())
			result = append(result, cmp)
		default:
			return nil, merr.WrapErrParameterInvalid("valid predicate target", fmt.Sprintf("%d", pred.Target()))
		}
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
