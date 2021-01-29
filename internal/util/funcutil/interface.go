package funcutil

import "github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

type StateComponent interface {
	GetComponentStates() (*internalpb2.ComponentStates, error)
}
