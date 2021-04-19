package typeutil

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type Service interface {
	Init()
	Start()
	Stop()
}

type Component interface {
	GetComponentStates() (*internalpb2.ComponentStates, error)
	GetTimeTickChannel() (string, error)
	GetStatisticsChannel() (string, error)
}
