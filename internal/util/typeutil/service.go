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
	GetServiceStates() (internalpb2.ServiceStates, error)
	GetTimeTickChannel() (string, error)
	GetStatisticsChannel() (string, error)
}
