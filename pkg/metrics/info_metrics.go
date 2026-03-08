package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	infoMutex sync.Mutex
	mqType    string
	metaType  string

	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Name:      "build_info",
			Help:      "Build information of milvus",
		},
		[]string{
			"version",
			"built",
			"git_commit",
		},
	)

	RuntimeInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Name:      "runtime_info",
			Help:      "Runtime information of milvus",
		},
		[]string{
			"mq",
			"meta",
		},
	)

	ThreadNum = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Name:      "thread_num",
			Help:      "the actual thread number of milvus process",
		},
	)
)

// RegisterMQType registers the type of mq
func RegisterMQType(mq string) {
	infoMutex.Lock()
	defer infoMutex.Unlock()
	mqType = mq
	updateRuntimeInfo()
}

// RegisterMetaType registers the type of meta
func RegisterMetaType(meta string) {
	infoMutex.Lock()
	defer infoMutex.Unlock()
	metaType = meta
	updateRuntimeInfo()
}

// updateRuntimeInfo update the runtime info of milvus if every label is ready.
func updateRuntimeInfo() {
	if mqType == "" || metaType == "" {
		return
	}
	RuntimeInfo.WithLabelValues(mqType, metaType).Set(1)
}
