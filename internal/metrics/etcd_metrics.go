package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	EtcdPutKvSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "etcd",
			Name:      "etcd_kv_size",
			Help:      "kv size stats",
			Buckets:   buckets,
		})

	EtcdRequestLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "etcd",
			Name:      "client_request_latency",
			Help:      "request latency on the client side ",
			Buckets:   buckets,
		})
)

//RegisterEtcdMetrics registers etcd metrics
func RegisterEtcdMetrics(registry *prometheus.Registry) {
	registry.MustRegister(EtcdPutKvSize)
	registry.MustRegister(EtcdRequestLatency)
}
