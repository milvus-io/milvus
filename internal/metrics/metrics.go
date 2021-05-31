package metrics

import (
	"net/http"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.uber.org/zap"
)

/*
var (
	PanicCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "server",
			Name:      "panic_total",
			Help:      "Counter of panic.",
		}, []string{"type"})
)
*/

var (
	MasterProxyNodeLister = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "list_of_proxy_node",
			Help:      "List of proxy nodes which has register with etcd",
		}, []string{"client_id"})
	MasterCreateCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "create_collection_total",
			Help:      "Counter of create collection",
		}, []string{"client_id", "type"})
	MasterDropCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milvus",
			Subsystem: "master",
			Name:      "drop_collection_total",
			Help:      "Counter of drop collection",
		}, []string{"client_id", "type"})
)

//RegisterMaster register Master metrics
func RegisterMaster() {
	prometheus.MustRegister(MasterCreateCollectionCounter)
	prometheus.MustRegister(MasterDropCollectionCounter)
	//prometheus.MustRegister(PanicCounter)
}

//RegisterProxyService register ProxyService metrics
func RegisterProxyService() {

}

//RegisterProxyNode register ProxyNode metrics
func RegisterProxyNode() {

}

//RegisterQueryService register QueryService metrics
func RegisterQueryService() {

}

//RegisterQueryNode register QueryNode metrics
func RegisterQueryNode() {

}

//RegisterDataService register DataService metrics
func RegisterDataService() {

}

//RegisterDataNode register DataNode metrics
func RegisterDataNode() {

}

//RegisterIndexService register IndexService metrics
func RegisterIndexService() {

}

//RegisterIndexNode register IndexNode metrics
func RegisterIndexNode() {

}

//RegisterMsgStreamService register MsgStreamService metrics
func RegisterMsgStreamService() {

}

//ServeHTTP serve prometheus http service
func ServeHTTP() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":9091", nil); err != nil {
			log.Error("handle metrics failed", zap.Error(err))
		}
	}()
}
