package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	CPUUsage        = newHardwareGauge("cpu_usage", "the cpu usage of milvus process")
	MemoryUsage     = newHardwareGauge("memory_usage", "the memory usage of milvus process")
	AverageCPUUsage = newHardwareGauge("avg_cpu_usage", "the average cpu usage of milvus process")
)

// newHardwareGauge creates a new hardware gauge.
func newHardwareGauge(name string, help string) prometheus.Gauge {
	return prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: "hardware",
			Name:      name,
			Help:      help,
		},
	)
}

func registerHardwareMetrics(registry prometheus.Registerer) {
	registry.MustRegister(CPUUsage)
	registry.MustRegister(MemoryUsage)
	registry.MustRegister(AverageCPUUsage)
}
