package common

import "time"

const (
	PULSAR_URL              = "pulsar://localhost:16650"
	PULSAR_MONITER_INTERVAL = 1 * time.Second
	PULSAR_TOPIC            = "monitor-topic"
	ETCD_ROOT_PATH          = "by-dev"
	SEGMENT_THRESHOLE       = 10000
	DEFAULT_GRPC_PORT       = ":53100"
)
