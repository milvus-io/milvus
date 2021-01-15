package timesync

import (
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type TimeTickWatcher interface {
	Watch(msg *ms.TimeTickMsg)
	Start()
	Close()
}
