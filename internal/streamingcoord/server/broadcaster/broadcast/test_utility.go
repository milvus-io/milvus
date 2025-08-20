//go:build test
// +build test

package broadcast

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func ResetBroadcaster() {
	singleton = syncutil.NewFuture[broadcaster.Broadcaster]()
}
