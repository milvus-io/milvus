//go:build test
// +build test

package channel

import "github.com/milvus-io/milvus/pkg/v2/util/syncutil"

func ResetStaticPChannelStatsManager() {
	StaticPChannelStatsManager = syncutil.NewFuture[*PchannelStatsManager]()
}
