package queryresource

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/growingruntime"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func defaultQueryRuntimeModuleBuilders(builders []QueryRuntimeModuleBuilder) []QueryRuntimeModuleBuilder {
	if len(builders) == 0 {
		return []QueryRuntimeModuleBuilder{NewGrowingRuntimeModuleBuilder(nil)}
	}
	return append([]QueryRuntimeModuleBuilder(nil), builders...)
}

func minQueryViewDataVersion(refs map[qviews.QueryViewKey]struct{}) (qviews.DataVersion, bool) {
	var min qviews.DataVersion
	ok := false
	for key := range refs {
		version := key.QueryViewVersion.DataVersion
		if !ok || min.GT(version) {
			min = version
			ok = true
		}
	}
	return min, ok
}

func cancelTask(task BuildTask) {
	if task != nil {
		task.Cancel()
	}
}

func closeRuntime(runtime *QueryRuntime) {
	if runtime != nil {
		runtime.Close()
	}
}

type growingRuntimeModuleBuilder struct {
	builder growingruntime.Builder
}

func NewGrowingRuntimeModuleBuilder(builder growingruntime.Builder) QueryRuntimeModuleBuilder {
	if builder == nil {
		builder = growingruntime.SnapshotBuilder{}
	}
	return growingRuntimeModuleBuilder{builder: builder}
}

func (b growingRuntimeModuleBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return b.builder.NewRuntime()
}
