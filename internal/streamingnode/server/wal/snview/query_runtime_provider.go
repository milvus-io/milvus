package snview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (h *SNQueryViewHandler) queryRuntime(key qviews.QueryViewKey) (QueryRuntime, error) {
	provider, ok := h.resMgr.(QueryRuntimeProvider)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("streamingnode query runtime provider is not available")
	}
	runtime, ok := provider.QueryRuntime(key)
	if !ok || runtime == nil {
		return nil, merr.WrapErrServiceUnavailable("query runtime %s is not available", key.String())
	}
	return runtime, nil
}
