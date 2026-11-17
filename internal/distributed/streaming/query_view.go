package streaming

import "github.com/milvus-io/milvus/internal/streamingnode/client/handler"

// QueryViewClient provides owner-routed planning and execution RPCs.
type QueryViewClient = handler.QueryViewClient

func (w *walAccesserImpl) QueryViewClient() QueryViewClient {
	return w.handlerClient.QueryViewClient()
}
