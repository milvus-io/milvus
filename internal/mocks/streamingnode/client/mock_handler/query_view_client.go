//go:build test && dynamic

package mock_handler

import "github.com/milvus-io/milvus/internal/streamingnode/client/handler"

// QueryViewClient keeps the legacy test client compatible with HandlerClient.
// Tests using this domain patch this method with Mockey.
func (*MockHandlerClient) QueryViewClient() handler.QueryViewClient {
	panic("QueryViewClient must be patched with Mockey")
}
