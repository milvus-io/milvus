//go:build test && dynamic

package mock_handler

import "github.com/milvus-io/milvus/internal/streamingnode/client/handler"

// QueryViewSyncClient keeps the existing test client compatible with the added
// domain accessor. Tests using it patch this method with mockey.
func (*MockHandlerClient) QueryViewSyncClient() handler.QueryViewSyncClient {
	panic("mock with mockey")
}
