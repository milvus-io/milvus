package snview

import "github.com/milvus-io/milvus/internal/views/worknode/handler"

type QueryViewHandlerProvider interface {
	QueryViewHandler() handler.QueryViewHandler
}
