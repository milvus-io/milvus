package proxy

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func (node *Proxy) SendSearchResult(ctx context.Context, req *internalpb.SearchResults) (*commonpb.Status, error) {
	node.searchResultCh <- req
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (node *Proxy) SendRetrieveResult(ctx context.Context, req *internalpb.RetrieveResults) (*commonpb.Status, error) {
	node.retrieveResultCh <- req
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}
