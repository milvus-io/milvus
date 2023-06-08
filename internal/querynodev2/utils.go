package querynodev2

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func WrapRetrieveResult(code commonpb.ErrorCode, msg string, errs ...error) *internalpb.RetrieveResults {
	return &internalpb.RetrieveResults{
		Status: util.WrapStatus(code, msg, errs...),
	}
}

func WrapSearchResult(code commonpb.ErrorCode, msg string, errs ...error) *internalpb.SearchResults {
	return &internalpb.SearchResults{
		Status: util.WrapStatus(code, msg, errs...),
	}
}

// CheckTargetID checks whether the target ID of request is the server itself,
// returns true if matched,
// returns false otherwise
func CheckTargetID[R interface{ GetBase() *commonpb.MsgBase }](req R) bool {
	return req.GetBase().GetTargetID() == paramtable.GetNodeID()
}
