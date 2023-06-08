package errorutil

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func UnhealthyStatus(code commonpb.StateCode) *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "proxy not healthy, StateCode=" + commonpb.StateCode_name[int32(code)],
	}
}

func UnhealthyError() error {
	return errors.New("unhealthy node")
}

func PermissionDenyError() error {
	return errors.New("permission deny")
}

func UnHealthReason(role string, nodeID typeutil.UniqueID, reason string) string {
	return fmt.Sprintf("role %s[nodeID: %d] is unhealthy, reason: %s", role, nodeID, reason)
}

func UnHealthReasonWithComponentStatesOrErr(role string, nodeID typeutil.UniqueID, cs *milvuspb.ComponentStates, err error) (bool, string) {
	if err != nil {
		return false, UnHealthReason(role, nodeID, fmt.Sprintf("inner error: %s", err.Error()))
	}

	if cs != nil && cs.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return false, UnHealthReason(role, nodeID, fmt.Sprintf("rpc status error: %d", cs.GetStatus().GetErrorCode()))
	}

	if cs != nil && cs.GetState().GetStateCode() != commonpb.StateCode_Healthy {
		return false, UnHealthReason(role, nodeID, fmt.Sprintf("node is unhealthy, state code: %d", cs.GetState().GetStateCode()))
	}

	return true, ""
}
