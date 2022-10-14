package errorutil

import (
	"errors"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// ErrorList for print error log
type ErrorList []error

// Error method return an string representation of retry error list.
func (el ErrorList) Error() string {
	limit := 10
	var builder strings.Builder
	builder.WriteString("All attempts results:\n")
	for index, err := range el {
		// if early termination happens
		if err == nil {
			break
		}
		if index > limit {
			break
		}
		builder.WriteString(fmt.Sprintf("attempt #%d:%s\n", index+1, err.Error()))
	}
	return builder.String()
}

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
