package rootcoord

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/util/errorutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func setNotServingStatus(status *commonpb.Status, stateCode commonpb.StateCode) {
	reason := fmt.Sprintf("sate code: %s", stateCode.String())
	status.Reason = errorutil.NotServingReason(typeutil.RootCoordRole, Params.DataCoordCfg.GetNodeID(), reason)
	status.ErrorCode = commonpb.ErrorCode_NotReadyServe
}
