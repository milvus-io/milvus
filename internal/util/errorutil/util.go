package errorutil

import (
	"errors"
	"strings"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"fmt"
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

func UnhealthyStatus(code internalpb.StateCode) *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "proxy not healthy, StateCode=" + internalpb.StateCode_name[int32(code)],
	}
}

func UnhealthyError() error {
	return errors.New("unhealthy node")
}

func PermissionDenyError() error {
	return errors.New("permission deny")
}

func ReadableError(errMsg string, err error, fields ...zap.Field) error {
	log.Error(errMsg, append(fields, zap.Error(err))...)
	return errors.New(errMsg)
}
