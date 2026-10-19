package viewquery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/viewerror"
)

func toRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	var viewErr *viewerror.ViewError
	if errors.As(err, &viewErr) {
		return viewerror.NewGRPCStatusFromViewError(viewErr).Err()
	}
	return viewerror.NewGRPCStatusFromViewError(viewerror.AsViewError(err)).Err()
}
