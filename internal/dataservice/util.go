package dataservice

import (
	"errors"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type Response interface {
	GetStatus() *commonpb.Status
}

func VerifyResponse(response interface{}, err error) error {
	if err != nil {
		return err
	}
	if response == nil {
		return errors.New("response is nil")
	}
	switch resp := response.(type) {
	case Response:
		if resp.GetStatus().ErrorCode != commonpb.ErrorCode_ERROR_CODE_SUCCESS {
			return errors.New(resp.GetStatus().Reason)
		}
	case *commonpb.Status:
		if resp.ErrorCode != commonpb.ErrorCode_ERROR_CODE_SUCCESS {
			return errors.New(resp.Reason)
		}
	default:
		return errors.New("unknown response type")
	}
	return nil
}
