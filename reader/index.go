package reader

import (
	msgPb "github.com/czs007/suvlim/pkg/message"
)

type IndexConfig struct {}

func buildIndex(config IndexConfig) msgPb.Status {
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func dropIndex(fieldName string) msgPb.Status {
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}
