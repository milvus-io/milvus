package reader

import (
	schema2 "github.com/czs007/suvlim/pulsar/client-go/schema"
)

type IndexConfig struct {}

func buildIndex(config IndexConfig) schema2.Status {
	return schema2.Status{Error_code: schema2.ErrorCode_SUCCESS}
}

func dropIndex(fieldName string) schema2.Status {
	return schema2.Status{Error_code: schema2.ErrorCode_SUCCESS}
}
