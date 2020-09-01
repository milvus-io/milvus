package reader

import "github.com/czs007/suvlim/pulsar/schema"

type IndexConfig struct {}

func buildIndex(config IndexConfig) schema.Status {
	return schema.Status{Error_code: schema.ErrorCode_SUCCESS}
}

func dropIndex(fieldName string) schema.Status {
	return schema.Status{Error_code: schema.ErrorCode_SUCCESS}
}
