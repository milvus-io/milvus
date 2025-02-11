package helper

import (
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

type GenSchemaOption struct {
	CollectionName     string
	Description        string
	AutoID             bool
	Fields             []*entity.Field
	EnableDynamicField bool
	Function           *entity.Function
}

func TNewSchemaOption() *GenSchemaOption {
	return &GenSchemaOption{}
}

func (opt *GenSchemaOption) TWithName(collectionName string) *GenSchemaOption {
	opt.CollectionName = collectionName
	return opt
}

func (opt *GenSchemaOption) TWithDescription(description string) *GenSchemaOption {
	opt.Description = description
	return opt
}

func (opt *GenSchemaOption) TWithAutoID(autoID bool) *GenSchemaOption {
	opt.AutoID = autoID
	return opt
}

func (opt *GenSchemaOption) TWithEnableDynamicField(enableDynamicField bool) *GenSchemaOption {
	opt.EnableDynamicField = enableDynamicField
	return opt
}

func (opt *GenSchemaOption) TWithFields(fields []*entity.Field) *GenSchemaOption {
	opt.Fields = fields
	return opt
}

func (opt *GenSchemaOption) TWithFunction(function *entity.Function) *GenSchemaOption {
	opt.Function = function
	return opt
}

func GenSchema(option *GenSchemaOption) *entity.Schema {
	if len(option.Fields) == 0 {
		log.Fatal("Require at least a primary field and a vector field")
	}
	if option.CollectionName == "" {
		option.CollectionName = common.GenRandomString("pre", 6)
	}
	schema := entity.NewSchema().WithName(option.CollectionName)
	for _, field := range option.Fields {
		schema.WithField(field)
	}

	if option.Description != "" {
		schema.WithDescription(option.Description)
	}
	if option.AutoID {
		schema.WithAutoID(option.AutoID)
	}
	if option.EnableDynamicField {
		schema.WithDynamicFieldEnabled(option.EnableDynamicField)
	}
	if option.Function != nil {
		schema.WithFunction(option.Function)
	}
	return schema
}
