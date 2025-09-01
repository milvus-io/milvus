//go:build test
// +build test

package walimplstest

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
)

func init() {
	// register the builder to the registry.
	registry.RegisterBuilder(&openerBuilder{})
	message.RegisterMessageIDUnmsarshaler(message.WALNameTest, UnmarshalTestMessageID)
}

var _ walimpls.OpenerBuilderImpls = &openerBuilder{}

type openerBuilder struct{}

func (o *openerBuilder) Name() message.WALName {
	return message.WALNameTest
}

func (o *openerBuilder) Build() (walimpls.OpenerImpls, error) {
	return &opener{}, nil
}
