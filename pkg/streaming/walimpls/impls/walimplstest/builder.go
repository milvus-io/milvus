//go:build test
// +build test

package walimplstest

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
)

const (
	WALName = "walimplstest"
)

func init() {
	// register the builder to the registry.
	registry.RegisterBuilder(&openerBuilder{})
	message.RegisterMessageIDUnmsarshaler(WALName, UnmarshalTestMessageID)
}

var _ walimpls.OpenerBuilderImpls = &openerBuilder{}

type openerBuilder struct{}

func (o *openerBuilder) Name() string {
	return WALName
}

func (o *openerBuilder) Build() (walimpls.OpenerImpls, error) {
	return &opener{}, nil
}
