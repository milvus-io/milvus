//go:build test
// +build test

package walimplstest

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
)

const (
	walName = "test"
)

func init() {
	// register the builder to the registry.
	registry.RegisterBuilder(&openerBuilder{})
	message.RegisterMessageIDUnmsarshaler(walName, UnmarshalTestMessageID)
}

var _ walimpls.OpenerBuilderImpls = &openerBuilder{}

type openerBuilder struct{}

func (o *openerBuilder) Name() string {
	return walName
}

func (o *openerBuilder) Build() (walimpls.OpenerImpls, error) {
	return &opener{}, nil
}
