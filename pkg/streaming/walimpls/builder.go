package walimpls

import "github.com/milvus-io/milvus/pkg/v2/streaming/util/message"

// OpenerBuilderImpls is the interface for building wal opener impls.
type OpenerBuilderImpls interface {
	// Name of the wal builder, should be a lowercase string.
	Name() message.WALName

	// Build build a opener impls instance.
	Build() (OpenerImpls, error)
}
