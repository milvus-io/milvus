package walimpls

// OpenerBuilderImpls is the interface for building wal opener impls.
type OpenerBuilderImpls interface {
	// Name of the wal builder, should be a lowercase string.
	Name() string

	// Build build a opener impls instance.
	Build() (OpenerImpls, error)
}
