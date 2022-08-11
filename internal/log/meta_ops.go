package log

type MetaOperation int

const (
	InvalidMetaOperation MetaOperation = iota - 1
	CreateCollection
	DropCollection
	CreateCollectionAlias
	AlterCollectionAlias
	DropCollectionAlias
	CreatePartition
	DropPartition
	CreateIndex
	DropIndex
	BuildSegmentIndex
)
