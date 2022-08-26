package meta

type Meta struct {
	*CollectionManager
	*ReplicaManager
}

func NewMeta(
	idAllocator func() (int64, error),
	store Store,
) *Meta {
	return &Meta{
		NewCollectionManager(store),
		NewReplicaManager(idAllocator, store),
	}
}
