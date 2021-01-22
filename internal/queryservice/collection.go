package queryservice

type collection struct {
	id         UniqueID
	partitions []*partition
}
