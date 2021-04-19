package queryservice

type partition struct {
	id       UniqueID
	segments []*segment
}
