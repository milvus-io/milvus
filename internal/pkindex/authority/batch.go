package authority

// Batch collects changes in memory. Nothing reaches the engine before Authority.Write.
// Changes are applied in the order of the Put and Delete calls that collected them.
type Batch struct {
	muts []Mutation
}

// Put maps pk to e.
func (b *Batch) Put(pk PK, e Entry) {
	b.muts = append(b.muts, Mutation{Key: pk.Encode(), Value: e.encode()})
}

// Delete makes pk absent.
func (b *Batch) Delete(pk PK) {
	b.muts = append(b.muts, Mutation{Key: pk.Encode(), Delete: true})
}

// Len returns the number of collected changes.
func (b *Batch) Len() int {
	return len(b.muts)
}
