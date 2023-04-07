package inproc

//go:generate go run gen/gen.go

// Designed implementation shall be following one, but golang cannot embed
// generic type T it self as anonymous field.
/*
type wrapper[T types.Component] struct {
	T
}

func (w *wrapper[T]) Set(v T) {
	w.T = v
}*/
