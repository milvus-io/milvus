package extension

import "context"

// internalDomainKey marks a request that arrived on an internal-domain
// listener. An unexported struct key cannot be forged from outside this
// package by accident, and never collides with a string key some middleware
// stuffed into the same context.
type internalDomainKey struct{}

// WithInternalDomain stamps ctx as originating on an internal-domain
// listener. Only the listeners a form's InternalSurfaces capability opened
// should stamp it: the mark is how a handler-level seam tells the control
// plane's call, arriving on the trusted internal port, from a tenant's call
// arriving on the external one - the handler itself is shared and cannot see
// which listener accepted the connection.
func WithInternalDomain(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalDomainKey{}, true)
}

// FromInternalDomain reports whether ctx carries the internal-domain mark.
func FromInternalDomain(ctx context.Context) bool {
	v, _ := ctx.Value(internalDomainKey{}).(bool)
	return v
}
