package registry

var (
	_ isLocal = localWAL{}
	_ isLocal = localScanner{}
)

// localTrait is used to make isLocal can only be implemented by current package.
type localTrait struct{}

// isLocal is a hint interface for local wal.
type isLocal interface {
	isLocal() localTrait
}

// IsLocal checks if the component is local.
func IsLocal(component any) bool {
	_, ok := component.(isLocal)
	return ok
}
