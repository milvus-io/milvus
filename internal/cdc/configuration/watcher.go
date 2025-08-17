package configuration

// Watcher watches the configuration changes and notifies the changes to the manager.
type Watcher interface {
	Start() error
	Stop() error
}

func NewWatcher() Watcher {
	return nil
}
