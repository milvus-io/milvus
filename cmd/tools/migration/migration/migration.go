package migration

type Migration interface {
	// Validate if migration can be executed. For example, higher version to lower version is not allowed.
	Validate() error
	// CheckCompatible check if target is compatible with source. If compatible, no migration should be executed.
	CheckCompatible() bool
	// CheckSessions check if any sessions are alive. Abort migration if any.
	CheckSessions() error
	// RegisterSession register session to avoid any other migration is also running, registered session will be deleted
	// as soon as possible after migration is done.
	RegisterSession() error
	// Backup source meta information.
	Backup() error
	// Migrate to target backend.
	Migrate() error
	// Rollback migration.
	Rollback() error
	// Stop complete the migration overflow.
	Stop()
}
