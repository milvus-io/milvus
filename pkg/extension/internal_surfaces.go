package extension

// InternalSurfaces declares the unauthenticated internal-domain listeners a
// deployment form serves its own control plane on.
//
// The shape comes from how a managed cloud reaches an instance it operates.
// The instance's EXTERNAL listeners authenticate end users - and a form may
// close them entirely until credentials are provisioned - but the control
// plane that creates databases, seeds accounts and sizes query clusters
// reaches the instance over a network path that is not the public one: a
// cross-cluster internal domain whose access control lives in the gateway in
// front of it, not in milvus. The fork this mechanism replaces served that
// plane on two fixed listeners - a second MilvusService gRPC server and a
// second REST server carrying /metrics - with no authentication interceptor,
// and every existing instance's control plane still speaks to those ports.
// A form that declares this capability is asking for exactly those listeners,
// so a new instance is operable by the same control plane as every old one.
//
// # What declaring this exposes
//
// Both listeners serve the FULL MilvusService with neither authentication nor
// privilege checks. That is safe under exactly one assumption: nothing
// reaches the ports except the control plane, enforced outside the process.
// A form must not declare this capability unless its deployment guarantees
// that isolation.
//
// With no provider installed - or the capability nil - no listener is opened
// and milvus serves exactly the surfaces it always did.
type InternalSurfaces interface {
	// InternalDomainPorts returns the ports for the internal-domain gRPC
	// listener (the unauthenticated MilvusService) and the internal REST
	// listener (unauthenticated /v2/vectordb plus /metrics). A zero disables
	// that listener individually.
	InternalDomainPorts() (grpcPort int, restPort int)
}
