package grpcproxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/extension"
)

type portsProvider struct{ grpcPort, restPort int }

func (portsProvider) Name() string                       { return "test" }
func (portsProvider) Requires() []extension.CapabilityID { return nil }
func (p portsProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{InternalSurfaces: p}
}
func (p portsProvider) InternalDomainPorts() (int, int) { return p.grpcPort, p.restPort }

// With no capability installed no listener is opened: a stock binary serves
// exactly the surfaces it always did.
func TestInternalDomainPortsAreZeroWithoutAProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	grpcPort, restPort := internalDomainPorts()
	assert.Zero(t, grpcPort)
	assert.Zero(t, restPort)
}

// A form's declaration is passed through verbatim; zero disables a listener
// individually.
func TestInternalDomainPortsFollowTheDeclaration(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	assert.NoError(t, extension.SetProvider(portsProvider{grpcPort: 26330, restPort: 0}))

	grpcPort, restPort := internalDomainPorts()
	assert.Equal(t, 26330, grpcPort)
	assert.Zero(t, restPort)
}
