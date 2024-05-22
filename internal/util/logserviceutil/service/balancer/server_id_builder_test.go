package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

func TestServerIDPickerBuilder(t *testing.T) {
	builder := &serverIDPickerBuilder{}
	picker := builder.Build(base.PickerBuildInfo{})
	assert.NotNil(t, picker)
	_, err := picker.Pick(balancer.PickInfo{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, balancer.ErrNoSubConnAvailable)
}
