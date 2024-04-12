package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckIndexValid(t *testing.T) {
	scalarIndexChecker := &scalarIndexChecker{}
	assert.NoError(t, scalarIndexChecker.CheckTrain(map[string]string{}))
}
