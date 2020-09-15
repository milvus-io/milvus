package reader

import (
	"testing"
)

func TestReader_startQueryNode(t *testing.T) {

	pulsarURL := "pulsar://localhost:6650"
	StartQueryNode(pulsarURL)
}
