package reader

import (
	"testing"
)

func TestReader_startQueryNode(t *testing.T) {
	pulsarURL := "pulsar://192.168.2.28:6650"
	startQueryNode(pulsarURL)
}
