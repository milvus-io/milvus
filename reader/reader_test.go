package reader

import (
	"testing"
)

func TestReader_startQueryNode(t *testing.T) {

	pulsarURL := "pulsar://localhost:6650"

	numOfQueryNode := 2

	go StartQueryNode(pulsarURL, numOfQueryNode, 0)
	StartQueryNode(pulsarURL, numOfQueryNode, 1)
}
