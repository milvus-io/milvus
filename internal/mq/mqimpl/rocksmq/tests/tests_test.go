package tests

import (
	"fmt"
	"os"
	"testing"
)

func TestXxx(t *testing.T) {
	for _, e := range os.Environ() {
		fmt.Printf("%s\n", e)
	}
}
