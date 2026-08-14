package milvus

import "testing"

// Main must return normally on too few arguments, taking the usage branch
// rather than panicking or exiting. That is the contract a distribution's own main relies on.
func TestMainWithoutSubcommandReturns(t *testing.T) {
	Main([]string{"milvus"})
}
