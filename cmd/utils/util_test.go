package utils

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPIdBasic(t *testing.T) {
	sType := "test"
	alias := "alias"
	InitRuntimeDir(os.Stdout, sType)

	// create file
	filename := GetPidFileName(sType, alias)
	fd, err := CreateFileWithContent(GlobalRuntimeDir.GetOperationLogger(), GlobalRuntimeDir.GetDir(), filename, "1111")
	assert.NoError(t, err)
	defer RemoveFile(fd)

	// read file
	reader, err := OpenFile(GlobalRuntimeDir.GetDir(), filename)
	assert.NoError(t, err)
	defer CloseFile(reader)
	var pid int
	lines, err := fmt.Fscanf(reader, "%d", &pid)
	assert.NoError(t, err)
	assert.Equal(t, lines, 1)
	assert.Equal(t, pid, 1111)
}

func TestSIdBasic(t *testing.T) {
	sType := "test"
	alias := "alias"
	InitRuntimeDir(os.Stdout, sType)

	// create file
	filename := GetServerIDFileName(sType, alias)
	fd, err := CreateFileWithContent(GlobalRuntimeDir.GetOperationLogger(), GlobalRuntimeDir.GetDir(), filename, "2222")
	assert.NoError(t, err)
	defer RemoveFile(fd)

	// read file
	reader, err := OpenFile(GlobalRuntimeDir.GetDir(), filename)
	assert.NoError(t, err)
	defer CloseFile(reader)
	var pid int
	lines, err := fmt.Fscanf(reader, "%d", &pid)
	assert.NoError(t, err)
	assert.Equal(t, lines, 1)
	assert.Equal(t, pid, 2222)
}
