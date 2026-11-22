// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateConfigDocs(t *testing.T) {
	const config = `# TLS configuration.
tls:
  serverPemPath: configs/cert/server.pem
  serverKeyPath: configs/cert/server.key
  caPemPath: configs/cert/ca.pem
common:
  security:
    authorizationEnabled: false
    tlsMode: 0
  session:
    # Session lifetime.
    # Measured in seconds.
    ttl: 15
etcd:
  endpoints: [localhost:2379, localhost:2380] # Etcd endpoints.
# unpublished:
#   internalSetting: true
`

	dir := t.TempDir()
	oldInputFile, oldOutputPath := inputFile, outputPath
	t.Cleanup(func() {
		inputFile, outputPath = oldInputFile, oldOutputPath
	})
	inputFile, outputPath = filepath.Join(dir, "milvus.yaml"), dir
	require.NoError(t, os.WriteFile(inputFile, []byte(config), 0o600))
	require.NoError(t, run())

	readPage := func(name string) string {
		t.Helper()
		page, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		return string(page)
	}

	index := readPage("system_configuration.md")
	assert.Contains(t, index, "3 sections")
	assert.NotContains(t, index, "%s sections")
	assert.NotContains(t, index, "all parameters take effect only")
	assert.Contains(t, index, "[Configure Milvus on the Fly](dynamic_config.md)")
	assert.Contains(t, index, "[tls-related Configurations](configure_tls.md)")
	assert.NotContains(t, index, "unpublished")

	tlsPage := readPage("configure_tls.md")
	assert.Contains(t, tlsPage, "TLS configuration.")
	assert.Contains(t, tlsPage, "<table id=\"tls.serverPemPath\">")
	assert.Contains(t, tlsPage, "<td></td>\n      <td>configs/cert/server.pem</td>")
	assert.Contains(t, tlsPage, "<table id=\"tls.serverKeyPath\">")
	assert.Contains(t, tlsPage, "<td>configs/cert/server.key</td>")
	assert.Contains(t, tlsPage, "<table id=\"tls.caPemPath\">")
	assert.Contains(t, tlsPage, "<td>configs/cert/ca.pem</td>")

	common := readPage("configure_common.md")
	assert.Contains(t, common, "<table id=\"common.security.authorizationEnabled\">")
	assert.Contains(t, common, "<td></td>\n      <td>false</td>")
	assert.Contains(t, common, "<table id=\"common.security.tlsMode\">")
	assert.Contains(t, common, "<td></td>\n      <td>0</td>")
	assert.Contains(t, common, "<table id=\"common.session.ttl\">")
	assert.Contains(t, common, "<li>Session lifetime.</li>")
	assert.Contains(t, common, "<li>Measured in seconds.</li>")
	assert.Contains(t, common, "<td>15</td>")

	etcd := readPage("configure_etcd.md")
	assert.Contains(t, etcd, "<td>        Etcd endpoints.      </td>")
	assert.Contains(t, etcd, "<td>localhost:2379, localhost:2380</td>")
}
