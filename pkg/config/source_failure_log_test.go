// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestImmutableConfigStartupLogsProtectEtcdPrefix(t *testing.T) {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	const canary = "immutable-etcd-root-secret-canary"
	info := &EtcdInfo{
		Endpoints: strings.Split(endpoints, ","),
		KeyPrefix: canary + "/" + filepath.Base(filepath.Dir(t.TempDir())),
	}
	client, err := newEtcdClient(info)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := client.Delete(ctx, info.KeyPrefix+"/", clientv3.WithPrefix())
		assert.NoError(t, err)
		assert.NoError(t, client.Close())
	})
	source, err := NewEtcdSource(client, info)
	require.NoError(t, err)
	mgr := NewManager()
	t.Cleanup(mgr.Close)
	require.NoError(t, mgr.AddSource(source))
	mgr.RegisterConfigKey("mq.type")
	mgr.SetConfig("mq.type", "woodpecker")
	mgr.ImmutableUpdate("mq.type")
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
	require.NoError(t, mgr.ProcessImmutableConfigs(nil))
	// Exercise the create-if-absent race branch after startup pinned the value.
	require.NoError(t, mgr.SaveConfigToEtcd(source, formatKey("mq.type"), "replacement"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := client.Get(ctx, info.KeyPrefix+"/config/"+formatKey("mq.type"))
	require.NoError(t, err)
	require.Len(t, response.Kvs, 1)
	assert.Equal(t, "woodpecker", string(response.Kvs[0].Value))
	output := sink.String()
	assert.Contains(t, output, "config atomically saved to etcd")
	assert.Contains(t, output, "config already exists in etcd, skip writing")
	assert.NotContains(t, output, canary)
}

func TestEtcdSourceInitializationLogsProtectConfig(t *testing.T) {
	const canary = "etcd-source-secret-canary"
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
	// Construction with no refresh interval does not access the injected client.
	client := &clientv3.Client{}
	source, err := NewEtcdSource(client, &EtcdInfo{
		EnableAuth: true,
		UseSSL:     true,
		UserName:   canary,
		PassWord:   canary,
		Endpoints:  []string{canary},
		KeyPrefix:  canary,
		CertFile:   canary,
		KeyFile:    canary,
		CaCertFile: canary,
		MinVersion: canary,
	})
	require.NoError(t, err)
	t.Cleanup(source.Close)
	assert.Same(t, client, source.etcdCli)
	assert.Equal(t, canary, source.keyPrefix)
	output := sink.String()
	assert.Contains(t, output, "init etcd source")
	assert.Contains(t, output, "endpointCount")
	for _, protected := range []string{canary, "authEnabled", "tlsEnabled"} {
		assert.NotContains(t, output, protected)
	}
}

func TestFileSourceFailureLogsProtectConfig(t *testing.T) {
	const canary = "yaml-secret-canary"
	for _, periodic := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial", true: "periodic"}[periodic], func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "milvus.yaml")
			invalid := []byte("minio:\n  secretAccessKey: !!int " + canary + "\n")
			content := invalid
			if periodic {
				content = []byte("minio:\n  secretAccessKey: old\n")
			}
			require.NoError(t, os.WriteFile(filename, content, 0o600))
			mgr := NewManager()
			t.Cleanup(mgr.Close)
			mgr.RegisterConfigKey("minio.secretAccessKey")
			mgr.RegisterSensitiveKey("minio.secretAccessKey")
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
			fs := NewFileSource(&FileInfo{Files: []string{filename}, RefreshInterval: 5 * time.Millisecond})
			err := mgr.AddSource(fs)
			if periodic {
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filename, invalid, 0o600))
				require.Eventually(t, func() bool { return strings.Contains(sink.String(), "can not pull configs") }, time.Second, time.Millisecond)
				fs.Close()
				_, err = fs.GetConfigurations()
			}
			require.ErrorContains(t, err, canary, "preserve parser details for the in-process consumer")
			assert.NotContains(t, sink.String(), canary)
		})
	}
}

func TestConfigInitFailureLogsProtectConfig(t *testing.T) {
	const childEnv = "MILVUS_TEST_CONFIG_INIT_FAILURE"
	if filename := os.Getenv(childEnv); filename != "" {
		Init(WithFilesSource(&FileInfo{Files: []string{filename}}))
		t.Fatal("config initialization did not terminate")
	}
	const canary = "yaml-init-secret-canary"
	filename := filepath.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(filename, []byte("minio:\n  secretAccessKey: !!int "+canary+"\n"), 0o600))
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(executable, "-test.run=^TestConfigInitFailureLogsProtectConfig$") // #nosec G204 -- Re-exec the current test binary with a fixed test filter.
	cmd.Env = append(os.Environ(), childEnv+"="+filename)
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())
	assert.Contains(t, string(output), "failed to add FileSource config")
	assert.NotContains(t, string(output), canary)
}

func TestYAMLFlattenFailureLogsProtectConfig(t *testing.T) {
	const canary = "yaml-dynamic-key-canary"
	filename := filepath.Join(t.TempDir(), "hook.yaml")
	require.NoError(t, os.WriteFile(filename, []byte(canary+":\n  - key: .nan\n"), 0o600))
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writer
	defer func() { os.Stdout = stdout; reader.Close(); writer.Close() }()
	fs := NewFileSource(&FileInfo{Files: []string{filename}})
	defer fs.Close()
	values, err := fs.GetConfigurations()
	require.NoError(t, err, "preserve the existing skip-invalid-entry fallback")
	assert.Empty(t, values)
	require.NoError(t, writer.Close())
	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.NotContains(t, string(output)+sink.String(), canary)
}
