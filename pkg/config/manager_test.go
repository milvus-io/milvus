// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"bytes"
	"context"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestAllConfigFromManager(t *testing.T) {
	mgr, _ := Init()
	all := mgr.GetConfigs()
	assert.Equal(t, 0, len(all))

	t.Setenv("MILVUS_CONF_PUBLIC_KEY", "declared-value")
	mgr, _ = Init(WithEnvSource(formatKey))
	raw := mgr.GetConfigs()
	assert.Less(t, 0, len(raw))

	// Nothing is declared yet, so the safe projection is empty: the process
	// environment that EnvSource imported is not Milvus configuration, and
	// publishing the names of every variable in the pod is a disclosure of its
	// own even with the values masked.
	all = mgr.ProjectConfigs()
	assert.Equal(t, 0, len(all))

	// Declaring one key publishes that key — under both the canonical identity
	// and the environment alias EnvSource stored it as, since both resolve to
	// the same declared ParamItem — and nothing else.
	mgr.RegisterConfigKey("public.key")
	all = mgr.ProjectConfigs()
	assert.Equal(t, map[string]string{
		formatKey("public.key"): "declared-value",
		"PUBLIC_KEY":            "declared-value",
	}, all)
}

func TestConfigChangeEvent(t *testing.T) {
	dir, _ := os.MkdirTemp("", "milvus")
	os.WriteFile(path.Join(dir, "milvus.yaml"), []byte("a.b: 1\nc.d: 2"), 0o600)
	os.WriteFile(path.Join(dir, "user.yaml"), []byte("a.b: 3"), 0o600)

	fs := NewFileSource(&FileInfo{[]string{path.Join(dir, "milvus.yaml"), path.Join(dir, "user.yaml")}, 1})
	mgr, _ := Init()
	err := mgr.AddSource(fs)
	assert.NoError(t, err)
	_, res, err := mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, res, "3")
	os.WriteFile(path.Join(dir, "user.yaml"), []byte("a.b: 6"), 0o600)
	time.Sleep(3 * time.Second)
	_, res, err = mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, res, "6")
}

func TestAllDupliateSource(t *testing.T) {
	mgr, _ := Init()
	err := mgr.AddSource(NewEnvSource(formatKey))
	assert.NoError(t, err)
	err = mgr.AddSource(NewEnvSource(formatKey))
	assert.Error(t, err)

	err = mgr.AddSource(ErrSource{})
	assert.Error(t, err, "error")

	err = mgr.pullSourceConfigs("ErrSource")
	assert.Error(t, err, "invalid source or source not added")
}

func TestBasic(t *testing.T) {
	mgr, _ := Init()

	// test set config
	mgr.SetConfig("a.b", "aaa")
	_, value, err := mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, value, "aaa")
	_, _, err = mgr.GetConfig("a.a")
	assert.Error(t, err)

	// test delete config
	mgr.SetConfig("a.b", "aaa")
	mgr.DeleteConfig("a.b")
	assert.Error(t, err)

	// test reset config
	mgr.ResetConfig("a.b")
	assert.Error(t, err)

	// test forbid config
	envSource := NewEnvSource(formatKey)
	err = mgr.AddSource(envSource)
	assert.NoError(t, err)

	envSource.configs.Insert("ab", "aaa")
	mgr.OnEvent(&Event{
		EventSource: envSource.GetSourceName(),
		EventType:   CreateType,
		Key:         "ab",
		Value:       "aaa",
	})
	_, value, err = mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, value, "aaa")

	mgr.ForbidUpdate("a.b")
	mgr.OnEvent(&Event{
		EventSource: envSource.GetSourceName(),
		EventType:   UpdateType,
		Key:         "a.b",
		Value:       "bbb",
	})
	_, value, err = mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, value, "aaa")

	configs := mgr.FileConfigs()
	assert.Len(t, configs, 0)
}

func TestSensitiveConfigRedaction(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterConfigKey("querynode.gracefulStopTimeout")
	mgr.RegisterConfigPrefix("credential.")
	mgr.RegisterSensitiveKey("minio.address")
	mgr.RegisterSensitivePrefix("credential.")
	mgr.SetMapConfig("credential.aksk1.secret_access_key", "group-secret")

	assert.True(t, isRegistered(mgr, "querynode.graceful_stop_timeout"))
	assert.True(t, isRegistered(mgr, "credential.aksk1.secret_access_key"))
	assert.False(t, isRegistered(mgr, "OPAQUE_RUNTIME_VALUE"))
	// A member of a declared group counts even before anything sets it: see
	// TestRegisteredGroupMemberLifecycle for why refusing it would break the
	// write-then-delete cycle.
	assert.True(t, isRegistered(mgr, "credential.aksk1.never_declared"))

	for _, key := range []string{
		"minio.address",
		"MINIO_ADDRESS",
		"credential.aksk1.secret_access_key",
		"AWS_SECRET_ACCESS_KEY",
		"service.api_key",
		"tls.private-key",
		"hook.backendURL",
		"storage.endpoint",
		"mq.brokerList",
		"metadata.rootPath",
	} {
		assert.True(t, mgr.IsSensitive(key), key)
	}
	assert.False(t, mgr.IsSensitive("querynode.gracefulStopTimeout"))

	for key, want := range map[string]string{
		"credential.aksk1.secret_access_key": RedactedValue,
		"AWS_SESSION_TOKEN":                  RedactedValue,
		"OPAQUE_RUNTIME_VALUE":               RedactedValue,
		"querynode.gracefulStopTimeout":      "30",
	} {
		assert.Equal(t, want, mgr.RedactValue(key, "30"), key)
	}
}

// A declared ParamItem must resolve identically however it is spelled, because
// sensitivity is decided from prefixes that only the dotted form can match.
func TestSensitivityIsIndependentOfKeySpelling(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterConfigPrefix("kafka.producer.")
	mgr.RegisterSensitivePrefix("kafka.producer.")
	mgr.RegisterConfigKey("kafka.producer.compression")
	mgr.SetConfig("kafka.producer.compression", "must-not-leak")

	for _, spelling := range []string{
		"kafka.producer.compression",
		"kafka/producer/compression",
		"KAFKA_PRODUCER_COMPRESSION",
		formatKey("kafka.producer.compression"),
	} {
		assert.True(t, mgr.IsSensitive(spelling), spelling)
		assert.Equal(t, RedactedValue, mgr.RedactValue(spelling, "must-not-leak"), spelling)
	}

	assert.Equal(t, RedactedValue, mgr.ProjectConfigs()[formatKey("kafka.producer.compression")])
	_, _, err := mgr.GetRegisteredConfig("kafka.producer.compression")
	require.ErrorIs(t, err, ErrKeySensitive)

	// An explicit NonSensitive declaration still overrides the group default.
	mgr.RegisterNonSensitiveKey("kafka.producer.compression")
	assert.False(t, mgr.IsSensitive(formatKey("kafka.producer.compression")))
	assert.Equal(t, "must-not-leak", mgr.ProjectConfigs()[formatKey("kafka.producer.compression")])
}

// syncBuffer adapts a bytes.Buffer to the WriteSyncer the logger expects.
// Declared locally rather than using zapcore.AddSync because depguard forbids
// importing zap outside pkg/mlog; the interface is structural, so naming the
// package is unnecessary. Same pattern as mlog's own benchmark test.
type syncBuffer struct {
	bytes.Buffer
}

func (s *syncBuffer) Sync() error { return nil }

func TestSensitiveConfigEventLogRedaction(t *testing.T) {
	var logs syncBuffer
	logger, props, err := mlog.InitLoggerWithWriteSyncer(&mlog.Config{
		Level:             "info",
		Format:            "text",
		DisableCaller:     true,
		DisableTimestamp:  true,
		DisableStacktrace: true,
	}, &logs)
	require.NoError(t, err)

	oldLogger := mlog.L()
	oldLevel := mlog.GetAtomicLevel()
	mlog.ReplaceGlobals(logger, props)
	defer mlog.ReplaceGlobals(oldLogger, &mlog.ZapProperties{Level: oldLevel})

	mgr := NewManager()
	mgr.RegisterConfigPrefix("credential.")
	mgr.RegisterSensitivePrefix("credential.")
	mgr.OnEvent(&Event{
		EventSource: "test",
		EventType:   CreateType,
		Key:         "credential.aksk1.secret_access_key",
		Value:       "must-not-appear-in-logs",
	})

	assert.NotContains(t, logs.String(), "must-not-appear-in-logs")
	assert.True(t, strings.Contains(logs.String(), RedactedValue), logs.String())
}

func TestOnEvent(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = t.TempDir()
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()

	client := v3client.New(e.Server)

	dir := t.TempDir()
	yamlFile := path.Join(dir, "milvus.yaml")
	os.WriteFile(yamlFile, []byte("a.b: \"\""), 0o600)
	mgr, _ := Init(WithEnvSource(formatKey),
		WithFilesSource(&FileInfo{
			Files:           []string{yamlFile},
			RefreshInterval: 10 * time.Millisecond,
		}),
		WithEtcdSource(&EtcdInfo{
			Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
			KeyPrefix:       "test",
			RefreshInterval: 10 * time.Millisecond,
		}))
	os.WriteFile(yamlFile, []byte("a.b: aaa"), 0o600)
	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "aaa"
	}, time.Second*5, time.Second)

	ctx := context.Background()
	client.Put(ctx, "test/config/a/b", "bbb")

	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "bbb"
	}, time.Second*5, time.Second)

	client.Put(ctx, "test/config/a/b", "ccc")
	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "ccc"
	}, time.Second*5, time.Second)

	os.WriteFile(yamlFile, []byte("a.b: ddd"), 0o600)
	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "ccc"
	}, time.Second*5, time.Second)

	client.Delete(ctx, "test/config/a/b")
	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "ddd"
	}, time.Second*5, time.Second)
}

func isRegistered(m *Manager, key string) bool {
	_, kind := m.ResolveRegisteredConfigKey(key)
	return kind != RegisteredConfigUnknown
}

func TestGetConfigAndSource(t *testing.T) {
	mgr, _ := Init()
	envSource := NewEnvSource(formatKey)
	err := mgr.AddSource(envSource)
	assert.NoError(t, err)

	mgr.RegisterConfigKey("ab-key")
	mgr.RegisterConfigKey("ac-key")

	envSource.configs.Insert("ab-key", "ab-value")
	mgr.OnEvent(&Event{
		EventSource: envSource.GetSourceName(),
		EventType:   CreateType,
		Key:         "ab-key",
	})

	mgr.SetConfig("ac-key", "ac-value")
	_, value, err := mgr.GetConfig("ac-key")
	assert.NoError(t, err)
	assert.Equal(t, value, "ac-value")

	// test get all configs
	configs := mgr.GetConfigsView()
	v, ok := configs["ab-key"]
	assert.True(t, ok)
	assert.Contains(t, v, "EnvironmentSource")

	v, ok = configs["ac-key"]
	assert.True(t, ok)
	assert.Contains(t, v, RuntimeSource)
}

func TestConfigProjectionsRedactByDefault(t *testing.T) {
	mgr, _ := Init()

	mgr.RegisterConfigKey("public.key")
	mgr.RegisterConfigKey("opaque.key")
	mgr.RegisterSensitiveKey("opaque.key")
	mgr.RegisterConfigPrefix("dynamic.")
	mgr.RegisterConfigPrefix("sensitive.group.")
	mgr.RegisterSensitivePrefix("sensitive.group.")

	mgr.SetConfig("public.key", "visible")
	mgr.SetConfig("opaque.key", "opaque-secret")
	mgr.SetConfig("unknown.key", "unknown-secret")
	mgr.SetMapConfig("dynamic.visible", "dynamic-value")
	mgr.SetMapConfig("dynamic.password", "dynamic-secret")
	mgr.SetMapConfig("sensitive.group.value", "group-secret")
	mgr.SetMapConfig("sensitive/group/slash", "slash-group-secret")
	mgr.SetMapConfig("sensitive.group.slash", "slash-group-secret")

	publicKey := formatKey("public.key")
	opaqueKey := formatKey("opaque.key")
	unknownKey := formatKey("unknown.key")
	dynamicKey := "dynamic.visible"
	dynamicSecretKey := "dynamic.password"
	groupKey := "sensitive.group.value"
	slashGroupKey := "sensitive/group/slash"

	safe := mgr.ProjectConfigs()
	assert.Equal(t, "visible", safe[publicKey])
	assert.Equal(t, RedactedValue, safe[opaqueKey])
	assert.Equal(t, "dynamic-value", safe[dynamicKey])
	assert.Equal(t, RedactedValue, safe[dynamicSecretKey])
	assert.Equal(t, RedactedValue, safe[groupKey])
	assert.Equal(t, RedactedValue, safe[slashGroupKey])
	// A declared key is named and masked; an undeclared one is not named at all.
	assert.NotContains(t, safe, unknownKey)

	raw := mgr.GetConfigs()
	assert.Equal(t, "opaque-secret", raw[opaqueKey])
	assert.Equal(t, "unknown-secret", raw[unknownKey])
	assert.Equal(t, "group-secret", raw[groupKey])
	assert.Equal(t, "dynamic-secret", raw[dynamicSecretKey])
	assert.Equal(t, "slash-group-secret", raw[slashGroupKey])

	safeView := mgr.GetConfigsView()
	assert.Contains(t, safeView[publicKey], RuntimeSource)
	// A redacted entry keeps the value[source] shape: which source supplies a
	// credential is not itself secret, and it is the most useful thing left to
	// say about it.
	assert.Equal(t, RedactedValue+"["+RuntimeSource+"]", safeView[opaqueKey])
	assert.NotContains(t, safeView, unknownKey)

	assert.Equal(t, "dynamic-value", mgr.ProjectBy(WithPrefix("dynamic"))[dynamicKey])
	assert.Equal(t, RedactedValue, mgr.ProjectBy(WithPrefix("dynamic"))[dynamicSecretKey])
	assert.Equal(t, "group-secret", mgr.GetBy(WithPrefix("sensitive"))[groupKey])

	source, value, err := mgr.GetRegisteredConfig("dynamic/visible")
	require.NoError(t, err)
	assert.Equal(t, RuntimeSource, source)
	assert.Equal(t, "dynamic-value", value)

	_, _, err = mgr.GetRegisteredConfig("unknown.key")
	require.ErrorIs(t, err, ErrKeyUnregistered)
	_, _, err = mgr.GetRegisteredConfig("opaque.key")
	require.ErrorIs(t, err, ErrKeySensitive)
	_, _, err = mgr.GetRegisteredConfig("sensitive.group.value")
	require.ErrorIs(t, err, ErrKeySensitive)
}

// mapSource is a high-priority source that holds exactly the keys it is given,
// so a test can reproduce what EtcdSource does: AlterConfigsInEtcd formats the
// key before writing, so an altered ParamGroup member exists in etcd under the
// separator-free identity ONLY, while the yaml still carries the dotted one.
type mapSource struct {
	name    string
	configs map[string]string
}

func (s *mapSource) GetConfigurations() (map[string]string, error) { return s.configs, nil }

func (s *mapSource) GetConfigurationByKey(key string) (string, error) {
	v, ok := s.configs[key]
	if !ok {
		return "", errors.Wrap(ErrKeyNotFound, key)
	}
	return v, nil
}

func (s *mapSource) GetPriority() int           { return HighPriority }
func (s *mapSource) GetSourceName() string      { return s.name }
func (*mapSource) SetEventHandler(EventHandler) {}
func (*mapSource) SetManager(ConfigManager)     {}
func (*mapSource) UpdateOptions(Options)        {}
func (*mapSource) Close()                       {}

// Reading a ParamGroup member by its dotted key alone finds the yaml entry and
// misses the override entirely, which is why the lookup must try the
// separator-free identity first. Swapping that order makes this test fail.
func TestRegisteredGroupMemberSeesEtcdOverride(t *testing.T) {
	const key = "proxy.accessLog.formatters.base.format"
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile, []byte(key+": from-yaml\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigPrefix("proxy.accessLog.formatters.")

	source, value, err := mgr.GetRegisteredConfig(key)
	require.NoError(t, err)
	assert.Equal(t, "from-yaml", value)
	assert.Equal(t, "FileSource", source)

	etcd := &mapSource{name: "EtcdLikeSource", configs: map[string]string{formatKey(key): "from-alter"}}
	require.NoError(t, mgr.AddSource(etcd))

	source, value, err = mgr.GetRegisteredConfig(key)
	require.NoError(t, err)
	assert.Equal(t, "from-alter", value, "an etcd override must not be reported as the stale yaml value")
	assert.Equal(t, etcd.name, source)
	// The management read must agree with what the process actually uses.
	_, live, err := mgr.GetConfig(key)
	require.NoError(t, err)
	assert.Equal(t, live, value)
}

// A ParamGroup member has to be creatable, readable and deletable through the
// management endpoints even when no yaml ever mentioned it: AlterConfigsInEtcd
// stores it under the separator-free identity only, so a rule that demanded the
// dotted spelling would accept the write and then refuse to read or delete it.
func TestRegisteredGroupMemberLifecycle(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterConfigPrefix("kafka.producer.")

	// Nothing anywhere yet: still resolves as a member of a declared group, so
	// it can be read, written and deleted like any other.
	_, kind := mgr.ResolveRegisteredConfigKey("kafka.producer.linger.ms")
	assert.Equal(t, RegisteredConfigGroup, kind)

	// What an alter call leaves behind.
	mgr.SetConfig("kafka.producer.compression.type", "zstd")
	canonical, kind := mgr.ResolveRegisteredConfigKey("kafka.producer.compression.type")
	assert.Equal(t, RegisteredConfigGroup, kind, "the key just written must still resolve, or it can never be deleted")
	assert.Equal(t, "kafka.producer.compression.type", canonical)
}

// GetRegisteredConfig must report the value the process actually uses. A scalar
// is read by ParamItem.get through Manager.GetConfig, which looks only under the
// separator-free identity, so a dotted overlay left by SetMapConfig is not a
// value anything consumes and must not be reported as one.
func TestRegisteredScalarIgnoresDottedOverlay(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterConfigKey("proxy.maxNameLength")
	// The dotted spelling is the only thing set, and nothing in the process
	// reads a ParamItem under it.
	mgr.SetMapConfig("proxy.maxNameLength", "stray-dotted-overlay")

	_, _, liveErr := mgr.GetConfig("proxy.maxNameLength")
	require.ErrorIs(t, liveErr, ErrKeyNotFound, "ParamItem.get would not find this either")
	_, _, err := mgr.GetRegisteredConfig("proxy.maxNameLength")
	require.ErrorIs(t, err, ErrKeyNotFound, "so the management API must not report it as the value in force")

	// Once it is set the way a ParamItem is actually read, both agree.
	mgr.SetConfig("proxy.maxNameLength", "the-value-milvus-uses")
	_, live, err := mgr.GetConfig("proxy.maxNameLength")
	require.NoError(t, err)
	_, reported, err := mgr.GetRegisteredConfig("proxy.maxNameLength")
	require.NoError(t, err)
	assert.Equal(t, live, reported)
}

// The one ambiguous case stays closed: a key whose sole backing is a process
// environment variable that collapses into the group's separator-free
// namespace. Everything else below a declared prefix is configuration.
func TestRegisteredGroupMemberRejectsEnvironmentOnlyKey(t *testing.T) {
	t.Setenv("PROXY_ACCESSLOG_FORMATTERS_DATABASE_URL", "env-secret")

	mgr, _ := Init(WithEnvSource(formatKey))
	mgr.RegisterConfigPrefix("proxy.accessLog.formatters.")

	_, kind := mgr.ResolveRegisteredConfigKey("proxy.accessLog.formatters.DATABASE_URL")
	assert.Equal(t, RegisteredConfigUnknown, kind)
	_, _, err := mgr.GetRegisteredConfig("proxy.accessLog.formatters.DATABASE_URL")
	require.ErrorIs(t, err, ErrKeyUnregistered)

	// A member of the same group that the environment does not back resolves
	// normally, so the refusal is targeted rather than a blanket one.
	_, kind = mgr.ResolveRegisteredConfigKey("proxy.accessLog.formatters.base.format")
	assert.Equal(t, RegisteredConfigGroup, kind)
}

// formatKey exempts knowhere.* from separator stripping; the EnvSource key
// formatter BaseTable installs (base_table.go) does not. An environment variable
// whose name lands in that gap must not be mistaken for a member of the knowhere
// group, which is what strippedKey exists to prevent.
func TestKnowherePrefixDoesNotAdmitEnvironmentVariables(t *testing.T) {
	t.Setenv("KNOWHERE.INJECTED", "must-not-be-published")
	t.Setenv("knowhere.lowercase", "must-not-be-published")

	// strippedKey is the production formatter's shape: no NotFormatPrefix
	// exemption. Passing formatKey here instead would hide the very gap the test
	// is for.
	mgr, _ := Init(WithEnvSource(strippedKey))
	mgr.RegisterConfigPrefix(NotFormatPrefix)

	require.NotEmpty(t, mgr.GetConfigs(), "the environment was never imported, the test proves nothing")
	for _, spelling := range []string{"KNOWHERE.INJECTED", "knowhere.INJECTED", "knowhere.lowercase"} {
		_, kind := mgr.ResolveRegisteredConfigKey(spelling)
		assert.Equal(t, RegisteredConfigUnknown, kind, spelling)
	}
	for key, value := range mgr.GetConfigsView() {
		assert.NotContains(t, value, "must-not-be-published", key)
	}
}

// The mirror image: an overlay written under the spelling its own consumer does
// not read is inert, so the projection must not advertise it.
func TestProjectionsOmitInertOverlays(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterConfigKey("some.declared.key")
	mgr.RegisterConfigPrefix("some.group.")

	// SetMapConfig on a scalar: ParamItem.get resolves the separator-free
	// identity and never sees this.
	mgr.SetMapConfig("some.declared.key", "inert-dotted")
	_, _, err := mgr.GetConfig("some.declared.key")
	require.ErrorIs(t, err, ErrKeyNotFound)
	assert.NotContains(t, mgr.ProjectConfigs(), "some.declared.key")
	assert.NotContains(t, mgr.GetConfigsView(), "some.declared.key")

	// A group member under the collapsed identity is NOT inert: that is the
	// identity AlterConfigsInEtcd writes, and a caller that builds the key
	// itself reads it back through Manager.GetConfig. Only the ParamGroup
	// aggregate misses it, and the aggregate is not the only consumer.
	mgr.SetConfig("some.group.member", "written-by-alter")
	assert.Equal(t, "written-by-alter", mgr.ProjectConfigs()[formatKey("some.group.member")])
	_, live, err := mgr.GetConfig("some.group.member")
	require.NoError(t, err)
	assert.Equal(t, "written-by-alter", live)

	// And the authoritative scalar spelling still comes through.
	mgr.SetConfig("some.declared.key", "live")
	assert.Equal(t, "live", mgr.ProjectConfigs()[formatKey("some.declared.key")])
}

// A member written through /management/config/alter lands in etcd under the
// collapsed identity only. It is in force — grpcConfig's per-cluster CDC
// settings read exactly that way — so the projections have to show it, or the
// endpoint accepts a write it then denies ever happened.
func TestAlterWrittenGroupMemberIsProjected(t *testing.T) {
	const dotted = "tls.clusters.prod.caPemPath"
	mgr := NewManager()
	mgr.RegisterConfigPrefix("tls.clusters.")
	require.NoError(t, mgr.AddSource(&mapSource{
		name:    "EtcdLikeSource",
		configs: map[string]string{EtcdConfigKey(dotted): "/etc/ca.pem"},
	}))

	_, kind := mgr.ResolveRegisteredConfigKey(dotted)
	assert.Equal(t, RegisteredConfigGroup, kind)
	assert.Contains(t, mgr.GetConfigsView()[EtcdConfigKey(dotted)], "/etc/ca.pem")
	_, reported, err := mgr.GetRegisteredConfig(dotted)
	require.NoError(t, err)
	// What a caller that builds the key itself reads.
	_, live, err := mgr.GetConfig(dotted)
	require.NoError(t, err)
	assert.Equal(t, live, reported)
}

// The same fuzzy match must not be extended to the environment: a collapsed
// name cannot be checked against the namespace's structure, and the environment
// holds everything in the pod rather than only configuration.
func TestCollapsedMatchIsNotExtendedToTheEnvironment(t *testing.T) {
	t.Setenv("TLS_CLUSTERS_PROD_CAPEMPATH", "/etc/env.pem")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
	t.Setenv("DATABASE_URL", "db-secret")

	mgr, _ := Init(WithEnvSource(formatKey))
	mgr.RegisterConfigPrefix("tls.clusters.")

	require.NotEmpty(t, mgr.GetConfigs(), "the environment was never imported")
	_, kind := mgr.ResolveRegisteredConfigKey("tls.clusters.prod.caPemPath")
	assert.Equal(t, RegisteredConfigUnknown, kind)
	assert.Empty(t, mgr.ProjectConfigs())
	assert.Empty(t, mgr.GetConfigsView())
}

// The one product of the two mechanisms this change adds: a member of a
// Sensitive ParamGroup that no ParamItem declares, reached by the collapsed
// spelling. Nothing normalises such a key — that is what makes it a
// ParamGroup member rather than a ParamItem — so a sensitivity rule that only
// consulted the dotted form would classify one spelling of an inline private
// key secret and the other, the same entry, public. FileSource inserts both
// spellings of every key it loads, so no attacker input is needed to reach it.
func TestSensitiveGroupMemberIsRedactedUnderEverySpelling(t *testing.T) {
	const secret = "-----BEGIN PRIVATE KEY-----"
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile,
		[]byte("kafka.producer.ssl.key.pem: "+secret+"\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigPrefix("kafka.producer.")
	mgr.RegisterSensitivePrefix("kafka.producer.")

	for _, spelling := range []string{
		"kafka.producer.ssl.key.pem",
		"kafka/producer/ssl/key/pem",
		"kafkaproducersslkeypem",
		"KAFKA_PRODUCER_SSL_KEY_PEM",
		"kafka_producer_ssl_key_pem",
	} {
		assert.True(t, mgr.IsSensitive(spelling), spelling)
		_, _, err := mgr.GetRegisteredConfig(spelling)
		require.ErrorIs(t, err, ErrKeySensitive, spelling)
	}

	require.Contains(t, mgr.GetConfigs(), "kafkaproducersslkeypem",
		"FileSource did not insert the collapsed alias, the test proves nothing")
	for key, value := range mgr.ProjectConfigs() {
		assert.NotContains(t, value, secret, key)
	}
	for key, value := range mgr.GetConfigsView() {
		assert.NotContains(t, value, secret, key)
	}
}

// The inverse of TestSensitiveGroupMemberIsRedactedUnderEverySpelling: an
// exempted leaf must be VISIBLE under every spelling too.
//
// Sources insert every key twice, so a projection lists both spellings of each
// entry. Deciding the collapsed one on the group default alone made the two
// disagree — the same setting appeared once readable and once as ***** in one
// response — and quietly reduced NonSensitiveSuffixes to a rule that held for
// half of each key. rememberSpelling is what closes it.
func TestExemptedGroupMemberIsVisibleUnderEverySpelling(t *testing.T) {
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile,
		[]byte("function.textEmbedding.providers.tei.enable: true\n"+
			"function.textEmbedding.providers.tei.credential: provider-secret\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigPrefix("function.textembedding.providers.")
	mgr.RegisterSensitivePrefix("function.textembedding.providers.")
	mgr.RegisterNonSensitiveSuffix("function.textembedding.providers.", "enable")

	for _, spelling := range []string{
		"function.textEmbedding.providers.tei.enable",
		"function/textEmbedding/providers/tei/enable",
		"functiontextembeddingprovidersteienable",
		"FUNCTION_TEXTEMBEDDING_PROVIDERS_TEI_ENABLE",
	} {
		assert.False(t, mgr.IsSensitive(spelling), spelling)
		_, value, err := mgr.GetRegisteredConfig(spelling)
		require.NoError(t, err, spelling)
		assert.Equal(t, "true", value, spelling)
	}
	// The exemption is one leaf, not the namespace.
	assert.True(t, mgr.IsSensitive("functiontextembeddingprovidersteicredential"))

	// No entry may contradict its own alias.
	projection := mgr.ProjectConfigs()
	for key, value := range projection {
		collapsed := formatKeyUncached(key)
		if aliased, ok := projection[collapsed]; ok {
			assert.Equal(t, value, aliased,
				"%s and %s are one entry and must project the same", key, collapsed)
		}
	}
}

// A suffix exemption is granted to a leaf name, so whoever gets to say where the
// leaf begins gets to say whether the exemption applies. That may not be the
// caller.
//
// A member that exists nowhere but etcd — which is where /management/config/alter
// writes, under the collapsed identity and nothing else — has no segmentation
// anyone in the process vouched for. "…newprovidersecret.enable" and
// "…newprovider.secret_enable" are one identity spelled two ways, and only the
// first ends in the leaf the group declared safe. Neither may claim it.
func TestUnendorsedSegmentationCannotClaimAnExemption(t *testing.T) {
	const prefix = "function.textembedding.providers."
	mgr := NewManager()
	mgr.RegisterConfigPrefix(prefix)
	mgr.RegisterSensitivePrefix(prefix)
	mgr.RegisterNonSensitiveSuffix(prefix, "enable")

	const identity = "functiontextembeddingprovidersnewprovidersecretenable"
	for _, spelling := range []string{
		"functiontextembeddingprovidersnewprovidersecretenable",
		"function.textembedding.providers.newprovider.secret_enable",
		// The same identity, re-cut so the leaf reads as the exempted "enable".
		"function.textembedding.providers.newprovidersecret.enable",
	} {
		require.Equal(t, identity, EtcdConfigKey(spelling), spelling)
		assert.True(t, mgr.IsSensitive(spelling),
			"%s: nothing vouched for this segmentation, so the group default stands", spelling)
	}
}

// The exemption does work, for a segmentation a source vouched for.
func TestEndorsedSegmentationKeepsItsExemption(t *testing.T) {
	const prefix = "function.textembedding.providers."
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile,
		[]byte("function.textEmbedding.providers.tei.enable: true\n"+
			"function.textEmbedding.providers.tei.credential: provider-secret\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigPrefix(prefix)
	mgr.RegisterSensitivePrefix(prefix)
	mgr.RegisterNonSensitiveSuffix(prefix, "enable")

	// Both spellings of the entry the file supplied, and both agree.
	for _, spelling := range []string{
		"function.textEmbedding.providers.tei.enable",
		"functiontextembeddingprovidersteienable",
	} {
		assert.False(t, mgr.IsSensitive(spelling), spelling)
		_, value, err := mgr.GetRegisteredConfig(spelling)
		require.NoError(t, err, spelling)
		assert.Equal(t, "true", value, spelling)
	}
	// A runtime overlay vouches for a segmentation too.
	mgr.SetMapConfig("function.textEmbedding.providers.other.enable", "false")
	assert.False(t, mgr.IsSensitive("function.textembedding.providers.other.enable"))

	assert.True(t, mgr.IsSensitive("function.textEmbedding.providers.tei.credential"))
}

// Membership and sensitivity must read a key the same way, or a spelling exists
// that one accepts and the other cannot claim.
//
// A caller can spell a key any way they like, and the management endpoints hand
// whatever they are given straight to the Manager. "kafkaconsumerssl.key.pem"
// matches the collapsed prefix "kafkaconsumer" while matching no dotted prefix,
// so admitting it as a group member — while deciding sensitivity on the dotted
// identity, which no sensitive prefix claims — returned the private key that
// its own canonical spelling refuses. Every spelling that addresses a stored
// value must be refused or redacted, not just the ones a well-behaved caller
// would produce.
func TestHalfCollapsedSpellingCannotReachASensitiveMember(t *testing.T) {
	const secret = "-----BEGIN PRIVATE KEY-----"
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile,
		[]byte("kafka.consumer.ssl.key.pem: "+secret+"\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigPrefix("kafka.consumer.")
	mgr.RegisterSensitivePrefix("kafka.consumer.")

	// Every spelling that collapses to the stored identity, however the caller
	// chose to place the separators. Each is refused and classified sensitive,
	// and none of them returns the key.
	for _, spelling := range []string{
		"kafka.consumer.ssl.key.pem", // as the file wrote it
		"kafkaconsumersslkeypem",     // as the source stores it
		"kafka.consumer.sslkeypem",   // namespace named, leaf run together
		"kafkaconsumerssl.key.pem",   // namespace collapsed, leaf spelled out
		"KAFKACONSUMERSSL.KEY.PEM",
		"kafka.consumersslkeypem",
		"kafkaconsumer.sslkey.pem",
		"kafka/consumer/ssl/key/pem",
	} {
		require.Equal(t, "kafkaconsumersslkeypem", formatKeyUncached(strings.ReplaceAll(strings.ToLower(spelling), "/", ".")),
			"%s does not address the stored entry, so it proves nothing", spelling)

		assert.True(t, mgr.IsSensitive(spelling), spelling)
		_, value, err := mgr.GetRegisteredConfig(spelling)
		require.Error(t, err, spelling)
		assert.NotContains(t, value, secret, spelling)
	}

	// The same holds for an identity no source ever spelled out, which is what
	// an alter-endpoint write leaves behind: nothing to recover the segments
	// from, so the collapsed prefix claims it rather than letting it through.
	assert.True(t, mgr.IsSensitive("kafkaconsumernevertaught"))
}

// A caller does not get to re-segment a key that already exists.
//
// Where the separators fall inside an identity decides two things every rule
// below reads: which namespace the key is in, and which leaf name a
// NonSensitiveSuffixes exemption is being claimed for. So a credential named
// "<provider>.credential_url" could be asked for as "<provider>credential.url",
// the same stored entry with its leaf renamed to one the group declared safe,
// and it came back in the clear from an endpoint with no authentication. The
// same spelling passed the write fence, and addresses the credential's own etcd
// identity.
func TestCallerCannotResegmentAKeyOntoAnExemptedLeaf(t *testing.T) {
	const secret = "provider-secret"
	const prefix = "function.textembedding.providers."
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile,
		[]byte("function.textEmbedding.providers.myprov.credential_url: "+secret+"\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigPrefix(prefix)
	mgr.RegisterSensitivePrefix(prefix)
	for _, suffix := range []string{"enable", "url", "resource_name"} {
		mgr.RegisterNonSensitiveSuffix(prefix, suffix)
	}

	const identity = "functiontextembeddingprovidersmyprovcredentialurl"
	for _, spelling := range []string{
		"function.textEmbedding.providers.myprov.credential_url",
		"functiontextembeddingprovidersmyprovcredentialurl",
		// Re-segmented so the leaf reads as the exempted "url".
		"function.textEmbedding.providers.myprovcredential.url",
		"function.textEmbedding.providers.myprovcredentialurl",
		"function/textEmbedding/providers/myprovcredential/url",
	} {
		canonical, _ := mgr.ResolveRegisteredConfigKey(spelling)
		require.Equal(t, identity, EtcdConfigKey(canonical),
			"%s must address the credential's own identity, or it proves nothing", spelling)

		assert.True(t, mgr.IsSensitive(spelling), spelling)
		_, value, err := mgr.GetRegisteredConfig(spelling)
		require.ErrorIs(t, err, ErrKeySensitive, spelling)
		assert.NotContains(t, value, secret, spelling)
	}

	// The exemption still works for a leaf something vouched for — see
	// TestEndorsedSegmentationKeepsItsExemption for that in full.
	mgr.SetMapConfig("function.textEmbedding.providers.myprov.url", "https://example.invalid")
	assert.False(t, mgr.IsSensitive("function.textEmbedding.providers.myprov.url"))
}

// A collapsed identity that two different keys can produce teaches nothing, so
// that neither one's classification depends on which source was walked first.
func TestSpellingRecoveryRefusesToGuessBetweenCollidingKeys(t *testing.T) {
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile,
		[]byte("kafka.consumer.a.bc: sensitive-member\n"+
			"kafkaconsumera.bc: not-a-member\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigPrefix("kafka.consumer.")
	mgr.RegisterSensitivePrefix("kafka.consumer.")

	// Both collapse to "kafkaconsumerabc". Whichever the file walk saw last, the
	// shared identity falls back to the collapsed prefixes, which claim it.
	assert.True(t, mgr.IsSensitive("kafkaconsumerabc"))
	_, _, err := mgr.GetRegisteredConfig("kafkaconsumerabc")
	assert.ErrorIs(t, err, ErrKeySensitive)

	// And it stays refused: an update arriving after the initial load must not
	// re-teach the identity one of the two spellings.
	mgr.OnEvent(&Event{
		EventSource: "FileSource",
		EventType:   UpdateType,
		Key:         "kafkaconsumera.bc",
		Value:       "not-a-member",
	})
	assert.True(t, mgr.IsSensitive("kafkaconsumerabc"),
		"a later event re-taught a collided identity")
}

// Both spellings of a collided identity are source-backed, but neither one's
// segmentation may exempt the value shared by that identity. Otherwise the
// non-sensitive leaf from one spelling can publish the secret supplied through
// the other.
func TestCollidingStoredSpellingsCannotClaimSuffixExemption(t *testing.T) {
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile,
		[]byte("group.foo.enable: public-looking\n"+
			"group.fooe.nable: secret-from-collapsed-identity\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigPrefix("group.")
	mgr.RegisterSensitivePrefix("group.")
	mgr.RegisterNonSensitiveSuffix("group.", "enable")

	for _, spelling := range []string{
		"group.foo.enable",
		"group.fooe.nable",
		"groupfooenable",
	} {
		assert.True(t, mgr.IsSensitive(spelling), spelling)
		_, _, err := mgr.GetRegisteredConfig(spelling)
		assert.ErrorIs(t, err, ErrKeySensitive, spelling)
	}

	projection := mgr.ProjectConfigs()
	for key, value := range projection {
		if EtcdConfigKey(key) == "groupfooenable" {
			assert.Equal(t, RedactedValue, value, key)
		}
	}
}

// A ParamGroup member set through both overlay spellings must be reported as
// the one ParamGroup.GetValue actually returns.
func TestRegisteredGroupMemberMatchesGroupValue(t *testing.T) {
	const (
		prefix = "proxy.accessLog.formatters."
		key    = prefix + "console.format"
	)
	mgr := NewManager()
	mgr.RegisterConfigPrefix(prefix)

	mgr.SetMapConfig(key, "dotted-value") // BaseTable.SaveGroup
	mgr.SetConfig(key, "stripped-value")  // BaseTable.Save

	group := mgr.GetBy(
		WithPrefix(prefix),
		RemovePrefix(prefix))
	require.Equal(t, "dotted-value", group["console.format"], "this is what the ParamGroup consumer sees")

	_, reported, err := mgr.GetRegisteredConfig(key)
	require.NoError(t, err)
	assert.Equal(t, group["console.format"], reported, "the management API must not name a different value")
}

// The empty prefix is what hookConfig.SoConfig registers, and it declares every
// key of the manager to be configuration. On a table that imports the process
// environment that must still publish nothing, because the environment guard is
// what contains the hazard rather than any restriction on the prefix.
func TestEmptyPrefixNeverPublishesTheEnvironment(t *testing.T) {
	t.Setenv("PROBESINGLE", "single-token-secret")
	t.Setenv("probe_lower_case", "lower-case-secret")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")

	// Registered before the source and after it: the guarantee must not depend
	// on which order they happen in.
	before := NewManager()
	before.RegisterConfigPrefix("")
	require.NoError(t, before.AddSource(NewEnvSource(formatKey)))

	after, _ := Init(WithEnvSource(formatKey))
	after.RegisterConfigPrefix("")

	for name, mgr := range map[string]*Manager{"prefix first": before, "source first": after} {
		require.NotEmpty(t, mgr.GetConfigs(), name)
		assert.Empty(t, mgr.ProjectConfigs(), name)
		assert.Empty(t, mgr.GetConfigsView(), name)
		for _, spelling := range []string{"PROBESINGLE", "probesingle", "probe_lower_case", "AWS_SECRET_ACCESS_KEY"} {
			_, _, err := mgr.GetRegisteredConfig(spelling)
			require.ErrorIs(t, err, ErrKeyUnregistered, name+" "+spelling)
		}
	}
}

// SetMapConfig has to store a key under the identity its readers use. Keys below
// NotFormatPrefix keep their case everywhere else — FileSource stores them that
// way and formatKey leaves them alone — so folding the case here would make
// BaseTable.SaveGroup add a second, differently-cased member beside the file's
// instead of overriding it, and lose the case the index engine needs.
func TestSetMapConfigKeepsCaseUnderNotFormatPrefix(t *testing.T) {
	const key = "knowhere.DISKANN.search_list"
	mgr := NewManager()
	mgr.RegisterConfigPrefix(NotFormatPrefix)
	mgr.SetMapConfig(key, "56")

	// The exact-key reader, which is how ParamItem.get and BaseTable.Get resolve.
	_, value, err := mgr.GetConfig(key)
	require.NoError(t, err, "stored under an identity GetConfig cannot reach")
	assert.Equal(t, "56", value)

	// The group reader, which is how ParamGroup.GetValue resolves.
	assert.Equal(t,
		map[string]string{"DISKANN.search_list": "56"},
		mgr.GetBy(WithPrefix(NotFormatPrefix), RemovePrefix(NotFormatPrefix)),
		"the suffix must keep the case the index engine needs")
}

// SetMapConfig, ResetConfig and DeleteConfig must agree on the identity a group
// overlay lives under. They previously diverged for keys below NotFormatPrefix,
// whose case one preserves and the other folds, and for keys spelled with
// slashes — so a value set through BaseTable.SaveGroup survived its own removal.
func TestGroupOverlayRemovalIsSymmetric(t *testing.T) {
	for _, key := range []string{"knowhere.Xyz", "a/b/c", "kafka.consumer.x"} {
		t.Run(key, func(t *testing.T) {
			mgr := NewManager()
			mgr.RegisterConfigPrefix("knowhere.")
			mgr.RegisterConfigPrefix("a.")
			mgr.RegisterConfigPrefix("kafka.consumer.")

			mgr.SetMapConfig(key, "value")
			require.NotEmpty(t, mgr.GetConfigs(), "the overlay was never stored")
			mgr.ResetConfig(key)
			assert.Empty(t, mgr.GetConfigs(), "reset left the overlay behind")

			mgr.SetMapConfig(key, "value")
			mgr.DeleteConfig(key)
			assert.Equal(t, TombValue, mgr.GetConfigs()[mapConfigKey(key)],
				"the historical raw API must expose the tombstone")
			assert.Empty(t, mgr.ProjectConfigs(),
				"a safe projection must not publish a tombstone as a real value")
		})
	}
}

// NonSensitiveSuffixes is the only new redaction-policy mechanism here, so pin
// every edge: the exemption applies at the depth the group defines, it survives
// a prefix whose own name matches a secret pattern, and it does not rescue a
// leaf that was never exempted.
func TestNonSensitiveSuffixExemption(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterConfigPrefix("credential.")
	mgr.RegisterSensitivePrefix("credential.")
	mgr.RegisterNonSensitiveSuffix("credential.", "enable")
	mgr.RegisterNonSensitiveSuffix("credential.", "url")

	for key, want := range map[string]string{
		// Exempted, even though the key collapses to "credentialaksk1enable",
		// which contains the "credential" pattern: an explicit declaration wins
		// over the name-shape guess. This is the case the prefixExempted branch
		// of isSensitiveResolved exists for.
		"credential.aksk1.enable": "visible",
		"credential.enable":       "visible",
		"credential.aksk1.url":    "visible",
		// Not exempted.
		"credential.aksk1.secret_access_key": RedactedValue,
		"credential.aksk1.apikey":            RedactedValue,
		// Exempted leaf name, but deeper than the group defines: a sensitive
		// group must not be escapable by ending an arbitrary subtree in "url".
		"credential.aksk1.inner.url": RedactedValue,
	} {
		mgr.SetMapConfig(key, "visible")
		assert.Equal(t, want, mgr.ProjectConfigs()[key], key)
	}

	// The exemption is scoped to the prefix that declared it.
	mgr.RegisterConfigPrefix("other.")
	mgr.RegisterSensitivePrefix("other.")
	mgr.SetMapConfig("other.aksk1.enable", "visible")
	assert.Equal(t, RedactedValue, mgr.ProjectConfigs()["other.aksk1.enable"])
}

// Keys below NotFormatPrefix keep their case, so every name-based rule has to
// fold it explicitly or the same key classifies two ways depending on spelling.
func TestNameBasedRulesIgnoreCaseUnderNotFormatPrefix(t *testing.T) {
	patterns := NewManager()
	patterns.RegisterConfigPrefix(NotFormatPrefix)
	for _, key := range []string{"knowhere.apiKey", "knowhere.apikey", "knowhere.DISKANN.authToken"} {
		patterns.SetConfig(key, "secret")
		assert.True(t, patterns.IsSensitive(key), key)
		assert.Equal(t, RedactedValue, patterns.ProjectConfigs()[key], key)
	}

	exempt := NewManager()
	exempt.RegisterConfigPrefix(NotFormatPrefix)
	exempt.RegisterSensitivePrefix(NotFormatPrefix)
	exempt.RegisterNonSensitiveSuffix(NotFormatPrefix, "enable")
	for _, key := range []string{"knowhere.Enable", "knowhere.enable"} {
		exempt.SetConfig(key, "visible")
		assert.False(t, exempt.IsSensitive(key), key)
	}
	assert.True(t, exempt.IsSensitive("knowhere.Secret"))
}

// Whatever the prefix, a variable whose raw name is ALREADY in canonical form —
// PATH, HOSTNAME, http_proxy — must not be mistaken for configuration. EnvSource
// stores the raw name as a key, so a check that only asked whether the dotted
// spelling exists would take that entry as proof and publish the environment.
func TestEnvironmentNamesInCanonicalFormAreNotConfiguration(t *testing.T) {
	t.Setenv("PROBESINGLE", "single-token-secret")
	t.Setenv("probe_lower_case", "lower-case-secret")
	t.Setenv("PROBE_MULTI_TOKEN", "multi-token-secret")

	mgr, _ := Init(WithEnvSource(formatKey))
	mgr.RegisterConfigPrefix("probe.")
	mgr.RegisterConfigPrefix("probesingle.")

	require.NotEmpty(t, mgr.GetConfigs(), "the environment was never imported, the test proves nothing")
	assert.Empty(t, mgr.ProjectConfigs())
	assert.Empty(t, mgr.GetConfigsView())
	for _, spelling := range []string{"PROBESINGLE", "probesingle", "probe_lower_case", "PROBE_MULTI_TOKEN", "probe.lower.case"} {
		_, _, err := mgr.GetRegisteredConfig(spelling)
		require.ErrorIs(t, err, ErrKeyUnregistered, spelling)
	}
}

// An operator overriding a declared ParamGroup member through the environment
// must not make it disappear: the value is live, so hiding it would leave a
// setting that is in force, unreadable, and unalterable.
func TestRegisteredGroupMemberSeesEnvironmentOverride(t *testing.T) {
	const key = "proxy.accessLog.formatters.base.format"
	t.Setenv("PROXY_ACCESSLOG_FORMATTERS_BASE_FORMAT", "from-env")
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile, []byte(key+": from-yaml\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}), WithEnvSource(formatKey))
	mgr.RegisterConfigPrefix("proxy.accessLog.formatters.")

	source, value, err := mgr.GetRegisteredConfig(key)
	require.NoError(t, err)
	assert.Equal(t, "from-env", value)
	assert.Equal(t, environmentSourceName, source)
	assert.Equal(t, "from-env", mgr.ProjectConfigs()[lowerKey(key)])
	assert.Equal(t, map[string]string{"base.format": "from-env"},
		mgr.ProjectBy(WithPrefix("proxy.accessLog.formatters."), RemovePrefix("proxy.accessLog.formatters.")))
}

func TestEnvironmentSecretsAreRedacted(t *testing.T) {
	sentinelValue := strings.Repeat("config-view-sentinel-", 2)
	t.Setenv("MILVUS_CONF_SERVICE_TOKEN", sentinelValue)
	t.Setenv("AWS_SESSION_TOKEN", sentinelValue)
	t.Setenv("OPENAI_API_KEY", sentinelValue)
	t.Setenv("DATABASE_URL", sentinelValue)
	// The impersonation attempt: an unrelated variable whose formatted alias
	// lands in a registered group's separator-free namespace. Spelling it with
	// dots is what would make it match the prefix.
	t.Setenv("PROXY_ACCESSLOG_FORMATTERS_DATABASE_URL", sentinelValue)
	t.Setenv("PUBLIC_KEY", "scalar-visible")

	mgr, _ := Init(WithEnvSource(formatKey))
	mgr.RegisterConfigPrefix("proxy.accessLog.formatters.")
	mgr.RegisterConfigKey("public.key")

	canonical, kind := mgr.ResolveRegisteredConfigKey("proxy/accessLog/formatters/DATABASE_URL")
	assert.Equal(t, "proxy.accesslog.formatters.database_url", canonical)
	assert.Equal(t, RegisteredConfigUnknown, kind,
		"a prefix match on a re-spelled environment variable is not a group member")
	assert.False(t, isRegistered(mgr, "PROXY_ACCESSLOG_FORMATTERS_DATABASE_URL"))
	assert.False(t, isRegistered(mgr, formatKey("PROXY_ACCESSLOG_FORMATTERS_DATABASE_URL")))
	assert.False(t, isRegistered(mgr, "proxy.accessLog.formatters.DATABASE_URL"))

	_, resolved, err := mgr.GetConfig("proxy.accessLog.formatters.DATABASE_URL")
	require.NoError(t, err)
	assert.Equal(t, sentinelValue, resolved, "the legacy internal lookup still resolves the ambiguous alias")
	_, _, err = mgr.GetRegisteredConfig("proxy.accessLog.formatters.DATABASE_URL")
	require.ErrorIs(t, err, ErrKeyUnregistered)

	_, scalarValue, err := mgr.GetRegisteredConfig("public.key")
	require.NoError(t, err)
	assert.Equal(t, "scalar-visible", scalarValue, "explicit ParamItems may still use environment overrides")
	for key, value := range mgr.GetConfigsView() {
		assert.NotContains(t, value, sentinelValue, key)
	}

	foundRaw := false
	for _, value := range mgr.GetConfigs() {
		if strings.Contains(value, sentinelValue) {
			foundRaw = true
			break
		}
	}
	assert.True(t, foundRaw)
}

func TestRegisteredConfigKeyResolutionPreservesKnowhereSuffixCase(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterConfigPrefix(NotFormatPrefix)
	// Keys below NotFormatPrefix keep their case in every identity, which is how
	// FileSource stores them too.
	mgr.SetConfig("knowhere.DISKANN.build.search_list", "100")

	canonical, kind := mgr.ResolveRegisteredConfigKey("knowhere.DISKANN/build/search_list")
	assert.Equal(t, "knowhere.DISKANN.build.search_list", canonical)
	assert.Equal(t, RegisteredConfigGroup, kind)

	_, kind = mgr.ResolveRegisteredConfigKey("knowhere.")
	assert.Equal(t, RegisteredConfigUnknown, kind, "a ParamGroup prefix without a suffix is not a concrete config key")
}

func TestRegisteredMetadataOverridesSecretNameFallback(t *testing.T) {
	mgr := NewManager()
	for _, key := range []string{
		"proxy.minPasswordLength",
		"proxy.maxPasswordLength",
		"dataCoord.compaction.storageVersion.rateLimitTokens",
	} {
		mgr.RegisterConfigKey(key)
		mgr.RegisterNonSensitiveKey(key)
		mgr.SetConfig(key, "visible")
		assert.False(t, mgr.IsSensitive(key), key)
		assert.Equal(t, "visible", mgr.ProjectConfigs()[formatKey(key)], key)
	}
}

func TestFileConfigProjectionRedactsByDefault(t *testing.T) {
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile, []byte("public.key: visible\nopaque.key: opaque-secret\nunknown.key: unknown-secret\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigKey("public.key")
	mgr.RegisterConfigKey("opaque.key")
	mgr.RegisterSensitiveKey("opaque.key")

	safe := mgr.ProjectFileConfigs()
	assert.Equal(t, "visible", safe["public.key"])
	assert.Equal(t, RedactedValue, safe["opaque.key"])
	assert.NotContains(t, safe, "unknown.key")

	raw := mgr.FileConfigs()
	assert.Equal(t, "opaque-secret", raw["opaque.key"])
	assert.Equal(t, "unknown-secret", raw["unknown.key"])
}

func TestDeadlock(t *testing.T) {
	mgr, _ := Init()

	// test concurrent lock and recursive rlock
	wg, _ := errgroup.WithContext(context.Background())
	wg.Go(func() error {
		for i := 0; i < 100; i++ {
			mgr.GetBy(WithPrefix("rootcoord."))
		}
		return nil
	})

	wg.Go(func() error {
		for i := 0; i < 100; i++ {
			mgr.SetConfig("rootcoord.xxx", "111")
		}
		return nil
	})

	wg.Wait()
}

func TestCachedConfig(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = t.TempDir()
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()

	dir := t.TempDir()
	yamlFile := path.Join(dir, "milvus.yaml")
	os.WriteFile(yamlFile, []byte("a.b: aaa"), 0o600)
	mgr, _ := Init(WithEnvSource(formatKey),
		WithFilesSource(&FileInfo{
			Files:           []string{yamlFile},
			RefreshInterval: 10 * time.Millisecond,
		}),
		WithEtcdSource(&EtcdInfo{
			Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
			KeyPrefix:       "test",
			RefreshInterval: 10 * time.Millisecond,
		}))
	// test get cached value from file
	{
		time.Sleep(time.Second)
		_, exist := mgr.GetCachedValue("a.b")
		assert.False(t, exist)
		ok := mgr.CASCachedValue("a.b", "aaa", "aaa")
		require.True(t, ok)
		val, exist := mgr.GetCachedValue("a.b")
		require.True(t, exist)
		assert.Equal(t, "aaa", val.(string))

		// after refresh, the cached value should be reset
		os.WriteFile(yamlFile, []byte("a.b: xxx"), 0o600)
		assert.Eventually(t, func() bool {
			// make sure the config is refreshed
			_, value, err := mgr.GetConfig("a.b")
			if err != nil || value != "xxx" {
				return false
			}

			// make sure the cached value is evicted
			_, exist := mgr.GetCachedValue("a.b")
			return !exist
		}, time.Second*5, 500*time.Millisecond)
	}
	client := v3client.New(e.Server)
	{
		_, exist := mgr.GetCachedValue("c.d")
		assert.False(t, exist)
		ok := mgr.CASCachedValue("cd", "", "xxx")
		require.True(t, ok)
		_, exist = mgr.GetCachedValue("cd")
		assert.True(t, exist)

		// after refresh, the cached value should be reset
		client.Put(t.Context(), "test/config/c/d", "www")
		assert.Eventually(t, func() bool {
			// make sure the config is refreshed
			_, value, err := mgr.GetConfig("cd")
			if err != nil || value != "www" {
				return false
			}

			// make sure the cached value is evicted
			_, exist := mgr.GetCachedValue("cd")
			return !exist
		}, time.Second*5, 500*time.Millisecond)
	}
}

type ErrSource struct{}

func (e ErrSource) Close() {
}

func (e ErrSource) GetConfigurationByKey(string) (string, error) {
	return "", errors.New("error")
}

// GetConfigurations implements Source
func (ErrSource) GetConfigurations() (map[string]string, error) {
	return nil, errors.New("error")
}

// GetPriority implements Source
func (ErrSource) GetPriority() int {
	return 2
}

func (ErrSource) SetManager(m ConfigManager) {
}

// GetSourceName implements Source
func (ErrSource) GetSourceName() string {
	return "ErrSource"
}

func (e ErrSource) SetEventHandler(eh EventHandler) {
}

func (e ErrSource) UpdateOptions(opt Options) {
}

func TestAlterConfigsInEtcd(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = "/tmp/milvus/test_alter_configs"
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()
	defer os.RemoveAll(cfg.Dir)

	mgr, _ := Init(WithEtcdSource(&EtcdInfo{
		Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
		KeyPrefix:       "test",
		RefreshInterval: 10 * time.Millisecond,
	}))

	etcdSource, ok := mgr.GetEtcdSource()
	assert.True(t, ok, "should get etcd source")

	t.Run("update multiple configs atomically", func(t *testing.T) {
		configs := map[string]string{
			"config.key1": "value1",
			"config.key2": "value2",
			"config.key3": "value3",
		}

		err := mgr.AlterConfigsInEtcd(etcdSource, configs, nil)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			for key, expectedValue := range configs {
				_, actualValue, err := mgr.GetConfig(key)
				if err != nil || actualValue != expectedValue {
					return false
				}
			}
			return true
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("update single config via helper", func(t *testing.T) {
		err := mgr.UpdateConfigInEtcd(etcdSource, "single.key", "single.value")
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("single.key")
			return err == nil && value == "single.value"
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("empty updates and deletes should fail", func(t *testing.T) {
		err := mgr.AlterConfigsInEtcd(etcdSource, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no configs to alter")
	})

	t.Run("nil etcd source should fail", func(t *testing.T) {
		err := mgr.AlterConfigsInEtcd(nil, map[string]string{"key": "value"}, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "etcd client is not available")
	})

	t.Run("overwrite existing config", func(t *testing.T) {
		err := mgr.UpdateConfigInEtcd(etcdSource, "overwrite.key", "initial.value")
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("overwrite.key")
			return err == nil && value == "initial.value"
		}, time.Second*5, 100*time.Millisecond)

		err = mgr.UpdateConfigInEtcd(etcdSource, "overwrite.key", "updated.value")
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("overwrite.key")
			return err == nil && value == "updated.value"
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("batch update with key normalization", func(t *testing.T) {
		configs := map[string]string{
			"config/key/with/slashes": "value1",
			"config.key.with.dots":    "value2",
		}

		err := mgr.AlterConfigsInEtcd(etcdSource, configs, nil)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, value1, err1 := mgr.GetConfig("config.key.with.slashes")
			_, value2, err2 := mgr.GetConfig("config.key.with.dots")
			return err1 == nil && value1 == "value1" && err2 == nil && value2 == "value2"
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("delete configs from etcd", func(t *testing.T) {
		// First write some configs
		err := mgr.AlterConfigsInEtcd(etcdSource, map[string]string{
			"delete.key1": "value1",
			"delete.key2": "value2",
		}, nil)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, v1, err1 := mgr.GetConfig("delete.key1")
			_, v2, err2 := mgr.GetConfig("delete.key2")
			return err1 == nil && v1 == "value1" && err2 == nil && v2 == "value2"
		}, time.Second*5, 100*time.Millisecond)

		// Delete them
		err = mgr.AlterConfigsInEtcd(etcdSource, nil, []string{"delete.key1", "delete.key2"})
		assert.NoError(t, err)
	})

	t.Run("mixed update and delete in one transaction", func(t *testing.T) {
		// Setup: write two configs
		err := mgr.AlterConfigsInEtcd(etcdSource, map[string]string{
			"mixed.keep":   "old_value",
			"mixed.remove": "to_be_deleted",
		}, nil)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, v1, err1 := mgr.GetConfig("mixed.keep")
			_, v2, err2 := mgr.GetConfig("mixed.remove")
			return err1 == nil && v1 == "old_value" && err2 == nil && v2 == "to_be_deleted"
		}, time.Second*5, 100*time.Millisecond)

		// Atomically: update one, delete the other
		err = mgr.AlterConfigsInEtcd(etcdSource,
			map[string]string{"mixed.keep": "new_value"},
			[]string{"mixed.remove"},
		)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, v, err := mgr.GetConfig("mixed.keep")
			return err == nil && v == "new_value"
		}, time.Second*5, 100*time.Millisecond)
	})
}

func TestProcessImmutableConfigsRenderer(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = "/tmp/milvus/test_process_immutable_renderer"
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()
	defer os.RemoveAll(cfg.Dir)

	mgr, _ := Init(WithEtcdSource(&EtcdInfo{
		Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
		KeyPrefix:       "test-immutable-renderer",
		RefreshInterval: 10 * time.Millisecond,
	}))

	etcdSource, ok := mgr.GetEtcdSource()
	assert.True(t, ok, "should get etcd source")

	t.Run("renderer converts placeholder value before first persist", func(t *testing.T) {
		mgr.SetConfig("render.mq.type", "default")
		mgr.ImmutableUpdate("render.mq.type")

		err := mgr.ProcessImmutableConfigs(map[string]func(string) string{
			"render.mq.type": func(raw string) string {
				assert.Equal(t, "default", raw)
				return "woodpecker"
			},
		})
		assert.NoError(t, err)

		v, err := etcdSource.GetConfigurationByKey(formatKey("render.mq.type"))
		assert.NoError(t, err)
		assert.Equal(t, "woodpecker", v)
	})

	t.Run("existing etcd value is not overwritten and renderer is not applied", func(t *testing.T) {
		err := mgr.UpdateConfigInEtcd(etcdSource, "render.existing", "pulsar")
		assert.NoError(t, err)

		mgr.SetConfig("render.existing", "default")
		mgr.ImmutableUpdate("render.existing")

		err = mgr.ProcessImmutableConfigs(map[string]func(string) string{
			"render.existing": func(raw string) string {
				t.Errorf("renderer must not run for a key already persisted in etcd")
				return "must-not-be-used"
			},
		})
		assert.NoError(t, err)

		v, err := etcdSource.GetConfigurationByKey(formatKey("render.existing"))
		assert.NoError(t, err)
		assert.Equal(t, "pulsar", v)
	})

	t.Run("key without renderer persists raw value unchanged", func(t *testing.T) {
		mgr.SetConfig("render.plain", "rawvalue")
		mgr.ImmutableUpdate("render.plain")

		err := mgr.ProcessImmutableConfigs(nil)
		assert.NoError(t, err)

		v, err := etcdSource.GetConfigurationByKey(formatKey("render.plain"))
		assert.NoError(t, err)
		assert.Equal(t, "rawvalue", v)
	})
}

func TestProcessImmutableConfigsRendererKeyAbsentFromSources(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = "/tmp/milvus/test_process_immutable_renderer_absent"
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()
	defer os.RemoveAll(cfg.Dir)

	mgr, _ := Init(WithEtcdSource(&EtcdInfo{
		Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
		KeyPrefix:       "test-immutable-renderer-absent",
		RefreshInterval: 10 * time.Millisecond,
	}))

	etcdSource, ok := mgr.GetEtcdSource()
	assert.True(t, ok, "should get etcd source")

	// The key exists in no source at all: a registered renderer must still be able
	// to produce the value to pin into etcd (raw is passed as empty string).
	mgr.ImmutableUpdate("render.absent")
	err = mgr.ProcessImmutableConfigs(map[string]func(string) string{
		"render.absent": func(raw string) string {
			assert.Equal(t, "", raw)
			return "woodpecker"
		},
	})
	assert.NoError(t, err)

	v, err := etcdSource.GetConfigurationByKey(formatKey("render.absent"))
	assert.NoError(t, err)
	assert.Equal(t, "woodpecker", v)
}
