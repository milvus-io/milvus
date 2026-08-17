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
	raw := mgr.GetConfigsRaw()
	assert.Less(t, 0, len(raw))

	// Nothing is declared yet, so the safe projection is empty: the process
	// environment that EnvSource imported is not Milvus configuration, and
	// publishing the names of every variable in the pod is a disclosure of its
	// own even with the values masked.
	all = mgr.GetConfigs()
	assert.Equal(t, 0, len(all))

	// Declaring one key publishes that key — under both the canonical identity
	// and the environment alias EnvSource stored it as, since both resolve to
	// the same declared ParamItem — and nothing else.
	mgr.RegisterConfigKey("public.key")
	all = mgr.GetConfigs()
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

	assert.Equal(t, RedactedValue, mgr.GetConfigs()[formatKey("kafka.producer.compression")])
	_, _, err := mgr.GetRegisteredConfig("kafka.producer.compression")
	require.ErrorIs(t, err, ErrKeySensitive)

	// An explicit NonSensitive declaration still overrides the group default.
	mgr.RegisterNonSensitiveKey("kafka.producer.compression")
	assert.False(t, mgr.IsSensitive(formatKey("kafka.producer.compression")))
	assert.Equal(t, "must-not-leak", mgr.GetConfigs()[formatKey("kafka.producer.compression")])
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

	safe := mgr.GetConfigs()
	assert.Equal(t, "visible", safe[publicKey])
	assert.Equal(t, RedactedValue, safe[opaqueKey])
	assert.Equal(t, "dynamic-value", safe[dynamicKey])
	assert.Equal(t, RedactedValue, safe[dynamicSecretKey])
	assert.Equal(t, RedactedValue, safe[groupKey])
	assert.Equal(t, RedactedValue, safe[slashGroupKey])
	// A declared key is named and masked; an undeclared one is not named at all.
	assert.NotContains(t, safe, unknownKey)

	raw := mgr.GetConfigsRaw()
	assert.Equal(t, "opaque-secret", raw[opaqueKey])
	assert.Equal(t, "unknown-secret", raw[unknownKey])
	assert.Equal(t, "group-secret", raw[groupKey])
	assert.Equal(t, "dynamic-secret", raw[dynamicSecretKey])
	assert.Equal(t, "slash-group-secret", raw[slashGroupKey])

	safeView := mgr.GetConfigsView()
	assert.Contains(t, safeView[publicKey], RuntimeSource)
	// A redacted entry carries no "value[source]" annotation: the source name is
	// the only part left, and printing "*****[RuntimeSource]" would suggest the
	// mask itself is the value.
	assert.Equal(t, RedactedValue, safeView[opaqueKey])
	assert.NotContains(t, safeView, unknownKey)

	assert.Equal(t, "dynamic-value", mgr.GetBy(WithPrefix("dynamic"))[dynamicKey])
	assert.Equal(t, RedactedValue, mgr.GetBy(WithPrefix("dynamic"))[dynamicSecretKey])
	assert.Equal(t, "group-secret", mgr.GetByRaw(WithPrefix("sensitive"))[groupKey])

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

	// Nothing anywhere yet: still a legal member of a declared group.
	_, kind := mgr.ResolveRegisteredConfigKey("kafka.producer.linger.ms")
	assert.Equal(t, RegisteredConfigGroup, kind, "a member that does not exist yet must still be creatable")

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
	mgr.RegisterConfigKey("minio.address")
	// The dotted spelling is the only thing set, and nothing in the process
	// reads a ParamItem under it.
	mgr.SetMapConfig("minio.address", "stray-dotted-overlay")

	_, _, liveErr := mgr.GetConfig("minio.address")
	require.ErrorIs(t, liveErr, ErrKeyNotFound, "ParamItem.get would not find this either")
	_, _, err := mgr.GetRegisteredConfig("minio.address")
	require.ErrorIs(t, err, ErrKeyNotFound, "so the management API must not report it as the value in force")

	// Once it is set the way a ParamItem is actually read, both agree.
	mgr.SetConfig("minio.address", "the-value-milvus-uses")
	_, live, err := mgr.GetConfig("minio.address")
	require.NoError(t, err)
	_, reported, err := mgr.GetRegisteredConfig("minio.address")
	require.NoError(t, err)
	assert.Equal(t, live, reported)
}

// A write only counts if Milvus reads it back. ParamGroup.GetValue selects
// members by the dotted prefix, while a write lands under the separator-free
// identity, so a brand-new member is stored and then ignored — while overriding
// one a config file already declares works end to end.
func TestWriteTakesEffect(t *testing.T) {
	const declared = "kafka.consumer.fetch.min.bytes"
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile, []byte(declared+": 1\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigPrefix("kafka.consumer.")
	mgr.RegisterConfigKey("querynode.gracefulStopTimeout")

	assert.True(t, mgr.WriteTakesEffect("querynode.gracefulStopTimeout"), "a declared scalar is always read back")
	assert.True(t, mgr.WriteTakesEffect(declared), "the config file supplied the dotted key the group filter needs")
	assert.False(t, mgr.WriteTakesEffect("kafka.consumer.brand.new.option"))
	assert.False(t, mgr.WriteTakesEffect("nothing.declares.this"))

	// A group below NotFormatPrefix keeps its separators all the way into etcd,
	// so a brand-new member there lands inside the namespace the group filters
	// on and is readable immediately.
	mgr.RegisterConfigPrefix(NotFormatPrefix)
	const newKnowhere = "knowhere.MYINDEX.build.max_degree"
	require.Equal(t, newKnowhere, EtcdConfigKey(newKnowhere))
	assert.True(t, mgr.WriteTakesEffect(newKnowhere))
	mgr.SetConfig(newKnowhere, "64")
	assert.Equal(t, map[string]string{"MYINDEX.build.max_degree": "64"},
		mgr.GetByRaw(WithPrefix(NotFormatPrefix), RemovePrefix(NotFormatPrefix)),
		"the prediction WriteTakesEffect made must hold once the write happens")

	// And the claim the predicate rests on: an override of the declared member
	// reaches the group, an invented one does not.
	etcd := &mapSource{name: "EtcdLikeSource", configs: map[string]string{
		EtcdConfigKey(declared):                          "42",
		EtcdConfigKey("kafka.consumer.brand.new.option"): "99",
	}}
	require.NoError(t, mgr.AddSource(etcd))
	group := mgr.GetByRaw(WithPrefix("kafka.consumer."), RemovePrefix("kafka.consumer."))
	assert.Equal(t, map[string]string{"fetch.min.bytes": "42"}, group)
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

// The variables that matter are the ones whose raw name is ALREADY in canonical
// form — PATH, HOSTNAME, http_proxy. EnvSource stores the raw name as a key, so
// a check that only asked whether the dotted spelling exists would take that raw
// entry as proof of configuration and publish the whole environment. The empty
// prefix is the worst case, and the one hookConfig.SoConfig actually registers.
func TestEmptyPrefixGroupNeverPublishesTheEnvironment(t *testing.T) {
	t.Setenv("PROBESINGLE", "single-token-secret")
	t.Setenv("probe_lower_case", "lower-case-secret")
	t.Setenv("PROBE_MULTI_TOKEN", "multi-token-secret")

	mgr, _ := Init(WithEnvSource(formatKey))
	mgr.RegisterConfigPrefix("")

	require.NotEmpty(t, mgr.GetConfigsRaw(), "the environment was never imported, the test proves nothing")
	assert.Empty(t, mgr.GetConfigs())
	assert.Empty(t, mgr.GetConfigsView())
	for _, spelling := range []string{"PROBESINGLE", "probesingle", "probe_lower_case", "PROBE_MULTI_TOKEN"} {
		_, _, err := mgr.GetRegisteredConfig(spelling)
		require.ErrorIs(t, err, ErrKeyUnregistered, spelling)
	}
}

// NonSensitiveSuffixes is the only new redaction-policy mechanism in this
// change, so pin every edge of it: the exemption applies at the depth the group
// defines, it survives a prefix whose own name matches a secret pattern, and it
// does not rescue a leaf that was never exempted.
func TestNonSensitiveSuffixExemption(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterConfigPrefix("credential.")
	mgr.RegisterSensitivePrefix("credential.")
	mgr.RegisterNonSensitiveSuffix("credential.", "enable")
	mgr.RegisterNonSensitiveSuffix("credential.", "url")

	for key, want := range map[string]string{
		// Exempted, even though the key collapses to "credentialaksk1enable",
		// which contains the "credential" pattern: an explicit declaration wins
		// over the name-shape guess.
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
		assert.Equal(t, want, mgr.GetConfigs()[key], key)
	}

	// The exemption is scoped to the prefix that declared it.
	mgr.RegisterConfigPrefix("other.")
	mgr.RegisterSensitivePrefix("other.")
	mgr.SetMapConfig("other.aksk1.enable", "visible")
	assert.Equal(t, RedactedValue, mgr.GetConfigs()["other.aksk1.enable"])
}

// A key deleted at runtime must disappear from the projections, not surface as
// a key whose value is the literal tombstone marker — and it must disappear for
// a ParamGroup member too, whose overlay SetMapConfig wrote under the dotted
// identity rather than the separator-free one.
func TestTombstonedKeysAreNotProjected(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterConfigKey("public.key")
	mgr.SetConfig("public.key", "visible")
	mgr.RegisterConfigPrefix("kafka.consumer.")
	mgr.SetMapConfig("kafka.consumer.fetch.min.bytes", "12345")
	require.Equal(t, "visible", mgr.GetConfigs()[formatKey("public.key")])
	require.Equal(t, "12345", mgr.GetConfigsRaw()["kafka.consumer.fetch.min.bytes"])

	mgr.DeleteConfig("public.key")
	mgr.DeleteConfig("kafka.consumer.fetch.min.bytes")

	assert.NotContains(t, mgr.GetConfigs(), formatKey("public.key"))
	assert.NotContains(t, mgr.GetConfigsView(), formatKey("public.key"))
	assert.NotContains(t, mgr.GetBy(WithPrefix("public")), formatKey("public.key"))
	// GetByRaw is the path ParamGroup.GetValue takes, so a delete that does not
	// reach it would leave the running system using a value the management API
	// reports as gone.
	assert.Empty(t, mgr.GetByRaw(WithPrefix("kafka.consumer.")))
	assert.Empty(t, mgr.GetBy(WithPrefix("kafka.consumer.")))
	_, _, err := mgr.GetRegisteredConfig("kafka.consumer.fetch.min.bytes")
	require.ErrorIs(t, err, ErrKeyNotFound)
	for _, value := range mgr.GetConfigsView() {
		assert.NotContains(t, value, TombValue)
	}
}

// formatKey exempts knowhere.* from separator stripping; the EnvSource key
// formatter BaseTable installs does not. An environment variable whose name
// lands in that gap must not be mistaken for a member of the knowhere group.
func TestKnowherePrefixDoesNotAdmitEnvironmentVariables(t *testing.T) {
	t.Setenv("KNOWHERE.INJECTED", "must-not-be-published")
	t.Setenv("knowhere.lowercase", "must-not-be-published")

	// The formatter BaseTable installs: no NotFormatPrefix exemption.
	mgr, _ := Init(WithEnvSource(strippedKey))
	mgr.RegisterConfigPrefix(NotFormatPrefix)

	require.NotEmpty(t, mgr.GetConfigsRaw(), "the environment was never imported, the test proves nothing")
	for _, spelling := range []string{"KNOWHERE.INJECTED", "knowhere.INJECTED", "knowhere.lowercase"} {
		_, kind := mgr.ResolveRegisteredConfigKey(spelling)
		assert.Equal(t, RegisteredConfigUnknown, kind, spelling)
	}
	for key, value := range mgr.GetConfigsView() {
		assert.NotContains(t, value, "must-not-be-published", key)
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
	assert.Equal(t, "from-env", mgr.GetConfigs()[lowerKey(key)])
	assert.Equal(t, map[string]string{"base.format": "from-env"},
		mgr.GetBy(WithPrefix("proxy.accessLog.formatters."), RemovePrefix("proxy.accessLog.formatters.")))
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
	for _, value := range mgr.GetConfigsRaw() {
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
		assert.Equal(t, "visible", mgr.GetConfigs()[formatKey(key)], key)
	}
}

func TestFileConfigProjectionRedactsByDefault(t *testing.T) {
	yamlFile := path.Join(t.TempDir(), "milvus.yaml")
	require.NoError(t, os.WriteFile(yamlFile, []byte("public.key: visible\nopaque.key: opaque-secret\nunknown.key: unknown-secret\n"), 0o600))

	mgr, _ := Init(WithFilesSource(&FileInfo{Files: []string{yamlFile}}))
	mgr.RegisterConfigKey("public.key")
	mgr.RegisterConfigKey("opaque.key")
	mgr.RegisterSensitiveKey("opaque.key")

	safe := mgr.FileConfigs()
	assert.Equal(t, "visible", safe["public.key"])
	assert.Equal(t, RedactedValue, safe["opaque.key"])
	assert.NotContains(t, safe, "unknown.key")

	raw := mgr.GetConfigsRaw()
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
