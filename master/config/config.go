// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/czs007/suvlim/util/grpcutil"
	//"google.golang.org/grpc"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/czs007/suvlim/errors"
	"github.com/czs007/suvlim/util/typeutil"
	"github.com/pingcap/log"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config is the pd server configuration.
type Config struct {
	flagSet *flag.FlagSet
	Version bool `json:"-"`
	ConfigCheck bool `json:"-"`

	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`


	Name    string `toml:"name" json:"name"`
	DataDir string     `toml:"data-dir" json:"data-dir"`
	EnableGRPCGateway bool   `json:"enable-grpc-gateway"`

	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`
	InitialClusterToken string `toml:"initial-cluster-token" json:"initial-cluster-token"`


	LeaderLease int64 `toml:"lease" json:"lease"`
	Log     log.Config `toml:"log" json:"log"`
	LogFileDeprecated  string `toml:"log-file" json:"log-file,omitempty"`
	LogLevelDeprecated string `toml:"log-level" json:"log-level,omitempty"`

	PDServerCfg PDServerConfig `toml:"pd-server" json:"pd-server"`

	TickInterval typeutil.Duration `toml:"tick-interval"`
	ElectionInterval typeutil.Duration `toml:"election-interval"`
	DisableStrictReconfigCheck bool

	// TsoSaveInterval is the interval to save timestamp.
	TsoSaveInterval typeutil.Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	PreVote bool `toml:"enable-prevote"`
	Security grpcutil.SecurityConfig `toml:"security" json:"security"`

	configFile string

	// For all warnings during parsing.
	WarningMsgs []string

	HeartbeatStreamBindInterval typeutil.Duration

	logger   *zap.Logger
	logProps *log.ZapProperties

	//ServiceRegister func(*grpc.Server) `json:"-"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("pd", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.Version, "V", false, "print version information and exit")
	fs.BoolVar(&cfg.Version, "version", false, "print version information and exit")
	fs.StringVar(&cfg.configFile, "config", "", "config file")
	fs.BoolVar(&cfg.ConfigCheck, "config-check", false, "check config file validity and exit")

	fs.StringVar(&cfg.Name, "name", "", "human-readable name for this pd member")

	fs.StringVar(&cfg.DataDir, "data-dir", "", "path to the data directory (default 'default.${name}')")
	fs.StringVar(&cfg.ClientUrls, "client-urls", defaultClientUrls, "url for client traffic")
	fs.StringVar(&cfg.AdvertiseClientUrls, "advertise-client-urls", "", "advertise url for client traffic (default '${client-urls}')")
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "url for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", "", "advertise url for peer traffic (default '${peer-urls}')")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")

	fs.StringVar(&cfg.Log.Level, "L", "", "log level: debug, info, warn, error, fatal (default 'info')")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")

	return cfg
}


const (
	defaultLeaderLease             = int64(3)
	defaultName                = "pd"
	defaultClientUrls          = "http://127.0.0.1:2379"
	defaultPeerUrls            = "http://127.0.0.1:2380"
	defaultInitialClusterState = embed.ClusterStateFlagNew
	defaultInitialClusterToken = "pd-cluster"

	// etcd use 100ms for heartbeat and 1s for election timeout.
	// We can enlarge both a little to reduce the network aggression.
	// now embed etcd use TickMs for heartbeat, we will update
	// after embed etcd decouples tick and heartbeat.
	defaultTickInterval = 500 * time.Millisecond
	// embed etcd has a check that `5 * tick > election`
	defaultElectionInterval = 3000 * time.Millisecond

	defaultHeartbeatStreamRebindInterval = time.Minute

	defaultMaxResetTSGap    = 24 * time.Hour
	defaultEnableGRPCGateway   = true
	defaultDisableErrorVerbose = true
)

func init() {

}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustFloat64(v *float64, defValue float64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustDuration(v *typeutil.Duration, defValue time.Duration) {
	if v.Duration == 0 {
		v.Duration = defValue
	}
}

func adjustPath(p *string) {
	absPath, err := filepath.Abs(*p)
	if err == nil {
		*p = absPath
	}
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	// Load config file if specified.
	var meta *toml.MetaData
	if c.configFile != "" {
		meta, err = c.configFromFile(c.configFile)
		if err != nil {
			return err
		}

		// Backward compatibility for toml config
		if c.LogFileDeprecated != "" {
			msg := fmt.Sprintf("log-file in %s is deprecated, use [log.file] instead", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
			if c.Log.File.Filename == "" {
				c.Log.File.Filename = c.LogFileDeprecated
			}
		}
		if c.LogLevelDeprecated != "" {
			msg := fmt.Sprintf("log-level in %s is deprecated, use [log] instead", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
			if c.Log.Level == "" {
				c.Log.Level = c.LogLevelDeprecated
			}
		}
		if meta.IsDefined("schedule", "disable-raft-learner") {
			msg := fmt.Sprintf("disable-raft-learner in %s is deprecated", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
		}
		if meta.IsDefined("dashboard", "disable-telemetry") {
			msg := fmt.Sprintf("disable-telemetry in %s is deprecated, use enable-telemetry instead", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	err = c.Adjust(meta)
	return err
}

// Validate is used to validate if some configurations are right.
func (c *Config) Validate() error {
	dataDir, err := filepath.Abs(c.DataDir)
	if err != nil {
		return errors.WithStack(err)
	}
	logFile, err := filepath.Abs(c.Log.File.Filename)
	if err != nil {
		return errors.WithStack(err)
	}
	rel, err := filepath.Rel(dataDir, filepath.Dir(logFile))
	if err != nil {
		return errors.WithStack(err)
	}
	if !strings.HasPrefix(rel, "..") {
		return errors.New("log directory shouldn't be the subdirectory of data directory")
	}

	return nil
}

// Utility to test if a configuration is defined.
type configMetaData struct {
	meta *toml.MetaData
	path []string
}

func newConfigMetadata(meta *toml.MetaData) *configMetaData {
	return &configMetaData{meta: meta}
}

func (m *configMetaData) IsDefined(key string) bool {
	if m.meta == nil {
		return false
	}
	keys := append([]string(nil), m.path...)
	keys = append(keys, key)
	return m.meta.IsDefined(keys...)
}

func (m *configMetaData) Child(path ...string) *configMetaData {
	newPath := append([]string(nil), m.path...)
	newPath = append(newPath, path...)
	return &configMetaData{
		meta: m.meta,
		path: newPath,
	}
}

func (m *configMetaData) CheckUndecoded() error {
	if m.meta == nil {
		return nil
	}
	undecoded := m.meta.Undecoded()
	if len(undecoded) == 0 {
		return nil
	}
	errInfo := "Config contains undefined item: "
	for _, key := range undecoded {
		errInfo += key.String() + ", "
	}
	return errors.New(errInfo[:len(errInfo)-2])
}

// Adjust is used to adjust the PD configurations.
func (c *Config) Adjust(meta *toml.MetaData) error {
	configMetaData := newConfigMetadata(meta)
	if err := configMetaData.CheckUndecoded(); err != nil {
		c.WarningMsgs = append(c.WarningMsgs, err.Error())
	}

	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		adjustString(&c.Name, fmt.Sprintf("%s-%s", defaultName, hostname))
	}
	adjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))
	adjustPath(&c.DataDir)

	if err := c.Validate(); err != nil {
		return err
	}

	adjustString(&c.ClientUrls, defaultClientUrls)
	adjustString(&c.AdvertiseClientUrls, c.ClientUrls)
	adjustString(&c.PeerUrls, defaultPeerUrls)
	adjustString(&c.AdvertisePeerUrls, c.PeerUrls)

	if len(c.InitialCluster) == 0 {
		// The advertise peer urls may be http://127.0.0.1:2380,http://127.0.0.1:2381
		// so the initial cluster is pd=http://127.0.0.1:2380,pd=http://127.0.0.1:2381
		items := strings.Split(c.AdvertisePeerUrls, ",")

		sep := ""
		for _, item := range items {
			c.InitialCluster += fmt.Sprintf("%s%s=%s", sep, c.Name, item)
			sep = ","
		}
	}

	adjustString(&c.InitialClusterState, defaultInitialClusterState)
	adjustString(&c.InitialClusterToken, defaultInitialClusterToken)

	adjustInt64(&c.LeaderLease, defaultLeaderLease)

	adjustDuration(&c.TsoSaveInterval, time.Duration(defaultLeaderLease)*time.Second)

	adjustDuration(&c.TickInterval, defaultTickInterval)
	adjustDuration(&c.ElectionInterval, defaultElectionInterval)


	if err := c.PDServerCfg.adjust(configMetaData.Child("pd-server")); err != nil {
		return err
	}

	c.adjustLog(configMetaData.Child("log"))
	adjustDuration(&c.HeartbeatStreamBindInterval, defaultHeartbeatStreamRebindInterval)

	if !configMetaData.IsDefined("enable-prevote") {
		c.PreVote = true
	}
	if !configMetaData.IsDefined("enable-grpc-gateway") {
		c.EnableGRPCGateway = defaultEnableGRPCGateway
	}

	return nil
}

func (c *Config) adjustLog(meta *configMetaData) {
	if !meta.IsDefined("disable-error-verbose") {
		c.Log.DisableErrorVerbose = defaultDisableErrorVerbose
	}
}

// Clone returns a cloned configuration.
func (c *Config) Clone() *Config {
	cfg := &Config{}
	*cfg = *c
	return cfg
}

func (c *Config) String() string {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "<nil>"
	}
	return string(data)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) (*toml.MetaData, error) {
	meta, err := toml.DecodeFile(path, c)
	return &meta, errors.WithStack(err)
}


// PDServerConfig is the configuration for pd server.
type PDServerConfig struct {
	// MaxResetTSGap is the max gap to reset the tso.
	MaxResetTSGap typeutil.Duration `toml:"max-gap-reset-ts" json:"max-gap-reset-ts"`
}

func (c *PDServerConfig) adjust(meta *configMetaData) error {
	adjustDuration(&c.MaxResetTSGap, defaultMaxResetTSGap)
	return nil
}

// Clone returns a cloned PD server config.
func (c *PDServerConfig) Clone() *PDServerConfig {
	return &PDServerConfig{
		MaxResetTSGap:    c.MaxResetTSGap,
	}
}


// ParseUrls parse a string into multiple urls.
// Export for api.
func ParseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		urls = append(urls, *u)
	}

	return urls, nil
}

// SetupLogger setup the logger.
func (c *Config) SetupLogger() error {
	lg, p, err := log.InitLogger(&c.Log, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return err
	}
	c.logger = lg
	c.logProps = p
	return nil
}

// GetZapLogger gets the created zap logger.
func (c *Config) GetZapLogger() *zap.Logger {
	return c.logger
}

// GetZapLogProperties gets properties of the zap logger.
func (c *Config) GetZapLogProperties() *log.ZapProperties {
	return c.logProps
}

// GetConfigFile gets the config file.
func (c *Config) GetConfigFile() string {
	return c.configFile
}

// RewriteFile rewrites the config file after updating the config.
func (c *Config) RewriteFile(new *Config) error {
	filePath := c.GetConfigFile()
	if filePath == "" {
		return nil
	}
	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(*new); err != nil {
		return err
	}
	dir := filepath.Dir(filePath)
	tmpfile := filepath.Join(dir, "tmp_pd.toml")

	f, err := os.Create(tmpfile)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(buf.Bytes()); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(tmpfile, filePath)
}

func (c *Config) GenEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.InitialClusterToken = c.InitialClusterToken
	cfg.EnablePprof = true
	cfg.PreVote = c.PreVote
	cfg.StrictReconfigCheck = !c.DisableStrictReconfigCheck
	cfg.TickMs = uint(c.TickInterval.Duration / time.Millisecond)
	cfg.ElectionMs = uint(c.ElectionInterval.Duration / time.Millisecond)

	allowedCN, serr := c.Security.GetOneAllowedCN()
	if serr != nil {
		return nil, serr
	}
	cfg.ClientTLSInfo.ClientCertAuth = len(c.Security.CAPath) != 0
	cfg.ClientTLSInfo.TrustedCAFile = c.Security.CAPath
	cfg.ClientTLSInfo.CertFile = c.Security.CertPath
	cfg.ClientTLSInfo.KeyFile = c.Security.KeyPath
	// Client no need to set the CN. (cfg.ClientTLSInfo.AllowedCN = allowedCN)
	cfg.PeerTLSInfo.ClientCertAuth = len(c.Security.CAPath) != 0
	cfg.PeerTLSInfo.TrustedCAFile = c.Security.CAPath
	cfg.PeerTLSInfo.CertFile = c.Security.CertPath
	cfg.PeerTLSInfo.KeyFile = c.Security.KeyPath
	cfg.PeerTLSInfo.AllowedCN = allowedCN
	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(c.logger, c.logger.Core(), c.logProps.Syncer)
	cfg.EnableGRPCGateway = c.EnableGRPCGateway
	cfg.EnableV2 = true
	cfg.Logger = "zap"
	var err error

	cfg.LPUrls, err = ParseUrls(c.PeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.APUrls, err = ParseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.LCUrls, err = ParseUrls(c.ClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.ACUrls, err = ParseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}
