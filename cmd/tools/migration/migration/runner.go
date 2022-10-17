package migration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/cmd/tools/migration/versions"

	"github.com/blang/semver/v4"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"

	"github.com/milvus-io/milvus/cmd/tools/migration/console"

	"github.com/milvus-io/milvus/cmd/tools/migration/backend"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

type Runner struct {
	ctx            context.Context
	cancel         context.CancelFunc
	cfg            *configs.Config
	initOnce       sync.Once
	session        *sessionutil.Session
	address        string
	etcdCli        *clientv3.Client
	wg             sync.WaitGroup
	backupFinished atomic.Bool
}

func NewRunner(ctx context.Context, cfg *configs.Config) *Runner {
	ctx1, cancel := context.WithCancel(ctx)
	runner := &Runner{
		ctx:            ctx1,
		cancel:         cancel,
		cfg:            cfg,
		backupFinished: *atomic.NewBool(false),
	}
	runner.initOnce.Do(runner.init)
	return runner
}

func (r *Runner) watchByPrefix(prefix string) {
	defer r.wg.Done()
	_, revision, err := r.session.GetSessions(prefix)
	console.AbnormalExitIf(err, r.backupFinished.Load())
	eventCh := r.session.WatchServices(prefix, revision, nil)
	for {
		select {
		case <-r.ctx.Done():
			return
		case event := <-eventCh:
			msg := fmt.Sprintf("session up/down, exit migration, event type: %s, session: %s", event.EventType.String(), event.Session.String())
			console.AbnormalExit(r.backupFinished.Load(), msg)
		}
	}
}

func (r *Runner) WatchSessions() {
	for _, role := range Roles {
		r.wg.Add(1)
		go r.watchByPrefix(role)
	}
}

func (r *Runner) initEtcdCli() {
	cli, err := etcd.GetEtcdClient(r.cfg.EtcdCfg)
	console.AbnormalExitIf(err, r.backupFinished.Load())
	r.etcdCli = cli
}

func (r *Runner) init() {
	r.initEtcdCli()

	r.session = sessionutil.NewSession(r.ctx, r.cfg.EtcdCfg.MetaRootPath, r.etcdCli,
		sessionutil.WithCustomConfigEnable(), sessionutil.WithSessionTTL(60), sessionutil.WithSessionRetryTimes(30))
	// address not important here.
	address := time.Now().String()
	r.address = address
	r.session.Init(Role, address, true, true)
	r.WatchSessions()
}

func (r *Runner) Validate() error {
	source, err := semver.Parse(r.cfg.SourceVersion)
	if err != nil {
		return err
	}
	target, err := semver.Parse(r.cfg.TargetVersion)
	if err != nil {
		return err
	}
	can := target.GTE(source)
	if can {
		return nil
	}
	return fmt.Errorf("higher version to lower version is not allowed, "+
		"source version: %s, target version: %s", source.String(), target.String())
}

func (r *Runner) CheckCompatible() bool {
	// TODO: more accurate.
	target, err := semver.Parse(r.cfg.TargetVersion)
	if err != nil {
		return false
	}
	return target.LT(versions.Version220)
}

func (r *Runner) checkSessionsWithPrefix(prefix string) error {
	sessions, _, err := r.session.GetSessions(prefix)
	if err != nil {
		return err
	}
	if len(sessions) > 0 {
		return fmt.Errorf("there are still sessions alive, prefix: %s, num of alive sessions: %d", prefix, len(sessions))
	}
	return nil
}

func (r *Runner) checkMySelf() error {
	sessions, _, err := r.session.GetSessions(Role)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if session.Address != r.address {
			return fmt.Errorf("other migration is running")
		}
	}
	return nil
}

func (r *Runner) CheckSessions() error {
	if err := r.checkMySelf(); err != nil {
		return err
	}
	for _, prefix := range Roles {
		if err := r.checkSessionsWithPrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) RegisterSession() error {
	r.session.Register()
	go r.session.LivenessCheck(r.ctx, func() {})
	return nil
}

func (r *Runner) Backup() error {
	source, err := backend.NewBackend(r.cfg.MilvusConfig, r.cfg.SourceVersion)
	if err != nil {
		return err
	}
	if err := source.BackupV2(r.cfg.BackupFilePath); err != nil {
		return err
	}
	r.backupFinished.Store(true)
	return nil
}

func (r *Runner) Rollback() error {
	source, err := backend.NewBackend(r.cfg.MilvusConfig, r.cfg.SourceVersion)
	if err != nil {
		return err
	}
	target, err := backend.NewBackend(r.cfg.MilvusConfig, r.cfg.TargetVersion)
	if err != nil {
		return err
	}
	if err := source.Clean(); err != nil {
		return err
	}
	if err := target.Clean(); err != nil {
		return err
	}
	return source.Restore(r.cfg.BackupFilePath)
}

func (r *Runner) Migrate() error {
	migrator, err := NewMigrator(r.cfg.SourceVersion, r.cfg.TargetVersion)
	if err != nil {
		return err
	}
	source, err := backend.NewBackend(r.cfg.MilvusConfig, r.cfg.SourceVersion)
	if err != nil {
		return err
	}
	metas, err := source.Load()
	if err != nil {
		return err
	}
	if err := source.Clean(); err != nil {
		return err
	}
	targetMetas, err := migrator.Migrate(metas)
	if err != nil {
		return err
	}
	target, err := backend.NewBackend(r.cfg.MilvusConfig, r.cfg.TargetVersion)
	if err != nil {
		return err
	}
	return target.Save(targetMetas)
}

func (r *Runner) Stop() {
	r.session.Revoke(time.Second)
	r.cancel()
	r.wg.Wait()
}
