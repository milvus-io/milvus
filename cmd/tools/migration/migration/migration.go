package migration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/util/etcd"

	"github.com/blang/semver/v4"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

type Migration interface {
	// Validate if migration can be executed. For example, higher version to lower version is not allowed.
	Validate() error
	// CheckCompatible check if target is compatible with source. If compatible, no migration should be executed.
	CheckCompatible() bool
	// CheckSessions check if any sessions are alive. Abort migration if any.
	CheckSessions() error
	// RegisterSession register session to avoid any other migration is also running, registered session will be deleted
	// as soon as possible after migration is done.
	RegisterSession() error
	// Backup source meta information.
	Backup() error
	// Load source meta.
	Load() error
	// Migrate to target backend.
	Migrate() error
	// Rollback all source meta information from backup.
	Rollback() error
	// CleanBackup clean backup of source meta information.
	CleanBackup() error
	// CleanTarget clean target meta information.
	CleanTarget() error
	// Stop complete the migration overflow.
	Stop()
	// DryRun dry run.
	DryRun()
}

type Runner struct {
	ctx      context.Context
	opt      *migrationDef
	meta     *Meta
	initOnce sync.Once
	session  *sessionutil.Session
	address  string
	etcdCli  *clientv3.Client
}

func NewRunner(ctx context.Context, opt *migrationDef) Migration {
	runner := &Runner{ctx: ctx, opt: opt}
	runner.initOnce.Do(runner.init)
	opt.show()
	return runner
}

func (r *Runner) initEtcdCli() {
	cli, err := etcd.GetEtcdClient(&r.opt.source.config.EtcdCfg)
	if err != nil {
		panic(err)
	}
	r.etcdCli = cli
}

func (r *Runner) init() {
	r.initEtcdCli()

	r.session = sessionutil.NewSession(r.ctx, r.opt.source.config.EtcdCfg.MetaRootPath, r.etcdCli)
	// address not important here.
	address := time.Now().String()
	r.address = address
	r.session.Init(Role, address, true, true)
}

func (r *Runner) Validate() error {
	source := r.opt.source.version
	target := r.opt.target.version
	can := target.GTE(source)
	if can {
		return nil
	}
	return fmt.Errorf("higher version to lower version is not allowed, "+
		"source version: %s, target version: %s", source.String(), target.String())
}

func (r *Runner) CheckCompatible() bool {
	source := r.opt.source.version
	version, _ := semver.Parse("2.2.0")
	return source.GTE(version)
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
	if err := r.checkSessionsWithPrefix(typeutil.RootCoordRole); err != nil {
		return err
	}
	if err := r.checkSessionsWithPrefix(typeutil.IndexCoordRole); err != nil {
		return err
	}
	if err := r.checkSessionsWithPrefix(typeutil.IndexNodeRole); err != nil {
		return err
	}
	if err := r.checkSessionsWithPrefix(typeutil.DataCoordRole); err != nil {
		return err
	}
	if err := r.checkSessionsWithPrefix(typeutil.DataNodeRole); err != nil {
		return err
	}
	if err := r.checkSessionsWithPrefix(typeutil.QueryCoordRole); err != nil {
		return err
	}
	if err := r.checkSessionsWithPrefix(typeutil.QueryNodeRole); err != nil {
		return err
	}
	if err := r.checkSessionsWithPrefix(typeutil.ProxyRole); err != nil {
		return err
	}
	return nil
}

func (r *Runner) RegisterSession() error {
	r.session.Register()
	go r.session.LivenessCheck(r.ctx, func() {})
	return nil
}

func (r *Runner) Backup() error {
	backend, err := NewBackend(r.opt.source)
	if err != nil {
		return err
	}
	return backend.Backup(false)
}

func (r *Runner) Load() error {
	backend, err := NewBackend(r.opt.source)
	if err != nil {
		return err
	}
	r.meta, err = backend.Load()
	if err != nil {
		return err
	}
	// r.meta.Show()
	return nil
}

func (r *Runner) Migrate() error {
	source, err := NewBackend(r.opt.source)
	if err != nil {
		return err
	}
	if err := source.Clean(); err != nil {
		return err
	}
	target, err := NewBackend(r.opt.target)
	if err != nil {
		return err
	}
	return target.Save(r.meta, false)
}

func (r *Runner) Rollback() error {
	backend, err := NewBackend(r.opt.source)
	if err != nil {
		return err
	}
	return backend.RollbackFromBackup()
}

func (r *Runner) CleanBackup() error {
	backend, err := NewBackend(r.opt.source)
	if err != nil {
		return err
	}
	return backend.CleanBackup()
}

func (r *Runner) CleanTarget() error {
	backend, err := NewBackend(r.opt.source)
	if err != nil {
		return err
	}
	return backend.Clean()
}

func (r *Runner) Stop() {
	r.session.Revoke(time.Second)
}

func (r *Runner) DryRun() {
	source, err := NewBackend(r.opt.source)
	if err != nil {
		panic(err)
	}
	if err := source.Backup(true); err != nil {
		panic(err)
	}
	r.meta, err = source.Load()
	if err != nil {
		panic(err)
	}
	target, err := NewBackend(r.opt.target)
	if err != nil {
		panic(err)
	}
	if err := target.Save(r.meta, true); err != nil {
		panic(err)
	}
}
