// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package localmigrate repairs the double-root layout written by Milvus 3.0.0
// and 3.0.1 and late 2.6 unified indexes written in the working directory.
// Migration merges directories without overwriting files, using rename where
// possible and copying across filesystems. No backup or migration journal is created.
//
// This package is unix-only: it needs a cross-root renameat and a
// directory-only unlinkat, neither of which os.Root offers (os.Root.Rename
// cannot cross roots, and os.Root.Remove would unlink a replacement file).
package localmigrate

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/flock"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// LayoutDirs are layouts written through the formerly double-rooted Arrow
// filesystem. ChunkManager layouts such as delta_log and stats_log are not moved.
var LayoutDirs = []string{"insert_log", "text_log", "json_stats", "index_files", "index_v1"}

const lockFileName = ".milvus-local-layout.lock"

// reservedNamespace is a directory name this package keeps for itself under the
// storage root. It is distinct from lockFileName and is never a move source or
// a cleanup target, so a user directory that happens to use the name survives.
const reservedNamespace = ".milvus-local-layout"

// maxReportedConflicts bounds the conflict list embedded in the returned error.
// Report.Conflicts keeps them all; an operator-facing message must not grow to
// the size of the storage root.
const maxReportedConflicts = 20

// Options controls automatic migration before the instance starts.
type Options struct {
	// LegacyPrefix is minio.rootPath. Its insert_log namespace is still read in
	// place by legacy manifests and must never be moved.
	LegacyPrefix string
	// OnPlan reports every directory this run decided to move, once, after
	// discovery and conflict checking and before anything has moved. OnDirStart
	// and OnDirDone then report each of those directories as it is moved, so a
	// migration that takes minutes is visible while it runs instead of only in
	// hindsight. All three are optional.
	OnPlan     func(dirs []Dir)
	OnDirStart func(dir Dir)
	OnDirDone  func(dir Dir, elapsed time.Duration)
}

// Dir describes one legacy directory the migration moves. Source and Target are
// absolute paths; Files and Bytes count the regular files below Source.
type Dir struct {
	Source string
	Target string
	Files  int
	Bytes  int64
}

// Report describes legacy directories moved in this run.
type Report struct {
	Root       string
	Candidates []string
	Conflicts  []string
	Renamed    int
	Copied     int
	Bytes      int64
	Elapsed    time.Duration
}

func (r *Report) String() string {
	return fmt.Sprintf("root=%s candidates=%v renamed=%d copied=%d bytes=%d conflicts=%d elapsed=%s",
		r.Root, r.Candidates, r.Renamed, r.Copied, r.Bytes, len(r.Conflicts), r.Elapsed)
}

// DisplacedRoots describes the one double-joined root; "/" has no displacement.
func DisplacedRoots(absRoot string) []string {
	absRoot = filepath.Clean(absRoot)
	if !filepath.IsAbs(absRoot) || absRoot == string(filepath.Separator) {
		return nil
	}
	return []string{filepath.Join(absRoot, strings.TrimLeft(absRoot, string(filepath.Separator)))}
}

type migrationSource struct {
	kind        string
	root        *os.Root
	base        string
	physical    string
	cleanupDirs []string // Validated legacy directories in WalkDir order.
}

type migrationDir struct {
	source *migrationSource
	path   string
	target string
	files  int
	bytes  int64
	dirs   []string // Inspected directories, reused if only an empty skeleton remains.
}

// describe renders the absolute source and target of d for reporting.
func (d migrationDir) describe(root *os.Root) Dir {
	return Dir{
		Source: filepath.Join(d.source.root.Name(), d.path),
		Target: filepath.Join(root.Name(), d.target),
		Files:  d.files,
		Bytes:  d.bytes,
	}
}

// Migrate merges old local layouts into the canonical storage root. Missing
// subtrees are renamed directly; cross-filesystem files are copied before their
// sources are removed. Existing directories are merged, never overwritten.
// A later startup resumes from the remaining sources without a migration journal.
//
// Canceling ctx stops the migration between files and directories, which is
// safe precisely because it is resumable: the sources that have not moved yet
// are rediscovered on the next run. A single file copy is not interrupted, so a
// deadline bounds the walk and the per-file loop, not one large file.
func Migrate(ctx context.Context, absRoot string, opts Options) (*Report, error) {
	if !filepath.IsAbs(absRoot) {
		return nil, merr.WrapErrParameterInvalidMsg("local storage root %q must be absolute", absRoot)
	}
	absRoot = filepath.Clean(absRoot)
	startedAt := time.Now()
	report := &Report{Root: absRoot}
	defer func() { report.Elapsed = time.Since(startedAt) }()
	if info, err := os.Stat(absRoot); err == nil && !info.IsDir() {
		return report, merr.WrapErrParameterInvalidMsg("local storage root %q is not a directory", absRoot)
	}
	root, err := os.OpenRoot(absRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return report, nil
		}
		return report, merr.Wrapf(err, "open local storage root %s", absRoot)
	}
	defer root.Close()
	cwd, err := openLegacyWorkingDirectory()
	if err != nil {
		return report, err
	}
	defer cwd.Close()
	physicalRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return report, err
	}
	activeNamespace, err := legacyNamespace(physicalRoot, opts.LegacyPrefix)
	if err != nil {
		return report, err
	}
	if err := rejectSymlinks(root, lockFileName); err != nil {
		return report, err
	}
	lock := flock.New(filepath.Join(root.Name(), lockFileName))
	locked, err := lock.TryLock()
	if err != nil {
		return report, merr.Wrap(err, "lock local layout migration")
	}
	if !locked {
		return report, merr.WrapErrServiceUnavailableMsg("another local layout migration holds %s", lock.Path())
	}
	defer lock.Unlock()
	sources, err := migrationSources(root, cwd, physicalRoot, absRoot)
	if err != nil {
		return report, err
	}
	dirs, err := discoverDirectories(ctx, sources, physicalRoot, activeNamespace, report)
	if err != nil {
		return report, err
	}
	// CWD leaves are discovered through a map, so fix a stable order before
	// anything is reported or moved: the plan, the move order and the log then
	// agree across runs and are comparable after an interruption.
	sort.SliceStable(dirs, func(i, j int) bool {
		if dirs[i].source.root.Name() != dirs[j].source.root.Name() {
			return dirs[i].source.root.Name() < dirs[j].source.root.Name()
		}
		return dirs[i].path < dirs[j].path
	})
	if err := checkTargets(ctx, root, dirs, report); err != nil {
		return report, err
	}
	if len(report.Conflicts) != 0 {
		sort.Strings(report.Conflicts)
		return report, merr.WrapErrDataIntegrityMsg("local layout files conflict (%d total): %v",
			len(report.Conflicts), report.Conflicts[:min(len(report.Conflicts), maxReportedConflicts)])
	}
	plan := make([]Dir, 0, len(dirs))
	for _, directory := range dirs {
		plan = append(plan, directory.describe(root))
	}
	if opts.OnPlan != nil {
		opts.OnPlan(plan)
	}
	for index, directory := range dirs {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if opts.OnDirStart != nil {
			opts.OnDirStart(plan[index])
		}
		directoryStartedAt := time.Now()
		if err := moveEntry(ctx, directory.source.root, directory.path, root, directory.target, report); err != nil {
			return report, err
		}
		report.Bytes += directory.bytes
		if opts.OnDirDone != nil {
			opts.OnDirDone(plan[index], time.Since(directoryStartedAt))
		}
	}
	for _, source := range sources {
		if err := removeEmptyLegacyDirectories(ctx, source); err != nil {
			return report, err
		}
	}
	return report, nil
}

func openLegacyWorkingDirectory() (*os.Root, error) {
	directory, err := os.Getwd()
	if err != nil {
		return nil, merr.Wrap(err, "resolve current working directory for local layout migration")
	}
	root, err := os.OpenRoot(filepath.Clean(directory))
	if err != nil {
		return nil, merr.Wrapf(err, "open legacy working directory %s", directory)
	}
	return root, nil
}

func migrationSources(root, cwd *os.Root, physicalRoot, configuredRoot string) ([]*migrationSource, error) {
	var sources []*migrationSource
	// R=/ has no displaced root, but may still have indexes under an old CWD.
	if candidates := DisplacedRoots(configuredRoot); len(candidates) != 0 {
		base, err := filepath.Rel(root.Name(), candidates[0])
		if err != nil {
			return nil, err
		}
		sources = append(sources, &migrationSource{kind: "displaced", root: root, base: base, physical: physicalRoot})
	}
	rootInfo, err := root.Stat(".")
	if err != nil {
		return nil, err
	}
	cwdInfo, err := cwd.Stat(".")
	if err != nil {
		return nil, err
	}
	if !os.SameFile(rootInfo, cwdInfo) {
		// The displaced source already includes its text_log and index_files.
		// Do not discover them twice when CWD names the same physical directory.
		for _, source := range sources {
			if info, err := source.root.Stat(source.base); err == nil && os.SameFile(info, cwdInfo) {
				return sources, nil
			}
			// Leave missing or unsafe source validation to discovery below.
		}
		physicalCWD, err := filepath.EvalSymlinks(cwd.Name())
		if err != nil {
			return nil, err
		}
		sources = append(sources, &migrationSource{kind: "cwd", root: cwd, base: ".", physical: physicalCWD})
	}
	return sources, nil
}

func legacyNamespace(root, prefix string) (string, error) {
	if prefix == "." {
		prefix = ""
	}
	if prefix != "" && (!filepath.IsLocal(prefix) || filepath.Clean(prefix) != prefix || strings.Contains(prefix, "://")) {
		return "", merr.WrapErrParameterInvalidMsg("unsafe legacy minio.rootPath %q", prefix)
	}
	return filepath.Join(root, prefix, "insert_log"), nil
}

func discoverDirectories(ctx context.Context, sources []*migrationSource, root, active string, report *Report) ([]migrationDir, error) {
	var directories []migrationDir
	for _, source := range sources {
		if source.kind == "displaced" {
			start := len(directories)
			for _, layout := range LayoutDirs {
				path := filepath.Join(source.base, layout)
				if err := addDirectory(ctx, source, root, active, path, layout, &directories); err != nil {
					return nil, err
				}
			}
			if len(directories) != start {
				report.Candidates = append(report.Candidates, filepath.Join(source.root.Name(), source.base))
			}
			continue
		}
		// CWD indexes are moved at their deepest eligible directory. This keeps
		// unrelated files in the old working directory untouched.
		groups := make(map[string]*migrationDir)
		for _, layout := range []string{"text_log", "index_files"} {
			if err := collectLegacyIndexDirs(ctx, source, root, active, layout, groups); err != nil {
				return nil, err
			}
		}
		for _, directory := range groups {
			directories = append(directories, *directory)
		}
		if len(groups) != 0 {
			report.Candidates = append(report.Candidates, source.root.Name())
		}
	}
	return directories, nil
}

func addDirectory(ctx context.Context, source *migrationSource, root, active, path, target string, directories *[]migrationDir) error {
	if err := rejectSymlinks(source.root, path); err != nil {
		return err
	}
	info, err := source.root.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return merr.WrapErrDataIntegrityMsg("legacy layout %s is not a directory", filepath.Join(source.root.Name(), path))
	}
	directory := migrationDir{source: source, path: path, target: target}
	if err := inspectDirectory(ctx, source.root, path, &directory); err != nil {
		return err
	}
	if directory.files == 0 {
		// An interrupted merge may have moved the last file but left its empty
		// parents. Reuse discovery for cleanup after all migration work succeeds.
		// Empty live/reserved namespaces must still be left untouched.
		if protectNamespace(source, path, active, root) == nil {
			source.cleanupDirs = append(source.cleanupDirs, directory.dirs...)
		}
		return nil
	}
	if err := protectNamespace(source, path, active, root); err != nil {
		return err
	}
	*directories = append(*directories, directory)
	return nil
}

func collectLegacyIndexDirs(ctx context.Context, source *migrationSource, root, active, layout string, groups map[string]*migrationDir) error {
	if err := rejectSymlinks(source.root, layout); err != nil {
		return err
	}
	info, err := source.root.Lstat(layout)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return merr.WrapErrDataIntegrityMsg("legacy index layout %s is not a directory", filepath.Join(source.root.Name(), layout))
	}
	return fs.WalkDir(source.root.FS(), layout, func(name string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return merr.WrapErrDataIntegrityMsg("legacy source %s is a symlink; resolve manually", filepath.Join(source.root.Name(), name))
		}
		if entry.IsDir() {
			physical, err := filepath.EvalSymlinks(filepath.Join(source.physical, name))
			if err != nil {
				return err
			}
			if entersCanonicalRoot(source.physical, root, physical) {
				return fs.SkipDir
			}
			// Include empty prefixes left by an interrupted migration, even when
			// no packed file remains to identify a leaf. Never collect live paths
			// or unrelated directories for cleanup.
			if legacyIndexPath(name, true) && !pathsOverlap(physical, active) &&
				!pathsOverlap(physical, filepath.Join(root, reservedNamespace)) {
				source.cleanupDirs = append(source.cleanupDirs, name)
			}
			return nil
		}
		if !legacyIndexPath(name, false) {
			return nil
		}
		parent := filepath.Dir(name)
		if directory := groups[parent]; directory != nil {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			directory.files++
			directory.bytes += info.Size()
			return nil
		}
		directory := &migrationDir{source: source, path: parent, target: parent}
		if err := protectNamespace(source, parent, active, root); err != nil {
			return err
		}
		groups[parent] = directory
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return merr.WrapErrDataIntegrityMsg("legacy source %s is not a regular file", filepath.Join(source.root.Name(), name))
		}
		directory.files = 1
		directory.bytes = info.Size()
		return nil
	})
}

func inspectDirectory(ctx context.Context, root *os.Root, path string, directory *migrationDir) error {
	return fs.WalkDir(root.FS(), path, func(name string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return merr.WrapErrDataIntegrityMsg("legacy source %s is a symlink; resolve manually", filepath.Join(root.Name(), name))
		}
		if entry.IsDir() {
			if directory.files == 0 {
				directory.dirs = append(directory.dirs, name)
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return merr.WrapErrDataIntegrityMsg("legacy source %s is not a regular file", filepath.Join(root.Name(), name))
		}
		// Nonempty trees are moved normally; retain directory names only while
		// this could still be an empty skeleton requiring cleanup.
		directory.dirs = nil
		directory.files++
		directory.bytes += info.Size()
		return nil
	})
}

func protectNamespace(source *migrationSource, path, active, root string) error {
	original := filepath.Join(source.physical, path)
	if source.kind == "displaced" {
		original = filepath.Join(source.physical, source.base, filepath.Base(path))
	}
	if pathsOverlap(original, active) {
		return merr.WrapErrDataIntegrityMsg("legacy source %s overlaps the live manifest namespace %s; refusing to move", original, active)
	}
	if pathsOverlap(original, filepath.Join(root, reservedNamespace)) {
		return merr.WrapErrDataIntegrityMsg("legacy source %s overlaps the reserved migration namespace", original)
	}
	return nil
}

func withinDirectory(directory, name string) bool {
	rel, err := filepath.Rel(directory, name)
	return err == nil && filepath.IsLocal(rel)
}

func pathsOverlap(a, b string) bool {
	return withinDirectory(a, b) || withinDirectory(b, a)
}

func entersCanonicalRoot(cwd, root, name string) bool {
	return withinDirectory(cwd, root) && withinDirectory(root, name)
}

// legacyIndexPath admits only the directory prefixes of eligible unified index files.
func legacyIndexPath(name string, directory bool) bool {
	parts := strings.Split(filepath.ToSlash(name), "/")
	ids := 4
	if parts[0] == "text_log" {
		ids = 6
	} else if parts[0] != "index_files" {
		return false
	}
	for i := 1; i < len(parts) && i <= ids; i++ {
		if !isLegacyIndexID(parts[i]) {
			return false
		}
	}
	if directory {
		return len(parts) <= ids+1
	}
	return len(parts) == ids+2 && isLegacyUnifiedIndexFile(parts[len(parts)-1])
}

func isLegacyIndexID(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func isLegacyUnifiedIndexFile(name string) bool {
	const prefix, suffix = "milvus_packed_", "_index.v3"
	return len(name) > len(prefix)+len(suffix) && strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix)
}

func rejectSymlinks(root *os.Root, name string) error {
	if !filepath.IsLocal(name) {
		return merr.WrapErrDataIntegrityMsg("migration path %q is outside its opened root", name)
	}
	current := ""
	for _, component := range strings.Split(filepath.Clean(name), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := root.Lstat(current)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return merr.WrapErrDataIntegrityMsg("migration path %s is a symlink; resolve manually", filepath.Join(root.Name(), current))
		}
	}
	return nil
}
