// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package localmigrate

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Check actual entries, not overlapping directory prefixes: legacy JSON stats
// can split even a single segment's _stats directory between the two roots.
func checkTargets(ctx context.Context, root *os.Root, dirs []migrationDir, report *Report) error {
	planned := make(map[string]bool)
	for _, dir := range dirs {
		err := fs.WalkDir(dir.source.root.FS(), dir.path, func(name string, entry fs.DirEntry, err error) error {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if err != nil {
				return err
			}
			if !entry.IsDir() && !entry.Type().IsRegular() {
				return merr.WrapErrDataIntegrityMsg("legacy source %s is not a regular file", name)
			}
			rel, err := filepath.Rel(dir.path, name)
			if err != nil {
				return err
			}
			target := filepath.Join(dir.target, rel)
			isDir, seen := planned[target]
			conflict := seen && (!isDir || !entry.IsDir())
			planned[target] = entry.IsDir()
			info, err := root.Lstat(target)
			switch {
			case err == nil:
				if !entry.IsDir() || !info.IsDir() {
					published, err := publishedCopy(dir.source.root, name, root, target)
					if err != nil {
						return err
					}
					conflict = conflict || !published
				}
			case errors.Is(err, unix.ENOTDIR):
				conflict = true
			case !os.IsNotExist(err):
				return err
			}
			if conflict {
				report.Conflicts = append(report.Conflicts, filepath.Join(root.Name(), target))
				if entry.IsDir() {
					return fs.SkipDir
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func moveEntry(ctx context.Context, source *os.Root, name string, root *os.Root, target string, report *Report) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := rejectSymlinks(source, name); err != nil {
		return err
	}
	if err := rejectSymlinks(root, target); err != nil {
		return err
	}
	info, err := source.Lstat(name)
	if err != nil {
		return err
	}
	dest, err := root.Lstat(target)
	if os.IsNotExist(err) {
		if err := root.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := renameEntry(source, name, root, target); err == nil {
			report.Renamed++
			return nil
		} else if !errors.Is(err, unix.EXDEV) {
			return merr.Wrapf(err, "rename local layout %s to %s", name, target)
		}
		if !info.IsDir() {
			return copyFile(source, name, root, target, report)
		}
		if err := root.Mkdir(target, info.Mode().Perm()); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !info.IsDir() || !dest.IsDir() {
		published, err := publishedCopy(source, name, root, target)
		if err != nil {
			return err
		}
		if published {
			return finishCopy(source, name, root, target, report)
		}
		return merr.WrapErrDataIntegrityMsg("local layout file already exists: %s", filepath.Join(root.Name(), target))
	}
	entries, err := fs.ReadDir(source.FS(), name)
	if err != nil {
		return err
	}
	// Keep a CWD leaf recognizable after an interruption while moving auxiliary
	// files: its packed index files are the last entries removed from the source.
	sort.SliceStable(entries, func(i, j int) bool {
		return !isLegacyUnifiedIndexFile(entries[i].Name()) && isLegacyUnifiedIndexFile(entries[j].Name())
	})
	for _, entry := range entries {
		if err := moveEntry(ctx, source, filepath.Join(name, entry.Name()), root, filepath.Join(target, entry.Name()), report); err != nil {
			return err
		}
	}
	return source.Remove(name) // Only the now-empty source directory is removed.
}

// removeEmptyLegacyDirectories prunes children before parents using the existing
// discovery order. Rmdir leaves nonempty directories and replacement files alone;
// it never removes the working directory itself or recursively deletes content.
func removeEmptyLegacyDirectories(ctx context.Context, source *migrationSource) error {
	for i := len(source.cleanupDirs) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := source.cleanupDirs[i]
		if err := rejectSymlinks(source.root, name); err != nil {
			if errors.Is(err, unix.ENOTDIR) {
				continue
			}
			return err
		}
		parent, err := source.root.Open(filepath.Dir(name))
		if os.IsNotExist(err) || errors.Is(err, unix.ENOTDIR) {
			continue
		}
		if err != nil {
			return merr.Wrapf(err, "open legacy parent for cleanup %s", name)
		}
		err = unix.Unlinkat(int(parent.Fd()), filepath.Base(name), unix.AT_REMOVEDIR)
		closeErr := parent.Close()
		if err != nil && !os.IsNotExist(err) && !errors.Is(err, unix.ENOTEMPTY) &&
			!errors.Is(err, unix.EEXIST) && !errors.Is(err, unix.ENOTDIR) {
			return merr.Wrapf(err, "remove empty legacy directory %s", name)
		}
		if closeErr != nil {
			return merr.Wrapf(closeErr, "close legacy parent for cleanup %s", name)
		}
	}
	return nil
}

func renameEntry(source *os.Root, name string, root *os.Root, target string) error {
	from, err := source.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer from.Close()
	to, err := root.Open(filepath.Dir(target))
	if err != nil {
		return err
	}
	defer to.Close()
	return unix.Renameat(int(from.Fd()), filepath.Base(name), int(to.Fd()), filepath.Base(target))
}

// The hash identifies the source path, not its contents. Keeping the temporary
// hard link until source removal lets a retry recognize its own published copy
// without accepting an unrelated existing file or hashing large data files.
func copyTempPath(source *os.Root, name, target string) string {
	sum := sha256.Sum256([]byte(filepath.Join(source.Name(), name)))
	return filepath.Join(filepath.Dir(target), fmt.Sprintf(".milvus-local-copy-%x", sum))
}

func publishedCopy(source *os.Root, name string, root *os.Root, target string) (bool, error) {
	temp, err := root.Lstat(copyTempPath(source, name, target))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !temp.Mode().IsRegular() {
		return false, merr.WrapErrDataIntegrityMsg("invalid local migration temporary file for %s", target)
	}
	dest, err := root.Lstat(target)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil && dest.Mode().IsRegular() && os.SameFile(temp, dest), err
}

func copyFile(source *os.Root, name string, root *os.Root, target string, report *Report) error {
	in, err := source.Open(name)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return merr.WrapErrDataIntegrityMsg("legacy source %s is not a regular file", name)
	}
	temp := copyTempPath(source, name, target)
	if err := rejectSymlinks(root, temp); err != nil {
		return err
	}
	if previous, err := root.Lstat(temp); err == nil {
		if !previous.Mode().IsRegular() {
			return merr.WrapErrDataIntegrityMsg("invalid local migration temporary file for %s", target)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	// A previous interrupted copy was never published. Recopy from the intact
	// source; never expose a partially written file under the final name.
	if err := root.Remove(temp); err != nil && !os.IsNotExist(err) {
		return err
	}
	out, err := root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	published := false
	defer func() {
		out.Close()
		if !published {
			_ = root.Remove(temp)
		}
	}()
	if _, err := io.Copy(out, in); err != nil {
		return merr.Wrapf(err, "copy local layout %s to %s", name, target)
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := root.Link(temp, target); err != nil { // Atomic publication, never overwrites.
		return err
	}
	published = true
	return finishCopy(source, name, root, target, report)
}

// Persist the target entry and every ancestor link that MkdirAll may have
// created. Syncing only the leaf directory does not make new parents durable.
// Repeat this on recovery too: an existing directory may be from an interrupted
// migration that never finished syncing its parents.
func syncCopyTargetDirectories(root *os.Root, directory string) error {
	for {
		parent, err := root.Open(directory)
		if err != nil {
			return err
		}
		err = parent.Sync()
		closeErr := parent.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if directory == "." {
			return nil
		}
		directory = filepath.Dir(directory)
	}
}

func finishCopy(source *os.Root, name string, root *os.Root, target string, report *Report) error {
	if err := syncCopyTargetDirectories(root, filepath.Dir(target)); err != nil {
		return err
	}
	sourceParent, err := source.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer sourceParent.Close()
	if err := source.Remove(name); err != nil {
		return err
	}
	// Persist source removal before dropping the publication marker. Otherwise
	// a power loss could resurrect the source without proof that target is ours.
	if err := sourceParent.Sync(); err != nil {
		return err
	}
	// Best-effort: the copy is already published and the source is gone, so the
	// migration is complete. Returning an error here would leave the temporary
	// hard link behind forever, because the next run no longer sees the source
	// and therefore never revisits this target.
	_ = root.Remove(copyTempPath(source, name, target))
	report.Copied++
	return nil
}
