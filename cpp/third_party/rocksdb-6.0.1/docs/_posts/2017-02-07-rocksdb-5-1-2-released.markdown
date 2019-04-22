---
title: RocksDB 5.1.2 Released!
layout: post
author: maysamyabandeh
category: blog
---

### Public API Change
* Support dynamically change `delete_obsolete_files_period_micros` option via SetDBOptions().
* Added EventListener::OnExternalFileIngested which will be called when IngestExternalFile() add a file successfully.
* BackupEngine::Open and BackupEngineReadOnly::Open now always return error statuses matching those of the backup Env.

### Bug Fixes
* Fix the bug that if 2PC is enabled, checkpoints may loss some recent transactions.
* When file copying is needed when creating checkpoints or bulk loading files, fsync the file after the file copying.
