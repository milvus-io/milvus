---
title: How to persist in-memory RocksDB database?
layout: post
author: icanadi
category: blog
redirect_from:
  - /blog/245/how-to-persist-in-memory-rocksdb-database/
---

In recent months, we have focused on optimizing RocksDB for in-memory workloads. With growing RAM sizes and strict low-latency requirements, lots of applications decide to keep their entire data in memory. Running in-memory database with RocksDB is easy -- just mount your RocksDB directory on tmpfs or ramfs [1]. Even if the process crashes, RocksDB can recover all of your data from in-memory filesystem. However, what happens if the machine reboots?

<!--truncate-->

In this article we will explain how you can recover your in-memory RocksDB database even after a machine reboot.

Every update to RocksDB is written to two places - one is an in-memory data structure called memtable and second is write-ahead log. Write-ahead log can be used to completely recover the data in memtable. By default, when we flush the memtable to table file, we also delete the current log, since we don't need it anymore for recovery (the data from the log is "persisted" in the table file -- we say that the log file is obsolete). However, if your table file is stored in in-memory file system, you may need the obsolete write-ahead log to recover the data after the machine reboots. Here's how you can do that.

Options::wal_dir is the directory where RocksDB stores write-ahead log files. If you configure this directory to be on flash or disk, you will not lose current log file on machine reboot.
Options::WAL_ttl_seconds is the timeout when we delete the archived log files. If the timeout is non-zero, obsolete log files will be moved to `archive/` directory under Options::wal_dir. Those archived log files will only be deleted after the specified timeout.

Let's assume Options::wal_dir is a directory on persistent storage and Options::WAL_ttl_seconds is set to one day. To fully recover the DB, we also need to backup the current snapshot of the database (containing table and metadata files) with a frequency of less than one day. RocksDB provides an utility that enables you to easily backup the snapshot of your database. You can learn more about it here: [How to backup RocksDB?](https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB%3F)

You should configure the backup process to avoid backing up log files, since they are already stored in persistent storage. To do that, set BackupableDBOptions::backup_log_files to false.

Restore process by default cleans up entire DB and WAL directory. Since we didn't include log files in the backup, we need to make sure that restoring the database doesn't delete log files in WAL directory. When restoring, configure RestoreOptions::keep_log_file to true. That option will also move any archived log files back to WAL directory, enabling RocksDB to replay all archived log files and rebuild the in-memory database state.

To reiterate, here's what you have to do:




  * Set DB directory to tmpfs or ramfs mounted drive



  * Set Options::wal_log to a directory on persistent storage



  * Set Options::WAL_ttl_seconds to T seconds



  * Backup RocksDB every T/2 seconds, with BackupableDBOptions::backup_log_files = false



  * When you lose data, restore from backup with RestoreOptions::keep_log_file = true





[1] You might also want to consider using [PlainTable format](https://github.com/facebook/rocksdb/wiki/PlainTable-Format) for table files
