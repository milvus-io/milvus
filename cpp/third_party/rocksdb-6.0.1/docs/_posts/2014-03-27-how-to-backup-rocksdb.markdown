---
title: How to backup RocksDB?
layout: post
author: icanadi
category: blog
redirect_from:
  - /blog/191/how-to-backup-rocksdb/
---

In RocksDB, we have implemented an easy way to backup your DB. Here is a simple example:



    #include "rocksdb/db.h"
    #include "utilities/backupable_db.h"
    using namespace rocksdb;

    DB* db;
    DB::Open(Options(), "/tmp/rocksdb", &db);
    BackupableDB* backupable_db = new BackupableDB(db, BackupableDBOptions("/tmp/rocksdb_backup"));
    backupable_db->Put(...); // do your thing
    backupable_db->CreateNewBackup();
    delete backupable_db; // no need to also delete db

<!--truncate-->


This simple example will create a backup of your DB in "/tmp/rocksdb_backup". Creating new BackupableDB consumes DB* and you should be calling all the DB methods on object `backupable_db` going forward.

Restoring is also easy:



    RestoreBackupableDB* restore = new RestoreBackupableDB(Env::Default(), BackupableDBOptions("/tmp/rocksdb_backup"));
    restore->RestoreDBFromLatestBackup("/tmp/rocksdb", "/tmp/rocksdb");
    delete restore;




This code will restore the backup back to "/tmp/rocksdb". The second parameter is the location of log files (In some DBs they are different from DB directory, but usually they are the same. See Options::wal_dir for more info).

An alternative API for backups is to use BackupEngine directly:



    #include "rocksdb/db.h"
    #include "utilities/backupable_db.h"
    using namespace rocksdb;

    DB* db;
    DB::Open(Options(), "/tmp/rocksdb", &db);
    db->Put(...); // do your thing
    BackupEngine* backup_engine = BackupEngine::NewBackupEngine(Env::Default(), BackupableDBOptions("/tmp/rocksdb_backup"));
    backup_engine->CreateNewBackup(db);
    delete db;
    delete backup_engine;




Restoring with BackupEngine is similar to RestoreBackupableDB:



    BackupEngine* backup_engine = BackupEngine::NewBackupEngine(Env::Default(), BackupableDBOptions("/tmp/rocksdb_backup"));
    backup_engine->RestoreDBFromLatestBackup("/tmp/rocksdb", "/tmp/rocksdb");
    delete backup_engine;




Backups are incremental. You can create a new backup with `CreateNewBackup()` and only the new data will be copied to backup directory (for more details on what gets copied, see "Under the hood"). Checksum is always calculated for any backuped file (including sst, log, and etc). It is used to make sure files are kept sound in the file system. Checksum is also verified for files from the previous backups even though they do not need to be copied. A checksum mismatch aborts the current backup (see "Under the hood" for more details). Once you have more backups saved, you can issue `GetBackupInfo()` call to get a list of all backups together with information on timestamp of the backup and the size (please note that sum of all backups' sizes is bigger than the actual size of the backup directory because some data is shared by multiple backups). Backups are identified by their always-increasing IDs. `GetBackupInfo()` is available both in `BackupableDB` and `RestoreBackupableDB`.

You probably want to keep around only small number of backups. To delete old backups, just call `PurgeOldBackups(N)`, where N is how many backups you'd like to keep. All backups except the N newest ones will be deleted. You can also choose to delete arbitrary backup with call `DeleteBackup(id)`.

`RestoreDBFromLatestBackup()` will restore the DB from the latest consistent backup. An alternative is `RestoreDBFromBackup()` which takes a backup ID and restores that particular backup. Checksum is calculated for any restored file and compared against the one stored during the backup time. If a checksum mismatch is detected, the restore process is aborted and `Status::Corruption` is returned. Very important thing to note here: Let's say you have backups 1, 2, 3, 4. If you restore from backup 2 and start writing more data to your database, newly created backup will delete old backups 3 and 4 and create new backup 3 on top of 2.



## Advanced usage


Let's say you want to backup your DB to HDFS. There is an option in `BackupableDBOptions` to set `backup_env`, which will be used for all file I/O related to backup dir (writes when backuping, reads when restoring). If you set it to HDFS Env, all the backups will be stored in HDFS.

`BackupableDBOptions::info_log` is a Logger object that is used to print out LOG messages if not-nullptr.

If `BackupableDBOptions::sync` is true, we will sync data to disk after every file write, guaranteeing that backups will be consistent after a reboot or if machine crashes. Setting it to false will speed things up a bit, but some (newer) backups might be inconsistent. In most cases, everything should be fine, though.

If you set `BackupableDBOptions::destroy_old_data` to true, creating new `BackupableDB` will delete all the old backups in the backup directory.

`BackupableDB::CreateNewBackup()` method takes a parameter `flush_before_backup`, which is false by default. When `flush_before_backup` is true, `BackupableDB` will first issue a memtable flush and only then copy the DB files to the backup directory. Doing so will prevent log files from being copied to the backup directory (since flush will delete them). If `flush_before_backup` is false, backup will not issue flush before starting the backup. In that case, the backup will also include log files corresponding to live memtables. Backup will be consistent with current state of the database regardless of `flush_before_backup` parameter.



## Under the hood


`BackupableDB` implements `DB` interface and adds four methods to it: `CreateNewBackup()`, `GetBackupInfo()`, `PurgeOldBackups()`, `DeleteBackup()`. Any `DB` interface calls will get forwarded to underlying `DB` object.

When you call `BackupableDB::CreateNewBackup()`, it does the following:





  1. Disable file deletions



  2. Get live files (this includes table files, current and manifest file).



  3. Copy live files to the backup directory. Since table files are immutable and filenames unique, we don't copy a table file that is already present in the backup directory. For example, if there is a file `00050.sst` already backed up and `GetLiveFiles()` returns `00050.sst`, we will not copy that file to the backup directory. However, checksum is calculated for all files regardless if a file needs to be copied or not. If a file is already present, the calculated checksum is compared against previously calculated checksum to make sure nothing crazy happened between backups. If a mismatch is detected, backup is aborted and the system is restored back to the state before `BackupableDB::CreateNewBackup()` is called. One thing to note is that a backup abortion could mean a corruption from a file in backup directory or the corresponding live file in current DB. Both manifest and current files are copied, since they are not immutable.



  4. If `flush_before_backup` was set to false, we also need to copy log files to the backup directory. We call `GetSortedWalFiles()` and copy all live files to the backup directory.



  5. Enable file deletions




Backup IDs are always increasing and we have a file `LATEST_BACKUP` that contains the ID of the latest backup. If we crash in middle of backing up, on a restart we will detect that there are newer backup files than `LATEST_BACKUP` claims there are. In that case, we will delete any backup newer than `LATEST_BACKUP` and clean up all the files since some of the table files might be corrupted. Having corrupted table files in the backup directory is dangerous because of our deduplication strategy.



## Further reading


For the API details, see `include/utilities/backupable_db.h`. For the implementation, see `utilities/backupable/backupable_db.cc`.
