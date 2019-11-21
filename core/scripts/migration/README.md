## Data Migration

####0.3.x
legacy data is not migrate-able for later versions

####0.4.x
legacy data can be reused directly by 0.5.x

legacy data can be migrated to 0.6.x

####0.5.x
legacy data can be migrated to 0.6.x

####0.6.x
how to migrate legacy 0.4.x/0.5.x data

for sqlite meta:
```shell
 $ sqlite3 [parth_to]/meta.sqlite < sqlite_4_to_6.sql
```

for mysql meta:
```shell
 $ mysql -h127.0.0.1 -uroot -p123456 -Dmilvus < mysql_4_to_6.sql
```



