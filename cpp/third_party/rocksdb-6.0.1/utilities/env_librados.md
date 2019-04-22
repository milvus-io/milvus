# Introduce to EnvLibrados
EnvLibrados is a customized RocksDB Env to use RADOS as the backend file system of RocksDB. It overrides all file system related API of default Env. The easiest way to use it is just like following:
```c++
std::string db_name = "test_db";
std::string config_path = "path/to/ceph/config";
DB* db;
Options options;
options.env = EnvLibrados(db_name, config_path);
Status s = DB::Open(options, kDBPath, &db);
...
```
Then EnvLibrados will forward all file read/write operation to the RADOS cluster assigned by config_path. Default pool is db_name+"_pool".

# Options for EnvLibrados
There are some options that users could set for EnvLibrados.
- write_buffer_size. This variable is the max buffer size for WritableFile. After reaching the buffer_max_size, EnvLibrados will sync buffer content to RADOS, then clear buffer.
- db_pool. Rather than using default pool, users could set their own db pool name
- wal_dir. The dir for WAL files. Because RocksDB only has 2-level structure (dir_name/file_name), the format of wal_dir is "/dir_name"(CAN'T be "/dir1/dir2"). Default wal_dir is "/wal".
- wal_pool. Corresponding pool name for WAL files. Default value is db_name+"_wal_pool"

The example of setting options looks like following:
```c++
db_name = "test_db";
db_pool = db_name+"_pool";
wal_dir = "/wal";
wal_pool = db_name+"_wal_pool";
write_buffer_size = 1 << 20;
env_ = new EnvLibrados(db_name, config, db_pool, wal_dir, wal_pool, write_buffer_size);

DB* db;
Options options;
options.env = env_;
// The last level dir name should match the dir name in prefix_pool_map
options.wal_dir = "/tmp/wal";                    

// open DB
Status s = DB::Open(options, kDBPath, &db);
...
```

# Performance Test
## Compile
Check this [link](https://github.com/facebook/rocksdb/blob/master/INSTALL.md) to install the dependencies of RocksDB. Then you can compile it by running `$ make env_librados_test ROCKSDB_USE_LIBRADOS=1` under `rocksdb\`. The configure file used by env_librados_test is `../ceph/src/ceph.conf`. For Ubuntu 14.04, just run following commands:
```bash
$ sudo apt-get install libgflags-dev
$ sudo apt-get install libsnappy-dev
$ sudo apt-get install zlib1g-dev
$ sudo apt-get install libbz2-dev
$ make env_librados_test ROCKSDB_USE_LIBRADOS=1
```

## Test Result
My test environment is Ubuntu 14.04 in VirtualBox with 8 cores and 8G RAM. Following is the test result.

1. Write (1<<20) keys in random order. The time of writing under default env is around 10s while the time of writing under EnvLibrados is varying from 10s to 30s.

2. Write (1<<20) keys in sequential order. The time of writing under default env drops to arround 1s. But the time of writing under EnvLibrados is not changed. 

3. Read (1<<16) keys from (1<<20) keys in random order. The time of reading under both Envs are roughly the same, around 1.8s.

# MyRocks Test
## Compile Ceph
See [link](http://docs.ceph.com/docs/master/install/build-ceph/)

## Start RADOS

```bash
cd ceph-path/src
( ( ./stop.sh; rm -rf dev/*; CEPH_NUM_OSD=3 ./vstart.sh --short --localhost -n
-x -d ; ) ) 2>&1
```

## Compile MySQL

```bash
sudo apt-get update
sudo apt-get install g++ cmake libbz2-dev libaio-dev bison \
zlib1g-dev libsnappy-dev 
sudo apt-get install libgflags-dev libreadline6-dev libncurses5-dev \
libssl-dev liblz4-dev gdb git

git clone https://github.com/facebook/mysql-5.6.git
cd mysql-5.6
git submodule init
git submodule update
cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_SSL=system \
-DWITH_ZLIB=bundled -DMYSQL_MAINTAINER_MODE=0 -DENABLED_LOCAL_INFILE=1 -DROCKSDB_USE_LIBRADOS=1
make install -j8
```

Check this [link](https://github.com/facebook/mysql-5.6/wiki/Build-Steps) for latest compile steps.

## Configure MySQL
Following is the steps of configuration of MySQL.

```bash
mkdir -p /etc/mysql
mkdir -p /var/lib/mysql
mkdir -p /etc/mysql/conf.d
echo -e '[mysqld_safe]\nsyslog' > /etc/mysql/conf.d/mysqld_safe_syslog.cnf
cp /usr/share/mysql/my-medium.cnf /etc/mysql/my.cnf
sed -i 's#.*datadir.*#datadir = /var/lib/mysql#g' /etc/mysql/my.cnf
chown mysql:mysql -R /var/lib/mysql

mysql_install_db --user=mysql --ldata=/var/lib/mysql/
export CEPH_CONFIG_PATH="path/of/ceph/config/file"
mysqld_safe -user=mysql --skip-innodb --rocksdb --default-storage-engine=rocksdb --default-tmp-storage-engine=MyISAM &
mysqladmin -u root password
mysql -u root -p
```

Check this [link](https://gist.github.com/shichao-an/f5639ecd551496ac2d70) for detail information.

```sql
show databases;
create database testdb;
use testdb;
show tables;
CREATE TABLE tbl (id INT AUTO_INCREMENT primary key, str VARCHAR(32));
insert into tbl values (1, "val2");
select * from tbl;
```
