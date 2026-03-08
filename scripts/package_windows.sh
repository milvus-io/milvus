ETCD_VERSION=3.5.0

set -xe

date_str="$(date +%Y%m%d)"
short_hash="$(git log -1 --pretty=%h)"

version="${date_str}-${short_hash}"
target_filename="milvus-windows-${version}.zip"

script_dir="$(cd $(dirname $0); pwd)"
repo_dir="$(dirname ${script_dir})"
bin_dir="${repo_dir}/bin"
package_dir="${repo_dir}/windows_package"


# prepare package dir
rm -fr ${package_dir}
mkdir -p ${package_dir}
cp -fr ${repo_dir}/deployments/windows ${package_dir}/milvus

# resolve all dll for milvus.exe
cd ${bin_dir}
cp -fr milvus milvus.exe

find ${repo_dir} -name \*.dll | xargs -I {} cp -frv {} . || :
for x in $(ldd milvus.exe | awk '{print $1}') ; do
  if [ -f ${MINGW_PREFIX}/bin/$x ] ; then
    cp -frv ${MINGW_PREFIX}/bin/$x .
  fi
done

# prepare package
cd ${package_dir}
mkdir -p milvus/{bin,configs,run}

cp -frv ${bin_dir}/*.dll milvus/bin
cp -frv ${bin_dir}/*.exe milvus/bin

# configs
cp -fr ${repo_dir}/configs/* milvus/configs/

# patch config /var -> var
sed s@/var/lib@var/lib@ -i milvus/configs/milvus.yaml

# patch all bat with dos format
find -name \*.bat | xargs -I {} unix2dos {}

# download minio
wget -q -O milvus/bin/minio.exe https://dl.min.io/server/minio/release/windows-amd64/minio.exe

# download etcd
wget -q -O etcd.zip https://github.com/etcd-io/etcd/releases/download/v${ETCD_VERSION}/etcd-v${ETCD_VERSION}-windows-amd64.zip
unzip etcd.zip
find -name etcd.exe | xargs -I {} cp -frv {} milvus/bin

zip -r ${target_filename} milvus
