%if 0%{!?version:1}
%global version 2.0.0
%endif

%if 0%{!?release:1}
%global release preGA
%endif

%if 0%{!?tag_version:1}
%global tag_version 2.0.0-pre-ga
%endif

%if 0%{!?git_commit:1}
%global git_commit 95f0e9a
%endif

%global openblas_version 0.3.9
%global boost_version 1.65.1
%global boost_version_alias 1_65_1
%global tbb_commit 9e219e24fe223b299783200f217e9d27790a87b0
%global go_version 1.15.2
%global etcd_version 3.5.0
%global arch linux-amd64
%global minio_version RELEASE.2021-02-14T04-01-33Z


Name:             milvus
Version:          %{version}
Release:          %{release}
Summary:          Milvus V2 RPM
License:          Apache License 2.0
BuildRequires:    epel-release centos-release-scl-rh wget make automake devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-gcc-gfortran
Provides:         libtbb.so()(64bit)
ExclusiveArch:    x86_64
Source0:          https://github.com/milvus-io/milvus/archive/refs/tags/v%{tag_version}.tar.gz#/milvus-%{tag_version}.tar.gz
Source1:          https://github.com/xianyi/OpenBLAS/archive/v%{openblas_version}.tar.gz#/OpenBLAS-%{openblas_version}.tar.gz
Source2:          https://boostorg.jfrog.io/artifactory/main/release/%{boost_version}/source/boost_%{boost_version_alias}.tar.gz
Source3:          https://github.com/wjakob/tbb/archive/%{tbb_commit}.zip#/tbb.zip
Source4:          https://go.dev/dl/go%{go_version}.%{arch}.tar.gz#/go.tar.gz
Source5:          https://github.com/etcd-io/etcd/releases/download/v%{etcd_version}/etcd-v%{etcd_version}-%{arch}.tar.gz
Source6:          https://dl.min.io/server/minio/release/%{arch}/archive/minio.%{minio_version}#/minio

%description
Milvus is an open-source vector database built to power embedding similarity search and AI applications. 
Milvus makes unstructured data search more accessible, and provides a consistent user experience regardless of the deployment environment.


%prep
mkdir -p %{_builddir}
tar -xf %{SOURCE0} -C %{_builddir}/
tar -xf %{SOURCE1} -C %{_builddir}/
tar -xf %{SOURCE2} -C %{_builddir}/
unzip %{SOURCE3} -d %{_builddir}/
# install go
rm -rf /usr/local/go && rm -f /usr/bin/go && tar -C /usr/local -xzf %{SOURCE4} && ln -s /usr/local/go/bin/go /usr/bin/go

tar -xf %{SOURCE5} -C %{_builddir}/
cp %{SOURCE6} %{_builddir}/
echo "source scl_source enable devtoolset-7" > /etc/profile.d/devtoolset-7.sh

%build
# build install lib tbb
cd %{_builddir}/tbb-%{tbb_commit}/build
source /etc/profile.d/devtoolset-7.sh && \
    cd build && \
    cmake .. && make -j && make install

# build install lib openblas
cd %{_builddir}/OpenBLAS-%{openblas_version}
source /etc/profile.d/devtoolset-7.sh && make TARGET=CORE2 DYNAMIC_ARCH=1 DYNAMIC_OLDER=1 USE_THREAD=0 USE_OPENMP=0 FC=gfortran CC=gcc COMMON_OPT="-O3 -g -fPIC" FCOMMON_OPT="-O3 -g -fPIC -frecursive" NMAX="NUM_THREADS=128" LIBPREFIX="libopenblas" LAPACKE="NO_LAPACKE=1" INTERFACE64=0 NO_STATIC=1 && \
    make PREFIX=/usr NO_STATIC=1 install

# build install lib boost
cd %{_builddir}/boost_%{boost_version_alias}
source /etc/profile.d/devtoolset-7.sh && ./bootstrap.sh --prefix=/usr/lib --with-toolset=gcc --without-libraries=python && \
    ./b2 -j2 --prefix=/usr/lib --without-python toolset=gcc install

# build install milvus
cd %{_builddir}/milvus-%{tag_version}
## remove rpath config
cmakeRpathFiles=(
    "internal/core/CMakeLists.txt"
    "internal/core/src/index/CMakeLists.txt"
)
for cmakeRpathFile in "${cmakeRpathFiles[@]}"; do
    # remove all set(CMAKE_INSTALL_RPATH.* lines
    sed -i '/^set( CMAKE_INSTALL_RPATH/d' "$cmakeRpathFile"
done

goRpathFiles=(
    "internal/indexnode/index.go"
    "internal/indexnode/indexnode.go"
    "internal/querynode/cgo_helper.go"
    "internal/querynode/collection.go"
    "internal/querynode/collection_replica.go"
    "internal/querynode/load_index_info.go"
    "internal/querynode/partition.go"
    "internal/querynode/plan.go"
    "internal/querynode/query_node.go"
    "internal/querynode/reduce.go"
    "internal/querynode/segment.go"
)

for goRpathFiles in "${goRpathFiles[@]}"; do
    # remove -Wl,-rpath=${SRCDIR}/../core/output/lib configs
    sed -i 's/-Wl,-rpath=${SRCDIR}\/..\/core\/output\/lib//g' "$goRpathFiles"
done

## build
make install -e BUILD_TAGS=v%{tag_version} -e GIT_COMMIT=%{git_commit}

%install

# dir
mkdir -p %{buildroot}/usr/bin/
mkdir -p %{buildroot}/lib64/milvus
mkdir -p %{buildroot}/etc/milvus/configs/advanced
mkdir -p %{buildroot}/etc/systemd/system/
mkdir -p %{buildroot}/etc/ld.so.conf.d/

cd %{_builddir}/milvus-%{tag_version}

# bin
echo 'export MILVUSCONF=/etc/milvus/configs/' > %{buildroot}/usr/bin/milvus
echo 'milvus-server $@' >> %{buildroot}/usr/bin/milvus
chmod 755 %{buildroot}/usr/bin/milvus
install -m 755 bin/milvus %{buildroot}/usr/bin/milvus-server
install -m 755 %{_builddir}/etcd-v%{etcd_version}-%{arch}/etcd %{buildroot}/usr/bin/milvus-etcd
install -m 755 %{_builddir}/minio %{buildroot}/usr/bin/milvus-minio

# lib
install -m 755 lib/libmilvus_indexbuilder.so %{buildroot}/lib64/milvus/libmilvus_indexbuilder.so
install -m 755 lib/libmilvus_segcore.so %{buildroot}/lib64/milvus/libmilvus_segcore.so
install -m 755 %{_builddir}/OpenBLAS-%{openblas_version}/libopenblas-r0.3.9.so %{buildroot}/lib64/milvus/libopenblas.so.0
install -m 755 %{_builddir}/tbb-%{tbb_commit}/build/libtbb.so %{buildroot}/lib64/milvus/libtbb.so
install -m 755 lib/libfiu.so.1.00 %{buildroot}/lib64/milvus/libfiu.so.0
install -m 755 lib/libngt.so.1.12.0 %{buildroot}/lib64/milvus/libngt.so.1
install -m 755 /usr/lib64/libgfortran.so.4.0.0 %{buildroot}/lib64/milvus/libgfortran.so.4

# conf
install -m 755 configs/milvus.yaml %{buildroot}/etc/milvus/configs/milvus.yaml
install -m 755 configs/advanced/etcd.yaml %{buildroot}/etc/milvus/configs/advanced/etcd.yaml

# service
install -m 644 build/rpm/services/milvus.service %{buildroot}/etc/systemd/system/milvus.service
install -m 644 build/rpm/services/milvus-etcd.service %{buildroot}/etc/systemd/system/milvus-etcd.service
install -m 644 build/rpm/services/milvus-minio.service %{buildroot}/etc/systemd/system/milvus-minio.service

# ldconf
echo '/usr/lib64/milvus' >> %{buildroot}/etc/ld.so.conf.d/milvus.conf
chmod 644 %{buildroot}/etc/ld.so.conf.d/milvus.conf

%post
# update ld, systemd cache
ldconfig
systemctl daemon-reload

%preun
# disable service before remove
systemctl stop milvus
systemctl disable milvus

%postun
# update ld, systemd cache
ldconfig
systemctl daemon-reload

%files
/usr/bin/milvus
/usr/bin/milvus-server
/usr/bin/milvus-etcd
/usr/bin/milvus-minio

/lib64/milvus/libmilvus_indexbuilder.so
/lib64/milvus/libmilvus_segcore.so
/lib64/milvus/libopenblas.so.0
/lib64/milvus/libfiu.so.0
/lib64/milvus/libngt.so.1
/lib64/milvus/libgfortran.so.4

/etc/milvus/configs/milvus.yaml
/etc/milvus/configs/advanced/etcd.yaml

/etc/systemd/system/milvus.service
/etc/systemd/system/milvus-etcd.service
/etc/systemd/system/milvus-minio.service

/etc/ld.so.conf.d/milvus.conf

%changelog
# let's skip this for now
