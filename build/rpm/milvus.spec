%if 0%{!?version:1}
%global version 2.0.2
%endif

%if 0%{!?release:1}
%global release 1%{?dist}
%endif

Name:             milvus
Version:          %{version}
Release:          %{release}
Summary:          Milvus V2 RPM
License:          Apache License 2.0
Requires(preun):  libstdc++ libgomp tbb-devel tzdata
# tbb-devel actually provides it, but not defined
Provides:         libtbb.so()(64bit)
BuildArch:        x86_64

%description
Milvus is an open-source vector database built to power embedding similarity search and AI applications. 
Milvus makes unstructured data search more accessible, and provides a consistent user experience regardless of the deployment environment.

%install
# dir
mkdir -p %{buildroot}/usr/bin/
mkdir -p %{buildroot}/lib64/milvus
mkdir -p %{buildroot}/etc/milvus/configs/advanced
mkdir -p %{buildroot}/etc/systemd/system/
mkdir -p %{buildroot}/etc/ld.so.conf.d/

# bin
echo 'export MILVUSCONF=/etc/milvus/configs/' > %{buildroot}/usr/bin/milvus
echo 'milvus-server $@' >> %{buildroot}/usr/bin/milvus
chmod 755 %{buildroot}/usr/bin/milvus
install -m 755 bin/milvus %{buildroot}/usr/bin/milvus-server
install -m 755 bin/etcd %{buildroot}/usr/bin/milvus-etcd
install -m 755 bin/minio %{buildroot}/usr/bin/milvus-minio

# lib
for lib in \
    libaddress_sorting.so* \
    libbson-1.0.so* \
    libbsoncxx.so* \
    libcrypto.so* \
    libdouble-conversion.so* \
    libevent_core-2.1.so* \
    libfolly.so* \
    libfolly_test_util.so* \
    libgflags_nothreads.so* \
    libglog.so* \
    libgpr.so* \
    libgrpc++.so* \
    libgrpc.so* \
    libicudata.so* \
    libjemalloc.so* \
    libknowhere.so* \
    liblzma.so* \
    libmilvus-common.so* \
    libmilvus_core.so* \
    libmilvus-planparser-cpp.so* \
    libmilvus-planparser.so* \
    libmilvus-storage.so* \
    libprotobuf.so* \
    librdkafka.so* \
    librocksdb.so* \
    libsimdjson.so* \
    libssl.so* \
    libtbb.so* \
    libupb_base_lib.so* \
    libupb_json_lib.so* \
    libupb_mem_lib.so* \
    libupb_message_lib.so* \
    libupb_mini_descriptor_lib.so* \
    libupb_textformat_lib.so* \
    libupb_wire_lib.so* \
    libutf8_range_lib.so*; do
    cp -a lib/${lib} %{buildroot}/lib64/milvus/
done

# conf
install -m 755 configs/milvus.yaml %{buildroot}/etc/milvus/configs/milvus.yaml
install -m 755 configs/advanced/etcd.yaml %{buildroot}/etc/milvus/configs/advanced/etcd.yaml

# service
install -m 644 services/milvus.service %{buildroot}/etc/systemd/system/milvus.service
install -m 644 services/milvus-etcd.service %{buildroot}/etc/systemd/system/milvus-etcd.service
install -m 644 services/milvus-minio.service %{buildroot}/etc/systemd/system/milvus-minio.service

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
rm -rf /lib64/milvus
rm -rf /etc/milvus

%postun
# update ld, systemd cache
ldconfig
systemctl daemon-reload

%files
/usr/bin/milvus
/usr/bin/milvus-server
/usr/bin/milvus-etcd
/usr/bin/milvus-minio

/lib64/milvus/libaddress_sorting.so*
/lib64/milvus/libbson-1.0.so*
/lib64/milvus/libbsoncxx.so*
/lib64/milvus/libcrypto.so*
/lib64/milvus/libdouble-conversion.so*
/lib64/milvus/libevent_core-2.1.so*
/lib64/milvus/libfolly.so*
/lib64/milvus/libfolly_test_util.so*
/lib64/milvus/libgflags_nothreads.so*
/lib64/milvus/libglog.so*
/lib64/milvus/libgpr.so*
/lib64/milvus/libgrpc++.so*
/lib64/milvus/libgrpc.so*
/lib64/milvus/libicudata.so*
/lib64/milvus/libjemalloc.so*
/lib64/milvus/libknowhere.so*
/lib64/milvus/liblzma.so*
/lib64/milvus/libmilvus-common.so*
/lib64/milvus/libmilvus_core.so*
/lib64/milvus/libmilvus-planparser-cpp.so*
/lib64/milvus/libmilvus-planparser.so*
/lib64/milvus/libmilvus-storage.so*
/lib64/milvus/libprotobuf.so*
/lib64/milvus/librdkafka.so*
/lib64/milvus/librocksdb.so*
/lib64/milvus/libsimdjson.so*
/lib64/milvus/libssl.so*
/lib64/milvus/libtbb.so*
/lib64/milvus/libupb_base_lib.so*
/lib64/milvus/libupb_json_lib.so*
/lib64/milvus/libupb_mem_lib.so*
/lib64/milvus/libupb_message_lib.so*
/lib64/milvus/libupb_mini_descriptor_lib.so*
/lib64/milvus/libupb_textformat_lib.so*
/lib64/milvus/libupb_wire_lib.so*
/lib64/milvus/libutf8_range_lib.so*

/etc/milvus/configs/milvus.yaml
/etc/milvus/configs/advanced/etcd.yaml

/etc/systemd/system/milvus.service
/etc/systemd/system/milvus-etcd.service
/etc/systemd/system/milvus-minio.service

/etc/ld.so.conf.d/milvus.conf

%changelog
# let's skip this for now
