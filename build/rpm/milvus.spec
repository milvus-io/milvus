%if 0%{!?version:1}
%global version 2.0.0
%endif

%if 0%{!?release:1}
%global release %(date +%Y%m%d)%{?dist}
%endif

Name:             milvus
Version:          %{version}
Release:          %{release}
Summary:          Milvus V2 RPM
License:          Apache License 2.0
Requires(preun):  libstdc++ libgomp tbb-devel
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
install -m 755 lib/libmilvus_indexbuilder.so %{buildroot}/lib64/milvus/libmilvus_indexbuilder.so
install -m 755 lib/libmilvus_segcore.so %{buildroot}/lib64/milvus/libmilvus_segcore.so
install -m 755 /usr/lib/libopenblas-r0.3.9.so %{buildroot}/lib64/milvus/libopenblas.so.0
install -m 755 lib/libfiu.so.1.00 %{buildroot}/lib64/milvus/libfiu.so.0
install -m 755 lib/libngt.so.1.12.0 %{buildroot}/lib64/milvus/libngt.so.1
install -m 755 /usr/lib64/libgfortran.so.4.0.0 %{buildroot}/lib64/milvus/libgfortran.so.4

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
