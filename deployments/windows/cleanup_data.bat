cd /d %~dp0

rmdir /s /q default.etcd s3data var
del /q *.log