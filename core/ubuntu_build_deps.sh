#!/bin/bash

sudo apt-get install -y gfortran libmysqlclient-dev mysql-client libcurl4-openssl-dev

sudo ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so /usr/lib/x86_64-linux-gnu/libmysqlclient_r.so
