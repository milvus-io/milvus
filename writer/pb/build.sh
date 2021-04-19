#!/usr/bin/env bash

pkg=pb
protoc --go_out=import_path=${pkg}:. suvlim.proto