#!/usr/bin/env bash

protoc -I=./ --cpp_out=./ pulsar.proto