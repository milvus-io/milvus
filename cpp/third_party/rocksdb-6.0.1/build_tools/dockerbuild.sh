#!/usr/bin/env bash
docker run -v $PWD:/rocks -w /rocks buildpack-deps make
