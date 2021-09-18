#!/usr/bin/env bash

set -e
echo "" > coverage.txt

for d in $(go list ./internal... | grep -v vendor); do
    go test -race -coverpkg=./... -coverprofile=profile.out -covermode=atomic "$d"
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        go tool cover -html=profile.out -o go_coverage.html
        rm profile.out
    fi
done
