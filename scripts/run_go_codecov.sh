#!/usr/bin/env bash

set -e
echo "mode: atomic" > go_coverage.txt

for d in $(go list ./internal... | grep -v vendor); do
    go test -race -coverpkg=./... -coverprofile=profile.out -covermode=atomic "$d"
    if [ -f profile.out ]; then
        sed '1d' profile.out >> go_coverage.txt
        rm profile.out
    fi
done

go tool cover -html=./go_coverage.txt -o ./go_coverage.html
