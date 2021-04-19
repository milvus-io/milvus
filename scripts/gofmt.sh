#!/usr/bin/env bash

## green to echo
function green(){
    echo -e "\033[32m $1 \033[0m"
}

## Error
function bred(){
    echo -e "\033[31m\033[01m $1 \033[0m"
}

files=()
files_need_gofmt=()

if [ -f "$1" ];then
    files+=("$1")
fi

if [ -d "$1" ];then
    for file in `find $1 -type f | grep "\.go$"`; do
        files+=("$file")
    done
fi

# Check for files that fail gofmt.
if [[ "${#files[@]}" -ne 0 ]]; then
    for file in "${files[@]}"; do
        diff="$(gofmt -s -d ${file} 2>&1)"
        if [[ -n "$diff" ]]; then
            files_need_gofmt+=("${file}")
        fi
    done
fi

if [[ "${#files_need_gofmt[@]}" -ne 0 ]]; then
    bred "ERROR!"
    for file in "${files_need_gofmt[@]}"; do
        gofmt -s -d ${file} 2>&1
    done
    echo ""
    bred "Some files have not been gofmt'd. To fix these errors, "
    bred "copy and paste the following:"
    for file in "${files_need_gofmt[@]}"; do
        bred "  gofmt -s -w ${file}"
    done
    exit 1
else
    green "OK"
    exit 0
fi
