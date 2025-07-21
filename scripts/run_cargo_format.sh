#!/bin/bash

(
    cd "$1" || exit 1

    if [ "$2" == "--check" ]; then
        if ! cargo fmt --all -- --check; then
            echo "Rust code is not properly formatted."
            exit 1
        fi
        echo "Check rust format success"
    else
        cargo fmt --all
    fi

    exit 0
)