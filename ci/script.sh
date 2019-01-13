#!/bin/bash
# This script takes care of testing your crate

set -ex

main() {
    cargo build
    if [ ! -z $DISABLE_TESTS ]; then
        return
    fi
    cargo test
    cargo run -- --help

    if [ $TEST_RELEASE ]; then
        cargo test --release
    fi
}

# we don't run the "test phase" when doing deploys
if [ -z $TRAVIS_TAG ]; then
    main
fi
