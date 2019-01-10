#!/usr/bin/env bash
# This script takes care of building your crate and packaging it for release

set -ex

main() {
    local src=$(pwd) \
          stage=

    case $TRAVIS_OS_NAME in
        linux | windows)
            stage=$(mktemp -d)
            ;;
        osx)
            stage=$(mktemp -d -t tmp)
            ;;
    esac


    test -f Cargo.lock || cargo generate-lockfile

    # TODO Update this to build the artifacts that matter to you

    if [ "$NATIVE_BUILD" ]; then
        cargo rustc --bin agrind --target $TARGET --release -- -C lto
    else
        cross rustc --bin agrind --target $TARGET --release -- -C lto
    fi

    # TODO Update this to package the right artifacts
    cp target/$TARGET/release/agrind $stage/

    cd $stage
    case $TRAVIS_OS_NAME in
        windows)
            zip $src/$CRATE_NAME-$TRAVIS_TAG-$TARGET.zip *
            ;;
        *)
            tar czf $src/$CRATE_NAME-$TRAVIS_TAG-$TARGET.tar.gz *
            ;;
    esac
    cd $src

    rm -rf $stage
}

main
