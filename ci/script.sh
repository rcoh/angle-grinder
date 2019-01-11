# This script takes care of testing your crate

set -ex

# TODO This is the "test phase", tweak it as you see fit
main_cross() {
    cross build --target $TARGET
    cross build --target $TARGET --release

    if [ "$DISABLE_TESTS" ]; then
        return
    fi

    cross test --target $TARGET
    cross test --target $TARGET --release

    cross run --target $TARGET -- --help
    cross run --target $TARGET --release -- --help
}

main_cargo() {
    cargo build --target $TARGET
    cargo build --target $TARGET --release

    if [ "$DISABLE_TESTS" ]; then
        return
    fi

    cargo test --target $TARGET
    cargo test --target $TARGET --release

    cargo run --target $TARGET -- --help
    cargo run --target $TARGET --release -- --help
}

# we don't run the "test phase" when doing deploys
if [ "$TRAVIS_TAG" ]; then
    exit 0
fi

if [ "$NATIVE_BUILD" ]; then
    main_cargo
else
    main_cross
fi

