#!/bin/bash
echo "Executing tests"
CARGO_INCREMENTAL=0 LLVM_PROFILE_FILE="%m.profraw" RUSTFLAGS="-Zinstrument-coverage" RUSTDOCFLAGS="-Cpanic=abort" cargo +nightly test --all-features
echo "Generating coverage report"
grcov . --binary-path ./target/debug/ -s . -t html --branch --ignore-not-existing --llvm -o coverage/
if command -v cargo-badge &> /dev/null
then
    echo "Generating badge"
    cargo badge -s "coverage" "`cat coverage/index.html | grep -oP '\d+(\.\d+)? %' | head -n1`" -o coverage/badge.svg
fi
echo "Cleaning up."
find . -name "*.profraw" -exec rm {} \;