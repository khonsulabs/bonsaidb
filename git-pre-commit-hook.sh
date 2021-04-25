#!/bin/bash

# exit when any command fails
set -e 

if [[ $1 == "install" ]]; then
  pushd .git/hooks
  rm pre-commit
  ln -s ../../git-pre-commit-hook.sh pre-commit
  echo "Installed precommit hook."
  popd
else
  cargo +nightly fmt
  cargo clippy -- -D warnings
  pushd local
  cargo clippy --no-default-features
  popd
  pushd server
  cargo clippy --no-default-features
  popd
  pushd client
  cargo clippy --no-default-features
  popd
  cargo doc --no-deps --all-features
  cargo test --all-features
fi