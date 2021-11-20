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
  cargo xtask test --fail-on-warnings
fi