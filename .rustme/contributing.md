## Developing BonsaiDb

### Writing Unit Tests

Unless there is a good reason not to, every feature in BonsaiDb should have
thorough unit tests. Many tests are implemented in `bonsaidb_core::test_util`
via a macro that allows the suite to run using various methods of accessing
BonsaiDb.

Some features aren't able to be tested using the `Connection`,
`StorageConnection`, `KeyValue`, and `PubSub` traits only. If that's the case,
you should add tests to whichever crates makes the most sense to test the code.
For example, if it's a feature that only can be used in `bonsaidb-server`, the
test should be somewhere in the `bonsaidb-server` crate.

Tests that require both a client and server can be added to the `core-suite`
test file in the `bonsaidb` crate.

### Checking Syntax

We use `clippy` to give additional guidance on our code. Clippy should always return with no errors, regardless of feature flags being enabled:

```bash
cargo clippy --all-features
```

### Running all tests

Our CI processes require that some commands succeed without warnings or errors. These checks can be performed manually by running:

```bash
cargo xtask test --fail-on-warnings
```

Or, if you would like to run all these checks before each commit, you can install the check as a pre-commit hook:

```bash
cargo xtask install-pre-commit-hook
```

### Formatting Code

We have a custom rustfmt configuration that enables several options only available in nightly builds:

```bash
cargo +nightly fmt
```
