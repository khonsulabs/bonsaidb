## Developing BonsaiDb

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
